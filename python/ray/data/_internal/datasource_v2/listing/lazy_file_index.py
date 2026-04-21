from typing import Iterable, Iterator, List, Optional, Set, Union, overload

import pyarrow as pa
from pyarrow.fs import FileSystem

from ray.data._internal.datasource_v2.listing.file_indexer import FileIndexer
from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest
from ray.data._internal.datasource_v2.listing.file_pruners import FilePruner

DEFAULT_PLANNING_SAMPLE_SIZE = 10


class LazyFileIndex:
    """Lazy, caching view over a ``FileIndexer``.

    ``list_files()`` streams every manifest; ``list_files(sample=N)`` returns
    a merged manifest of up to ``N`` files and stops early. Both modes share
    one cache: manifests yielded by either call are replayed on subsequent
    calls without re-invoking the indexer, so a planning-time sample is
    reused when execution later streams the full listing.

    Driver-side listing yields balanced buckets but blocks planning;
    worker-side listing hides cost behind read I/O but loses balance.
    ``DataContext.planning_file_listing_budget`` caps driver-side work and
    flips to worker-side for very large listings (e.g. millions of files).

    The planner samples on the driver for schema and size estimation, then
    continues listing on the driver up to the budget. If ``fully_listed``,
    the scanner produces balanced buckets; otherwise the planner splits
    remaining paths into shards that workers list and read in a pipeline::

        Driver                                               Workers
        ──────                                               ───────
        sample = lfi.list_files(sample=N)   ──▶ schema + size estimate
              │
              ▼
        for chunk in lfi.list_files():      # warm cache on driver
            if total >= ctx.planning_file_listing_budget:
                break
              │
              ▼
        if lfi.fully_listed:                # single-stage
            buckets = scanner.plan(         ─────────────▶  reader.read(bucket)
                lfi.cached_manifest)
        else:                               # two-stage
            shards = split_paths(           ─────────────▶  shard.list_files()
                lfi.remaining_paths)                        + reader.read(manifest)

    Concurrency: only one active iteration per instance; a second
    ``list_files`` call while an iterator is live raises.

    Pickle: the in-flight iterator is dropped; only the cache survives.
    After unpickling, cached manifests replay first and the indexer is
    re-invoked for the remainder, filtering paths already cached. Assumes
    the filesystem listing is stable across pickle boundaries.
    """

    def __init__(
        self,
        indexer: FileIndexer,
        paths: List[str],
        filesystem: "FileSystem",
        pruners: Optional[List[FilePruner]] = None,
        preserve_order: bool = False,
    ):
        """Initialize the lazy file index.

        Args:
            indexer: The underlying FileIndexer to use.
            paths: List of paths to index.
            filesystem: PyArrow filesystem for file operations.
            pruners: Optional file pruners applied during listing.
            preserve_order: If True, preserve deterministic listing order.
        """

        # If some of these properties are not pickleable, we need to remove them from the __dict__ before pickling.
        self._indexer = indexer
        self._paths = paths
        self._filesystem = filesystem
        self._pruners = pruners
        self._cached_manifests: List[FileManifest] = []
        self._cached_paths: Set[str] = set()
        self._listing_iterator: Optional[Iterator[FileManifest]] = None
        self._fully_listed: bool = False
        self._preserve_order = preserve_order
        self._iterating: bool = False

    @property
    def fully_listed(self) -> bool:
        return self._fully_listed

    @overload
    def list_files(self, *, sample: None = None) -> Iterable[FileManifest]:
        ...

    @overload
    def list_files(self, *, sample: int) -> FileManifest:
        ...

    def list_files(
        self, *, sample: Optional[int] = None
    ) -> Union[Iterable[FileManifest], FileManifest]:
        """List files, caching results for subsequent calls.

        Args:
            sample: If provided, return a merged FileManifest with up to this
                many files (early exit). If None, return an iterable of all
                manifests. Must be positive if provided. Planning-time callers
                should pass ``DEFAULT_PLANNING_SAMPLE_SIZE``.

        Returns:
            If sample is None: Iterable[FileManifest] streaming all files.
            If sample is int: Single FileManifest with up to ``sample`` files.

        Raises:
            ValueError: If sample is provided but no files are found.
        """
        if sample is not None:
            return self._sample(sample)
        return self._iter_manifests()

    def _iter_manifests(self) -> Iterable[FileManifest]:
        """Yield cached manifests, then stream remaining manifests, caching
        each new manifest as it arrives.

        When the indexer iterator is re-created (e.g. on a worker after
        unpickling), paths already present in the cache are filtered out so
        they are never re-listed.
        """
        assert (
            not self._iterating
        ), "LazyFileIndex does not support concurrent iteration"
        self._iterating = True
        try:
            yield from self._cached_manifests

            if self._fully_listed:
                return

            if self._listing_iterator is None:
                self._listing_iterator = self._indexer.list_files(
                    pa.array(self._paths),
                    filesystem=self._filesystem,
                    pruners=self._pruners,
                    preserve_order=self._preserve_order,
                )

            for manifest in self._listing_iterator:
                filtered = self._dedup_and_cache(manifest)
                if filtered is not None:
                    yield filtered

            self._fully_listed = True
            self._listing_iterator = None
            # Dedup set is only consulted when re-listing; once fully cached
            # it is no longer needed and can be large for big listings.
            self._cached_paths = set()
        finally:
            self._iterating = False

    def _dedup_and_cache(self, manifest: FileManifest) -> Optional[FileManifest]:
        paths = manifest.paths.tolist()
        sizes = manifest.file_sizes.tolist()

        new_paths: List[str] = []
        new_sizes: List[int] = []
        for path, size in zip(paths, sizes):
            if path in self._cached_paths:
                continue
            self._cached_paths.add(path)
            new_paths.append(path)
            new_sizes.append(size)

        if not new_paths:
            return None

        if len(new_paths) == len(paths):
            to_cache = manifest
        else:
            to_cache = FileManifest.construct_manifest(new_paths, new_sizes)

        self._cached_manifests.append(to_cache)
        return to_cache

    def _sample(self, n: int) -> FileManifest:
        assert n > 0, f"sample must be positive, got {n}"

        collected: List[FileManifest] = []
        total = 0

        gen = self._iter_manifests()
        try:
            for manifest in gen:
                remaining = n - total
                if remaining <= 0:
                    break

                if len(manifest) > remaining:
                    collected.append(
                        FileManifest(manifest.as_block().slice(0, remaining))
                    )
                    break

                collected.append(manifest)
                total += len(manifest)
        finally:
            gen.close()

        if not collected:
            raise ValueError(f"No files found in {self._paths}")

        return FileManifest.concat(collected)

    def __getstate__(self) -> dict:
        state = self.__dict__.copy()
        # The in-flight iterator can't be pickled; ``_iterating`` is a
        # runtime guard that doesn't carry meaning across processes.
        state["_listing_iterator"] = None
        state["_iterating"] = False
        return state
