from typing import Iterable, Iterator, List, Optional, Set, Union, overload

import pyarrow as pa
from pyarrow.fs import FileSystem

from ray.data._internal.datasource_v2.listing.file_indexer import FileIndexer
from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest
from ray.data._internal.datasource_v2.listing.file_pruners import FilePruner


class LazyFileIndex:
    """Lazy file index with caching and early-exit sampling.

    Provides a caching wrapper around FileIndexer:
    - list_files(): Streams all manifests, caching for subsequent calls
    - list_files(sample=N): Returns merged manifest of up to N files (early exit)

    Both modes share the same cache - sampling caches manifests as it iterates,
    and subsequent list_files() calls continue from where sampling stopped.

    Usage:
        index = LazyFileIndex(indexer, paths, filesystem, pruners)

        # Planning: sample for schema inference (early exit, caches as it goes)
        sample = index.list_files(sample=10)
        schema = infer_schema(sample)

        # Execution: continues from cache, then lists remaining files
        for manifest in index.list_files():
            process(manifest)

    Workers receive a pickled index; the in-flight iterator is not preserved
    across pickle boundaries. Two-stage reads are expected to hand each worker
    a fresh `LazyFileIndex` over its own disjoint path shard, so workers never
    resume a partially-consumed iterator.
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
        self._indexer = indexer
        self._paths = paths
        self._filesystem = filesystem
        self._pruners = pruners
        self._cached_manifests: List[FileManifest] = []
        self._cached_paths: Set[str] = set()
        self._listing_iterator: Optional[Iterator[FileManifest]] = None
        self._is_fully_cached: bool = False
        self._preserve_order = preserve_order

    @property
    def is_fully_cached(self) -> bool:
        return self._is_fully_cached

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
                manifests.

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
        yield from self._cached_manifests

        if self._is_fully_cached:
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

        self._is_fully_cached = True
        self._listing_iterator = None

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
        collected: List[FileManifest] = []
        total = 0

        for manifest in self._iter_manifests():
            remaining = n - total
            if remaining <= 0:
                break

            if len(manifest) > remaining:
                collected.append(FileManifest(manifest.as_block().slice(0, remaining)))
                break

            collected.append(manifest)
            total += len(manifest)

        if not collected:
            raise ValueError(f"No files found in {self._paths}")

        return FileManifest.concat(collected)

    def __getstate__(self) -> dict:
        return {
            "_indexer": self._indexer,
            "_paths": self._paths,
            "_filesystem": self._filesystem,
            "_pruners": self._pruners,
            "_cached_manifests": self._cached_manifests,
            "_cached_paths": self._cached_paths,
            "_is_fully_cached": self._is_fully_cached,
        }

    def __setstate__(self, state: dict) -> None:
        self._indexer = state["_indexer"]
        self._paths = state["_paths"]
        self._filesystem = state["_filesystem"]
        self._pruners = state["_pruners"]
        self._cached_manifests = state["_cached_manifests"]
        self._cached_paths = state["_cached_paths"]
        self._is_fully_cached = state["_is_fully_cached"]
        self._listing_iterator = None
