"""ORC footer-based ``FileIndexer`` that emits one manifest row per stripe.

Reads each ORC file's footer (0.2 ms per file, no data IO) to discover how
many stripes it contains, then fans out one ``FileManifest`` row per stripe.
Each row carries a :class:`~ray.data._internal.datasource_v2.interfaces.file_manifest.FileChunk`
whose ``unit_ids`` are the stripe indices and whose ``size_bytes`` /
``num_rows`` are footer-derived estimates (pyarrow does not expose the exact
encoded byte length of each stripe, so we apportion the on-disk file size
and total ``nrows`` evenly across stripes).

The indexer inherits directory traversal, pruner application, and shuffle
from :class:`NonSamplingFileIndexer`; only the manifest construction step
differs. ``OrcFileReader`` consumes the resulting per-stripe ``unit_ids`` and
reads each stripe directly via :func:`pyarrow.orc.ORCFile.read_stripe` rather
than going through :mod:`pyarrow.dataset`, whose ``FileFragment`` API for ORC
does not expose a stripe-level ``subset`` equivalent.

ORC files with a single stripe fall back to the whole-file representation
(``chunk_metadata is None``) so ``FileReader`` can still scan them through the
shared :mod:`pyarrow.dataset` path. This keeps manifest cardinality low when
stripe splitting buys nothing.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Iterator
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING

from pyarrow.fs import FileSystem
from typing_extensions import override

from ray.data._internal.datasource_v2.common.non_sampling_file_indexer import (
    NonSamplingFileIndexer,
    _shuffle_file_infos,
)
from ray.data._internal.datasource_v2.interfaces.file_indexer import FileInfo
from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    FileChunk,
    FileManifest,
)

if TYPE_CHECKING:
    from ray.data._internal.datasource_v2.interfaces.file_pruner import FilePruner
    from ray.data.block import BlockColumn
    from ray.data.datasource.file_based_datasource import FileShuffleConfig
    from ray.data.expressions import Expr

logger = logging.getLogger(__name__)


def _open_orc(path: str, filesystem: FileSystem | None):
    """Open an ORCFile for ``path`` and return it plus any file handle to close.

    ``filesystem=None`` means a local absolute path, which pyarrow opens
    directly from the OS and needs no explicit close. For remote filesystems
    we return the opened ``source`` alongside the :class:`pyarrow.orc.ORCFile`
    so the caller can close the Python source wrapper once footer metadata
    (nstripes, nrows, schema) has been consumed. Returning both values avoids
    closing the source inside a ``with`` before the caller is done with the
    ORCFile object (PyArrow's wrapper keeps a reference to the underlying
    C++ stream).
    """
    from pyarrow import orc

    if filesystem is None:
        return orc.ORCFile(path), None
    source = filesystem.open_input_file(path)
    try:
        return orc.ORCFile(source), source
    except Exception:
        # ORCFile constructor failed -- source was opened but never returned,
        # so close it here to avoid leaking descriptors on unreadable files.
        source.close()
        raise


def _chunk_orc_file(file_info: FileInfo, filesystem: FileSystem | None) -> FileManifest:
    """Return one manifest row per stripe of ``file_info``.

    When the file has exactly one stripe (or pyarrow cannot report stripe
    count), falls back to the whole-file row so the scanner can still use
    the :mod:`pyarrow.dataset` path without a per-stripe reader.
    """
    assert file_info.size is not None
    source_to_close = None
    try:
        of, source_to_close = _open_orc(file_info.path, filesystem)
        nstripes = int(of.nstripes)
        nrows = int(of.nrows)
    except Exception as exc:
        # Catch any error reading the footer -- corrupt files (ORC mid-write),
        # permission problems, unsupported ORC features, etc. The fallback
        # path reads the whole file through pyarrow.dataset, which handles
        # corruption as well as we can. Don't narrow the exception list;
        # callers have crashed here on ``AttributeError`` when the old code
        # referenced a non-existent ``pa.ArrowError``.
        logger.warning(
            "Failed to read ORC footer for %s: %s. Falling back to whole-file read.",
            file_info.path,
            exc,
        )
        return FileManifest.construct_manifest(
            paths=[file_info.path],
            sizes=[file_info.size],
            chunk_metadatas=[None],
        )
    finally:
        if source_to_close is not None:
            source_to_close.close()

    if nstripes <= 1:
        # No point splitting; emit the whole file so the existing
        # pyarrow.dataset reader path keeps working.
        return FileManifest.construct_manifest(
            paths=[file_info.path],
            sizes=[file_info.size],
            chunk_metadatas=[None],
        )

    # Apportion file size and total rows evenly across stripes. PyArrow
    # exposes nstripes and nrows from the footer but not the exact encoded
    # byte length of each stripe; the divisors are the best weights a
    # stripe-level partitioner has. Rounding toward earlier stripes keeps
    # the last stripe from reading past file size; slight inaccuracy is
    # acceptable because ``OnlineBinPacker`` is already estimator-driven
    # and will re-balance chunks across files.
    base_size = file_info.size // nstripes
    base_rows = nrows // nstripes
    extra_size = file_info.size - base_size * nstripes
    extra_rows = nrows - base_rows * nstripes

    paths = [file_info.path] * nstripes
    sizes = [file_info.size] * nstripes
    chunk_metadatas: list[FileChunk] = []
    for i in range(nstripes):
        size = base_size + (1 if i < extra_size else 0)
        rows = base_rows + (1 if i < extra_rows else 0)
        chunk_metadatas.append(
            FileChunk(
                unit_ids=(i,),
                num_rows=rows,
                size_bytes=size,
                fully_matched=False,
                # Per-unit breakdown is a single stripe per run, so we can
                # leave unit_sizes / unit_rows empty -- the packer doesn't
                # need to split runs that are already at unit granularity.
                unit_sizes=(),
                unit_rows=(),
            )
        )

    return FileManifest.construct_manifest(
        paths=paths,
        sizes=sizes,
        chunk_metadatas=[c.to_metadata() for c in chunk_metadatas],
    )


class OrcFileIndexer(NonSamplingFileIndexer):
    """``NonSamplingFileIndexer`` that splits each ORC file into stripes.

    Directory traversal, pruner application, and shuffle are inherited from
    :class:`NonSamplingFileIndexer`; this class overrides the final
    file-info-to-manifest step to fan each file into one row per stripe
    (when it has more than one stripe) or one whole-file row otherwise.

    The indexer reads each file's ORC footer once per ``list_files`` call,
    so ``as_whole_file_indexer`` returns ``None`` -- callers that need a
    pure listing without per-file IO (e.g. ``PushdownCountFiles``) must
    use the base :class:`NonSamplingFileIndexer` directly.
    """

    def __init__(
        self,
        *,
        ignore_missing_paths: bool,
        skip_paths: Iterable[str] | None = None,
        num_workers: int | None = None,
        max_paths_per_output: int | None = None,
    ):
        super().__init__(
            ignore_missing_paths=ignore_missing_paths,
            skip_paths=skip_paths,
            num_workers=num_workers,
            max_paths_per_output=max_paths_per_output,
        )

    @override
    def as_whole_file_indexer(self) -> None:
        # Footer reads are per-file IO; cannot pretend we don't do them.
        return None

    @override
    def list_files(
        self,
        paths: BlockColumn,
        *,
        filesystem: FileSystem | None,
        pruners: list[FilePruner] | None = None,
        preserve_order: bool = False,
        predicate: Expr | None = None,
        limit: int | None = None,
        projected_columns: list[str] | None = None,
        shuffle_config: FileShuffleConfig | None = None,
        execution_idx: int = 0,
        excluded_read_unit_ids: Iterable[str] | None = None,
    ) -> Iterable[FileManifest]:
        file_infos = self._iter_file_infos_for_list(
            paths,
            filesystem=filesystem,
            pruners=pruners,
            preserve_order=preserve_order,
            shuffle_config=shuffle_config,
            execution_idx=execution_idx,
            excluded_read_unit_ids=excluded_read_unit_ids,
        )
        yield from self._process_file_infos(
            file_infos,
            filesystem=filesystem,
            limit=limit,
        )

    def list_files_from_file_infos(
        self,
        file_infos: Iterable[FileInfo],
        *,
        filesystem: FileSystem | None,
        preserve_order: bool = False,
        predicate: Expr | None = None,
        limit: int | None = None,
        projected_columns: list[str] | None = None,
        shuffle_config: FileShuffleConfig | None = None,
        execution_idx: int = 0,
        excluded_read_unit_ids: Iterable[str] | None = None,
    ) -> Iterable[FileManifest]:
        if excluded_read_unit_ids:
            file_infos = (
                info for info in file_infos if info.path not in excluded_read_unit_ids
            )
        if shuffle_config is not None:
            file_infos = _shuffle_file_infos(
                list(file_infos), seed=shuffle_config.get_seed(execution_idx)
            )
        yield from self._process_file_infos(
            file_infos,
            filesystem=filesystem,
            limit=limit,
        )

    def _process_file_infos(
        self,
        file_infos: Iterable[FileInfo],
        *,
        filesystem: FileSystem | None,
        limit: int | None = None,
    ) -> Iterator[FileManifest]:
        """Read ORC footers in parallel and emit stripe-level manifests.

        We cap the thread-pool workers at 32 since footer reads are quick
        (0.2 ms each locally) and we only want to keep the listing pipe
        flowing without spawning hundreds of threads on a big directory.

        Yields manifests incrementally as footers finish parsing, so the
        first read task can start before every footer has been read. When
        ``limit`` is set we stop as soon as we've yielded enough rows, so
        ``limit(10)`` does not waste footer reads on files we'll never
        read.
        """
        # Materialize once so we can iterate multiple times if shuffling is
        # needed, but don't block on footer reads for all of them before
        # yielding any -- ``pool.map`` below iterates results lazily.
        infos = list(file_infos)
        if not infos:
            return

        # ORC footer reads are bound only by filesystem latency -- open
        # the file, seek to the footer, parse a small protobuf. 32 workers
        # keeps the listing responsive on S3 directories with thousands of
        # files without oversubscribing the driver.
        max_workers = min(len(infos), 32)
        total_rows_yielded = 0
        stop_at = limit if limit is not None else float("inf")
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            # ``pool.map`` preserves input order but runs jobs in parallel.
            # Iterating yields results as each footer read completes; we
            # don't ``list()`` so the first manifest rows leave this method
            # before every footer has been parsed.
            for manifest in pool.map(
                lambda info: _chunk_orc_file(info, filesystem), infos
            ):
                num_rows = len(manifest)
                remaining = stop_at - total_rows_yielded
                if remaining <= 0:
                    return
                if num_rows > remaining:
                    # The manifest has more rows than we still need. Slice
                    # it to ``remaining`` rows so we emit exactly ``limit``
                    # rows total.
                    manifest = FileManifest.construct_manifest(
                        paths=manifest.paths[:remaining].tolist(),
                        sizes=manifest.file_sizes[:remaining].tolist(),
                        chunk_metadatas=manifest.file_chunk_metadatas[
                            :remaining
                        ].tolist(),
                    )
                    num_rows = len(manifest)
                total_rows_yielded += num_rows
                yield manifest
                if total_rows_yielded >= stop_at:
                    return

        logger.debug(
            "ORC footer listing: %d files → %d manifest rows (one per stripe)",
            len(infos),
            total_rows_yielded,
        )
