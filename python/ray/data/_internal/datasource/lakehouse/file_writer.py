"""Format-agnostic file writer protocol.

Adapters that emit physical data files (Parquet today; ORC / Avro / custom in
the future) delegate the on-disk writing to a ``DataFileWriter``. This keeps
the choice of file format independent from the choice of catalog format, which
matters because not every lakehouse uses Parquet (Hudi's MOR row-groups, custom
columnar stores, etc.).

The Delta adapter composes a ``ParquetFileWriter`` whose implementation is the
mature, partition-aware buffered writer that previously lived in
``delta/writer.py``. The Iceberg adapter currently keeps using
``pyiceberg._dataframe_to_data_files`` because PyIceberg produces fully-formed
``DataFile`` records with format-specific metadata; migrating Iceberg to this
protocol is a follow-up.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Generic, List, Tuple, TypeVar

import pyarrow as pa

FileAction = TypeVar("FileAction")


class DataFileWriter(Generic[FileAction], ABC):
    """Write one or more Arrow tables to physical files.

    Implementations are stateful (they may buffer per partition) and are
    used inside a single Ray task. They are NOT expected to be pickled or
    shared across tasks.
    """

    @abstractmethod
    def add_table(self, table: pa.Table) -> List[FileAction]:
        """Push an Arrow table through the writer.

        Returns any file actions that were flushed as a result of this call.
        If the writer is buffering, the returned list may be empty.
        """

    @abstractmethod
    def flush(self) -> List[FileAction]:
        """Drain any remaining buffer and return its file actions."""


# ----------------------------------------------------------------------
# Parquet writer — generalised from ``delta/writer.py``.
# ----------------------------------------------------------------------

import logging  # noqa: E402
import time  # noqa: E402
import uuid  # noqa: E402
from collections import defaultdict  # noqa: E402
from typing import Callable, DefaultDict, Optional, Set  # noqa: E402

import pyarrow.compute as pc  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402

logger = logging.getLogger(__name__)

_MAX_PARTITIONS = 10_000
_VALID_COMPRESSIONS = {"snappy", "gzip", "brotli", "zstd", "lz4", "none"}


class ParquetFileWriter(DataFileWriter[FileAction]):
    """Partition-aware buffered Parquet writer.

    Generalises ``ray.data._internal.datasource.delta.writer.DeltaFileWriter``
    so it can be reused by any lakehouse adapter whose file format is Parquet.
    The Delta-specific ``AddAction`` shape is supplied via the ``action_factory``
    callback so this writer stays format-agnostic.

    Args:
        filesystem: PyArrow filesystem rooted at the table directory.
        partition_cols: Hive-style partition columns (may be empty).
        write_uuid: Stable identifier woven into filenames (8-char prefix).
        compression: Parquet compression codec; one of
            ``{"snappy", "gzip", "brotli", "zstd", "lz4", "none"}``.
        write_statistics: Whether to write Parquet stats per file.
        target_file_size_bytes: If set, the writer buffers per partition until
            this many bytes accumulate before writing a file. ``None`` flushes
            one file per ``add_table`` call.
        path_builder: ``(partition_cols, partition_values_tuple) ->
            (relative_dir, partition_values_dict)``. Builds the Hive-style
            partition directory; supplied by the adapter so it can honour
            format-specific encoding (NULL handling, NaN, URL-encoding).
        action_factory: Callable that wraps file metadata into the adapter's
            ``FileAction`` type. Receives keyword args
            ``(path, size, partition_values, modification_time, stats)``.
        stats_factory: Callable producing a stats blob (JSON string for
            Delta; the adapter may pass ``lambda _t: None`` to disable).
        written_files: Mutable set that the writer extends with every path it
            writes; the adapter passes this in for orphan-cleanup tracking.
        validate_path: Optional callable used to validate every output path
            before write (e.g. reject ``..`` traversal).
        retry: Optional callable wrapping the Parquet write with retry/backoff.
            Signature: ``retry(func, description)``. Default: no retry.
    """

    def __init__(
        self,
        *,
        filesystem: pa.fs.FileSystem,
        partition_cols: List[str],
        write_uuid: Optional[str],
        compression: str = "snappy",
        write_statistics: bool = True,
        target_file_size_bytes: Optional[int] = None,
        path_builder: Callable[
            [List[str], Tuple], Tuple[str, Dict[str, Optional[str]]]
        ],
        action_factory: Callable[..., FileAction],
        stats_factory: Callable[[pa.Table], Any] = lambda _t: None,
        written_files: Optional[Set[str]] = None,
        validate_path: Optional[Callable[[str], None]] = None,
        retry: Optional[Callable[[Callable[[], None], str], None]] = None,
    ):
        if compression not in _VALID_COMPRESSIONS:
            raise ValueError(
                f"Invalid compression '{compression}'. "
                f"Supported: {sorted(_VALID_COMPRESSIONS)}"
            )
        if target_file_size_bytes is not None and target_file_size_bytes <= 0:
            raise ValueError("target_file_size_bytes must be > 0")

        self._filesystem = filesystem
        self._partition_cols = partition_cols
        self._write_uuid = write_uuid
        self._compression = compression
        self._write_statistics = write_statistics
        self._target_file_size_bytes = target_file_size_bytes
        self._path_builder = path_builder
        self._action_factory = action_factory
        self._stats_factory = stats_factory
        self._written_files = written_files if written_files is not None else set()
        self._validate_path = validate_path
        self._retry = retry

        self._buffers: DefaultDict[Tuple, List[pa.Table]] = defaultdict(list)
        self._buffer_bytes: DefaultDict[Tuple, int] = defaultdict(int)
        self._file_seq = 0
        # The owning task index — adapters that care about task identity in
        # filenames set this via ``set_task_idx`` before writing.
        self._task_idx: int = 0

    # ------------------------------------------------------------------

    def set_task_idx(self, task_idx: int) -> None:
        """Set the Ray task index used in generated filenames."""
        self._task_idx = task_idx

    @property
    def written_files(self) -> Set[str]:
        """Set of relative paths written so far; mutated by ``add_table``."""
        return self._written_files

    # ------------------------------------------------------------------
    # DataFileWriter interface.
    # ------------------------------------------------------------------

    def add_table(self, table: pa.Table) -> List[FileAction]:
        if len(table) == 0:
            return []

        if not self._target_file_size_bytes:
            # No buffering: one file per partition per add_table call.
            self._file_seq += 1
            return self._write_table_immediate(table, block_idx=self._file_seq)

        # Buffered path.
        if self._partition_cols:
            parts = self._partition_table(table, self._partition_cols)
        else:
            parts = {(): table}

        actions: List[FileAction] = []
        for partition_values, partition_table in parts.items():
            self._buffers[partition_values].append(partition_table)
            self._buffer_bytes[partition_values] += getattr(
                partition_table, "nbytes", 0
            )
            if self._buffer_bytes[partition_values] >= self._target_file_size_bytes:
                actions.extend(self._flush_partition(partition_values))
        return actions

    def flush(self) -> List[FileAction]:
        actions: List[FileAction] = []
        for partition_values in list(self._buffers.keys()):
            actions.extend(self._flush_partition(partition_values))
        return actions

    # ------------------------------------------------------------------
    # Internal helpers — kept structurally identical to the original
    # ``DeltaFileWriter`` so behaviour matches commit-for-commit.
    # ------------------------------------------------------------------

    def _write_table_immediate(
        self, table: pa.Table, block_idx: int
    ) -> List[FileAction]:
        if self._partition_cols:
            parts = self._partition_table(table, self._partition_cols)
            return [
                a
                for a in (
                    self._write_partition(t, k, block_idx) for k, t in parts.items()
                )
                if a is not None
            ]
        a = self._write_partition(table, (), block_idx)
        return [a] if a is not None else []

    def _flush_partition(self, partition_values: Tuple) -> List[FileAction]:
        tables = self._buffers.get(partition_values)
        if not tables:
            return []
        merged = pa.concat_tables(tables, promote_options="none")
        self._buffers[partition_values].clear()
        self._buffer_bytes[partition_values] = 0
        self._file_seq += 1
        action = self._write_partition(merged, partition_values, self._file_seq)
        return [action] if action is not None else []

    def _partition_table(
        self, table: pa.Table, cols: List[str]
    ) -> Dict[Tuple, pa.Table]:
        """Partition table by columns using Arrow-native operations."""
        if len(table) == 0:
            return {}

        if len(cols) == 1:
            col = cols[0]
            unique_vals = pc.unique(table[col])
            if len(unique_vals) > _MAX_PARTITIONS:
                raise ValueError(
                    f"Too many partition values ({len(unique_vals)}). Max: {_MAX_PARTITIONS}"
                )
            out: Dict[Tuple, pa.Table] = {}
            for v in unique_vals:
                vpy = v.as_py()
                if vpy is None:
                    sub = table.filter(pc.is_null(table[col]))
                elif (
                    pa.types.is_floating(table[col].type)
                    and isinstance(vpy, float)
                    and vpy != vpy
                ):
                    sub = table.filter(pc.is_nan(table[col]))
                else:
                    sub = table.filter(pc.equal(table[col], v))
                if len(sub) > 0:
                    out[(vpy,)] = sub
            return out

        # Multi-column partitioning via struct + dictionary_encode.
        arrays = []
        fields = []
        for c in cols:
            arr = table[c]
            if isinstance(arr, pa.ChunkedArray):
                arr = arr.combine_chunks()
            arrays.append(arr)
            fields.append(pa.field(c, table.schema.field(c).type, nullable=True))

        struct_arr = pa.StructArray.from_arrays(arrays, fields=fields)

        try:
            enc = pc.dictionary_encode(struct_arr)
            dictionary, indices = enc.dictionary, enc.indices
            use_dictionary_keys = True
        except pa.ArrowNotImplementedError:
            use_dictionary_keys = False
            col_arrays = [table[c] for c in cols]
            col_arrays = [
                a.combine_chunks() if isinstance(a, pa.ChunkedArray) else a
                for a in col_arrays
            ]

            groups: Dict[Tuple, List[int]] = {}
            for i in range(len(table)):
                key = tuple(
                    arr[i].as_py() if arr[i].is_valid else None for arr in col_arrays
                )
                groups.setdefault(key, []).append(i)

            if len(groups) > _MAX_PARTITIONS:
                raise ValueError(
                    f"Too many partition combinations ({len(groups)}). Max: {_MAX_PARTITIONS}"
                )
            out = {}
            for key, indices_list in groups.items():
                sub = table.take(pa.array(indices_list))
                if len(sub) > 0:
                    out[key] = sub
            return out

        if len(dictionary) > _MAX_PARTITIONS:
            raise ValueError(
                f"Too many partition combinations ({len(dictionary)}). Max: {_MAX_PARTITIONS}"
            )

        out = {}
        for dict_idx in range(len(dictionary)):
            sub = table.filter(pc.equal(indices, dict_idx))
            if len(sub) == 0:
                continue
            if use_dictionary_keys:
                key_struct = dictionary[dict_idx]
                key = tuple(
                    key_struct[i].as_py() if key_struct[i].is_valid else None
                    for i in range(len(cols))
                )
            else:
                first = sub.slice(0, 1)
                key = tuple(first[c][0].as_py() for c in cols)
            out[key] = sub
        return out

    def _write_partition(
        self, table: pa.Table, partition_values: Tuple, block_idx: int
    ) -> Optional[FileAction]:
        if len(table) == 0:
            return None

        # Drop partition columns from the on-disk payload — both Iceberg and
        # Delta encode partition values in the directory, not the file.
        if self._partition_cols:
            data_cols = [c for c in table.column_names if c not in self._partition_cols]
            table = table.select(data_cols)

        filename = self._filename(block_idx)
        partition_path, partition_dict = self._path_builder(
            self._partition_cols, partition_values
        )
        rel_path = partition_path + filename

        if self._validate_path is not None:
            self._validate_path(rel_path)
        self._written_files.add(rel_path)

        size = self._write_parquet(table, rel_path)
        stats = self._stats_factory(table)

        return self._action_factory(
            path=rel_path,
            size=size,
            partition_values=partition_dict,
            modification_time=int(time.time() * 1000),
            stats=stats,
        )

    def _filename(self, block_idx: int) -> str:
        uid = uuid.uuid4().hex[:16]
        prefix = self._write_uuid or "00000000"
        prefix = prefix[:8].ljust(8, "0")
        return f"part-{prefix}-{self._task_idx:05d}-{block_idx:05d}-{uid}.parquet"

    def _write_parquet(self, table: pa.Table, rel_path: str) -> int:
        parent = _safe_dirname(rel_path)
        if parent:
            try:
                self._filesystem.create_dir(parent, recursive=True)
            except Exception:
                pass

        result: Dict[str, int] = {"size": 0}

        def _do_write() -> None:
            pq.write_table(
                table,
                rel_path,
                filesystem=self._filesystem,
                compression=self._compression,
                write_statistics=self._write_statistics,
            )
            info = self._filesystem.get_file_info(rel_path)
            if info.size == 0:
                try:
                    self._filesystem.delete_file(rel_path)
                except Exception:
                    pass
                raise RuntimeError(f"Written file is empty: {rel_path}")
            result["size"] = info.size

        if self._retry is not None:
            self._retry(_do_write, f"write Parquet file '{rel_path}'")
        else:
            _do_write()
        return result["size"]


def _safe_dirname(path: str) -> str:
    """Mirror of ``delta.utils.safe_dirname`` to keep this module standalone."""
    import os
    import posixpath

    if "://" in path:
        scheme, rest = path.split("://", 1)
        directory = posixpath.dirname(rest)
        return f"{scheme}://{directory}" if directory else ""
    return os.path.dirname(path)
