"""Parquet writer for the Delta Lake datasink.

Supports Hive-style partitioning and an optional buffered/large-file mode
controlled by ``target_file_size_bytes``. Schema evolution is handled at the
adapter level (PR 6); cloud filesystem reconstruction at the fs level (PR 7).

PyArrow Parquet: https://arrow.apache.org/docs/python/parquet.html
"""

import logging
import time
from collections import defaultdict
from typing import Any, DefaultDict, Dict, List, Optional, Set, Tuple

import pyarrow as pa
import pyarrow.compute as pc

from ray.data._internal.datasource._parquet_io import (
    build_uuid_filename,
    write_parquet_with_retry,
)
from ray.data._internal.datasource.delta.utils import (
    build_partition_path,
    compute_parquet_statistics,
    resolve_under_table_root,
    safe_dirname,
    validate_file_path,
    validate_partition_value,
)

logger = logging.getLogger(__name__)

_MAX_PARTITIONS = 10_000
_VALID_COMPRESSIONS = {"snappy", "gzip", "brotli", "zstd", "lz4", "none"}


def _import_add_action():
    """Import AddAction from deltalake, handling different package layouts."""
    for mod in ("deltalake.transaction", "deltalake", "deltalake.writer"):
        try:
            m = __import__(mod, fromlist=["AddAction"])
            return m.AddAction
        except Exception:
            continue
    raise ImportError(
        "Could not import AddAction from deltalake. Install/upgrade deltalake."
    )


class DeltaFileWriter:
    """Partition-aware buffered Parquet writer for Delta tables."""

    def __init__(
        self,
        *,
        filesystem: pa.fs.FileSystem,
        partition_cols: List[str],
        write_uuid: Optional[str],
        write_kwargs: Dict[str, Any],
        written_files: Set[str],
        target_file_size_bytes: Optional[int] = None,
        local_filesystem_root: Optional[str] = None,
    ):
        self.filesystem = filesystem
        self.partition_cols = partition_cols
        self.write_uuid = write_uuid
        self.write_kwargs = write_kwargs
        self.written_files = written_files
        self._AddAction = _import_add_action()
        self.target_file_size_bytes = target_file_size_bytes

        # Per-task buffers keyed by partition values tuple.
        self._buffers: DefaultDict[Tuple, List[pa.Table]] = defaultdict(list)
        self._buffer_bytes: DefaultDict[Tuple, int] = defaultdict(int)
        self._file_seq = 0
        self._local_filesystem_root = local_filesystem_root

        compression = self.write_kwargs.get("compression", "snappy")
        if compression not in _VALID_COMPRESSIONS:
            raise ValueError(
                f"Invalid compression '{compression}'. "
                f"Supported: {sorted(_VALID_COMPRESSIONS)}"
            )
        if self.target_file_size_bytes is not None and self.target_file_size_bytes <= 0:
            raise ValueError("target_file_size_bytes must be > 0")

    # ------------------------------------------------------------------
    # Public API.
    # ------------------------------------------------------------------
    def add_table(self, table: pa.Table, task_idx: int) -> List[Any]:
        """Push an Arrow table through the writer.

        With ``target_file_size_bytes`` unset, one file per partition per
        ``add_table`` call. Set, buffers per partition until threshold.
        """
        if len(table) == 0:
            return []

        if not self.target_file_size_bytes:
            return self._write_immediate(table, task_idx)

        # Buffered path.
        parts = (
            self.partition_table(table, self.partition_cols)
            if self.partition_cols
            else {(): table}
        )
        actions: List[Any] = []
        for partition_values, partition_table in parts.items():
            self._buffers[partition_values].append(partition_table)
            self._buffer_bytes[partition_values] += getattr(
                partition_table, "nbytes", 0
            )
            if self._buffer_bytes[partition_values] >= self.target_file_size_bytes:
                actions.extend(self._flush_partition(partition_values, task_idx))
        return actions

    def flush(self, task_idx: int) -> List[Any]:
        """Drain remaining buffered partitions and return their actions."""
        actions: List[Any] = []
        for partition_values in list(self._buffers.keys()):
            actions.extend(self._flush_partition(partition_values, task_idx))
        return actions

    # ------------------------------------------------------------------
    # Internal helpers.
    # ------------------------------------------------------------------
    def _write_immediate(self, table: pa.Table, task_idx: int) -> List[Any]:
        self._file_seq += 1
        if self.partition_cols:
            parts = self.partition_table(table, self.partition_cols)
            return [
                a
                for a in (
                    self._write_partition(t, k, task_idx, self._file_seq)
                    for k, t in parts.items()
                )
                if a is not None
            ]
        a = self._write_partition(table, (), task_idx, self._file_seq)
        return [a] if a is not None else []

    def _flush_partition(self, partition_values: Tuple, task_idx: int) -> List[Any]:
        tables = self._buffers.get(partition_values)
        if not tables:
            return []
        # Permissive promotion so buffered same-partition blocks with
        # promotable type diffs (e.g. int32/int64) merge — matching the
        # framework's permissive worker-schema unification, so buffered writes
        # (target_file_size_bytes) behave the same as unbuffered ones.
        merged = pa.concat_tables(tables, promote_options="permissive")
        self._buffers[partition_values].clear()
        self._buffer_bytes[partition_values] = 0
        self._file_seq += 1
        action = self._write_partition(
            merged, partition_values, task_idx, self._file_seq
        )
        return [action] if action is not None else []

    def partition_table(
        self, table: pa.Table, cols: List[str]
    ) -> Dict[Tuple, pa.Table]:
        """Partition the table by ``cols`` using Arrow-native ops."""
        if len(table) == 0:
            return {}

        if len(cols) == 1:
            col = cols[0]
            unique_vals = pc.unique(table[col])
            if len(unique_vals) > _MAX_PARTITIONS:
                raise ValueError(
                    f"Too many partition values ({len(unique_vals)}). "
                    f"Max: {_MAX_PARTITIONS}"
                )
            out: Dict[Tuple, pa.Table] = {}
            for v in unique_vals:
                vpy = v.as_py()
                if vpy is not None and not (isinstance(vpy, float) and vpy != vpy):
                    validate_partition_value(vpy)
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

        # Multi-column: struct + dictionary_encode (with fallback).
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
            # Manual grouping fallback for builds without struct dict_encode.
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
                    f"Too many partition combinations ({len(groups)}). "
                    f"Max: {_MAX_PARTITIONS}"
                )
            out = {}
            for key, indices_list in groups.items():
                sub = table.take(pa.array(indices_list))
                if len(sub) > 0:
                    out[key] = sub
            return out

        if len(dictionary) > _MAX_PARTITIONS:
            raise ValueError(
                f"Too many partition combinations ({len(dictionary)}). "
                f"Max: {_MAX_PARTITIONS}"
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
        self, table: pa.Table, partition_values: Tuple, task_idx: int, block_idx: int
    ) -> Optional[Any]:
        if len(table) == 0:
            return None
        # Drop partition columns from the on-disk payload.
        if self.partition_cols:
            data_cols = [c for c in table.column_names if c not in self.partition_cols]
            table = table.select(data_cols)

        filename = self._filename(task_idx, block_idx)
        partition_path, partition_dict = build_partition_path(
            self.partition_cols, partition_values
        )
        rel_path = partition_path + filename

        validate_file_path(rel_path)
        self.written_files.add(rel_path)

        size = self._write_parquet(table, rel_path)
        stats = compute_parquet_statistics(table)

        return self._AddAction(
            path=rel_path,
            size=size,
            partition_values=partition_dict,
            modification_time=int(time.time() * 1000),
            data_change=True,
            stats=stats,
        )

    def _filename(self, task_idx: int, block_idx: int) -> str:
        return build_uuid_filename(self.write_uuid, task_idx, block_idx)

    def _write_parquet(self, table: pa.Table, rel_path: str) -> int:
        phys_path = resolve_under_table_root(self._local_filesystem_root, rel_path)
        parent = safe_dirname(rel_path)
        phys_parent = (
            resolve_under_table_root(self._local_filesystem_root, parent)
            if self._local_filesystem_root and parent
            else parent
        )

        def _ensure_parent() -> None:
            if phys_parent:
                try:
                    self.filesystem.create_dir(phys_parent, recursive=True)
                except Exception:
                    pass

        return write_parquet_with_retry(
            table,
            phys_path,
            self.filesystem,
            compression=self.write_kwargs.get("compression", "snappy"),
            write_statistics=self.write_kwargs.get("write_statistics", True),
            on_each_attempt=_ensure_parent,
        )
