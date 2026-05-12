"""Delta Lake datasink — thin facade over LakehouseDatasink + DeltaAdapter.

Historically this module owned both the Ray Data write lifecycle *and* the
Delta-specific commit logic. After the lakehouse-abstraction refactor the
lifecycle lives in ``ray.data._internal.datasource.lakehouse.LakehouseDatasink``
and the Delta-specific behaviour lives in
``ray.data._internal.datasource.delta.adapter.DeltaAdapter``. This file keeps
the original ``DeltaDatasink`` constructor signature so existing callers and
tests continue to work — it simply builds a ``DeltaAdapter`` and wraps it in
the generic framework.
"""

from typing import TYPE_CHECKING, Any, Dict, List, Optional

import pyarrow as pa
import pyarrow.fs as pa_fs

from ray.data._internal.datasource.delta.adapter import DeltaAdapter
from ray.data._internal.datasource.lakehouse import (
    LakehouseDatasink,
    SaveMode,
)

if TYPE_CHECKING:
    from deltalake.transaction import AddAction  # noqa: F401


class DeltaDatasink(LakehouseDatasink["AddAction"]):
    """Datasink for writing to Delta Lake tables.

    Supports APPEND, OVERWRITE, UPSERT, ERROR, and IGNORE modes. ACID
    guarantees come from delta-rs's transaction log; the Ray-level
    orchestration (per-task Parquet writes followed by a single driver-side
    commit) is provided by :class:`LakehouseDatasink`.

    Delta Lake: https://delta.io/
    deltalake Python library: https://delta-io.github.io/delta-rs/python/
    """

    def __init__(
        self,
        path: str,
        *,
        mode: Any = SaveMode.APPEND,
        partition_cols: Optional[List[str]] = None,
        filesystem: Optional[pa_fs.FileSystem] = None,
        schema: Optional[pa.Schema] = None,
        upsert_kwargs: Optional[Dict[str, Any]] = None,
        schema_mode: str = "error",
        **write_kwargs,
    ):
        """Construct a Delta Lake datasink.

        Args:
            path: Path to Delta table (local or cloud storage).
            mode: Write mode — APPEND, OVERWRITE, UPSERT, ERROR, or IGNORE.
            partition_cols: Columns to partition by (Hive-style).
            filesystem: Optional PyArrow filesystem. For distributed writes,
                prefer ``storage_options`` so the filesystem is
                reconstructible on workers.
            schema: Optional explicit schema for the table.
            upsert_kwargs: Options for UPSERT mode. Supported keys:

                * ``join_cols``: List of column names to match rows on
                  (required for UPSERT).
            schema_mode: How to handle schema changes when writing to an
                existing table.

                * ``"error"`` — Reject new columns (default).
                * ``"merge"`` — Add new columns via
                  ``DeltaTable.alter.add_columns``.
            **write_kwargs: Additional options forwarded to the Delta writer:

                * ``target_file_size_bytes``: Target file size for buffering
                  (optional). When set, buffers per partition until the
                  threshold is reached.
                * ``max_commit_retries``: Maximum retries for commit
                  operations.
                * ``compression``: Compression codec (default ``"snappy"``).
                * ``write_statistics``: Whether to write Parquet statistics
                  (default ``True``).
                * ``partition_overwrite_mode``: For OVERWRITE mode with
                  partitioned tables — ``"static"`` (default, delete all
                  before writing) or ``"dynamic"`` (only delete partitions
                  being written).
        """
        adapter = DeltaAdapter(
            path,
            partition_cols=partition_cols,
            filesystem=filesystem,
            schema=schema,
            upsert_kwargs=upsert_kwargs,
            schema_mode=schema_mode,
            **write_kwargs,
        )

        coerced_mode = LakehouseDatasink._coerce_mode(mode)
        join_cols: List[str] = []
        if upsert_kwargs:
            cols = upsert_kwargs.get("join_cols") or []
            if isinstance(cols, list):
                join_cols = list(cols)

        super().__init__(
            adapter,
            coerced_mode,
            partition_cols=partition_cols,
            declared_schema=schema,
            join_cols=join_cols,
            name="Delta",
        )

    # ------------------------------------------------------------------
    # Compatibility shims.
    # ------------------------------------------------------------------
    @property
    def table_uri(self) -> str:
        return self._adapter.table_uri

    @property
    def mode(self) -> SaveMode:
        return self._mode

    @property
    def partition_cols(self) -> List[str]:
        return list(self._adapter.partition_cols)

    @property
    def schema(self) -> Optional[pa.Schema]:
        return self._adapter.schema

    @property
    def write_kwargs(self) -> Dict[str, Any]:
        return self._adapter.write_kwargs

    @property
    def storage_options(self) -> Dict[str, str]:
        return self._adapter.storage_options

    @property
    def filesystem(self) -> Optional[pa_fs.FileSystem]:
        return self._adapter.filesystem
