"""Delta Lake datasink -- thin facade over TableDatasink + DeltaAdapter.

This MVP build exposes APPEND mode only. Subsequent PRs extend the public
surface (OVERWRITE/ERROR/IGNORE in PR 4, partition_cols in PR 5,
schema_mode in PR 6, cloud storage_options in PR 7).
"""

from typing import TYPE_CHECKING, Any, Dict, List, Optional, cast

import pyarrow as pa
import pyarrow.fs as pa_fs

from ray.data._internal.datasource.delta.adapter import DeltaAdapter
from ray.data._internal.datasource.table import SaveMode, TableDatasink

if TYPE_CHECKING:
    from deltalake.transaction import AddAction  # noqa: F401


class DeltaDatasink(TableDatasink["AddAction", str]):
    """Datasink for writing to Delta Lake tables (MVP, APPEND only)."""

    def __init__(
        self,
        path: str,
        *,
        mode: Any = SaveMode.APPEND,
        partition_cols: Optional[List[str]] = None,
        filesystem: Optional[pa_fs.FileSystem] = None,
        schema: Optional[pa.Schema] = None,
        schema_mode: str = "error",
        **write_kwargs,
    ):
        adapter = DeltaAdapter(
            path,
            partition_cols=partition_cols,
            filesystem=filesystem,
            schema=schema,
            schema_mode=schema_mode,
            **write_kwargs,
        )
        coerced_mode = TableDatasink._coerce_mode(mode)
        super().__init__(
            adapter,
            coerced_mode,
            partition_cols=partition_cols,
            declared_schema=schema,
            name="Delta",
        )

    # ------------------------------------------------------------------
    # Compatibility shims.
    # ------------------------------------------------------------------
    @property
    def _delta_adapter(self) -> DeltaAdapter:
        # __init__ always constructs a DeltaAdapter; narrow the base
        # ``TableAdapter`` type so the shims below resolve.
        return cast(DeltaAdapter, self._adapter)

    @property
    def table_uri(self) -> str:
        return self._delta_adapter.table_uri

    @property
    def mode(self) -> SaveMode:
        return self._mode

    @property
    def schema(self) -> Optional[pa.Schema]:
        return self._delta_adapter.schema

    @property
    def write_kwargs(self) -> Dict[str, Any]:
        return self._delta_adapter.write_kwargs

    @property
    def storage_options(self) -> Dict[str, str]:
        return self._delta_adapter.storage_options

    @property
    def filesystem(self) -> Optional[pa_fs.FileSystem]:
        return self._delta_adapter.filesystem

    @property
    def partition_cols(self) -> List[str]:
        return list(self._delta_adapter.partition_cols)
