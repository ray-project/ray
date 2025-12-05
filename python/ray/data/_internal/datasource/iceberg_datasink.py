"""
Module to write a Ray Dataset into an iceberg table, by using the Ray Datasink API.
"""
import logging
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional

from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.savemode import SaveMode
from ray.data.block import Block, BlockAccessor
from ray.data.datasource.datasink import Datasink, WriteResult
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    import pyarrow as pa
    from pyiceberg.catalog import Catalog
    from pyiceberg.table import Table

    from ray.data.expressions import Expr


logger = logging.getLogger(__name__)


@DeveloperAPI
class IcebergDatasink(Datasink[List["pa.Table"]]):
    """
    Iceberg datasink to write a Ray Dataset into an existing Iceberg table. This module
    heavily uses PyIceberg to write to iceberg table. All the routines in this class override
    `ray.data.Datasink`.

    """

    def __init__(
        self,
        table_identifier: str,
        catalog_kwargs: Optional[Dict[str, Any]] = None,
        snapshot_properties: Optional[Dict[str, str]] = None,
        mode: SaveMode = SaveMode.APPEND,
        overwrite_filter: Optional["Expr"] = None,
        upsert_kwargs: Optional[Dict[str, Any]] = None,
        overwrite_kwargs: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize the IcebergDatasink

        Args:
            table_identifier: The identifier of the table to read such as `default.taxi_dataset`
            catalog_kwargs: Optional arguments to use when setting up the Iceberg
                catalog
            snapshot_properties: Custom properties to write to snapshot summary, such as commit metadata
            mode: Write mode - APPEND, UPSERT, or OVERWRITE. Defaults to APPEND.
                - APPEND: Add new data without checking for duplicates
                - UPSERT: Update existing rows or insert new ones based on a join condition
                - OVERWRITE: Replace table data (all data or filtered subset)
            overwrite_filter: Optional filter for OVERWRITE mode to perform partial overwrites.
                Must be a Ray Data expression from `ray.data.expressions`. Only rows matching
                this filter are replaced. If None with OVERWRITE mode, replaces all table data.
            upsert_kwargs: Optional arguments to pass through to PyIceberg's table.upsert()
                method. Supported parameters include join_cols (List[str]),
                when_matched_update_all (bool), when_not_matched_insert_all (bool),
                case_sensitive (bool), branch (str). See PyIceberg documentation for details.
            overwrite_kwargs: Optional arguments to pass through to PyIceberg's table.overwrite()
                method. Supported parameters include case_sensitive (bool) and branch (str).
                See PyIceberg documentation for details.

        Note:
            Schema evolution is automatically enabled. New columns in the incoming data
            are automatically added to the table schema.
        """

        self.table_identifier = table_identifier
        self._catalog_kwargs = (catalog_kwargs or {}).copy()
        self._snapshot_properties = snapshot_properties or {}
        self._mode = mode
        self._overwrite_filter = overwrite_filter
        self._upsert_kwargs = (upsert_kwargs or {}).copy()
        self._overwrite_kwargs = (overwrite_kwargs or {}).copy()

        # Validate kwargs are only set for relevant modes
        if self._upsert_kwargs and self._mode != SaveMode.UPSERT:
            raise ValueError(
                f"upsert_kwargs can only be specified when mode is SaveMode.UPSERT, "
                f"but mode is {self._mode}"
            )
        if self._overwrite_kwargs and self._mode != SaveMode.OVERWRITE:
            raise ValueError(
                f"overwrite_kwargs can only be specified when mode is SaveMode.OVERWRITE, "
                f"but mode is {self._mode}"
            )

        if "name" in self._catalog_kwargs:
            self._catalog_name = self._catalog_kwargs.pop("name")
        else:
            self._catalog_name = "default"

        self._table: "Table" = None

    # Since iceberg table is not pickle-able, we need to exclude it during serialization
    def __getstate__(self) -> dict:
        """Exclude `_table` during pickling."""
        state = self.__dict__.copy()
        state.pop("_table", None)
        return state

    def __setstate__(self, state: dict) -> None:
        self.__dict__.update(state)
        self._table = None

    def _get_catalog(self) -> "Catalog":
        from pyiceberg import catalog

        return catalog.load_catalog(self._catalog_name, **self._catalog_kwargs)

    def _update_schema(self, incoming_schema: "pa.Schema") -> None:
        """
        Update the table schema to accommodate incoming data using union-by-name semantics.

        This automatically handles:
        - Adding new columns from the incoming schema
        - Type promotion (e.g., int32 -> int64) where compatible
        - Preserving existing columns not in the incoming schema

        Args:
            incoming_schema: The PyArrow schema from the incoming data
        """
        # Use PyIceberg's update_schema API
        with self._table.update_schema() as update:
            update.union_by_name(incoming_schema)

        # Reload table completely after schema evolution
        catalog = self._get_catalog()
        self._table = catalog.load_table(self.table_identifier)

    def on_write_start(self) -> None:
        """Initialize table for writing."""
        catalog = self._get_catalog()
        self._table = catalog.load_table(self.table_identifier)

    def _collect_tables_from_blocks(self, blocks: Iterable[Block]) -> List["pa.Table"]:
        """Collect PyArrow tables from blocks."""
        collected_tables = []

        for block in blocks:
            pa_table = BlockAccessor.for_block(block).to_arrow()

            if pa_table.num_rows > 0:
                collected_tables.append(pa_table)

        return collected_tables

    def write(self, blocks: Iterable[Block], ctx: TaskContext) -> List["pa.Table"]:
        """Collect blocks as PyArrow tables for all write modes."""
        return self._collect_tables_from_blocks(blocks)

    def _collect_and_concat_tables(
        self, write_result: WriteResult[List["pa.Table"]]
    ) -> Optional["pa.Table"]:
        """Collect and concatenate all PyArrow tables from write results."""
        import pyarrow as pa

        all_tables = []
        for tables_batch in write_result.write_returns:
            all_tables.extend(tables_batch)

        if not all_tables:
            logger.warning("No data to write")
            return None

        return pa.concat_tables(all_tables)

    def _complete_append(self, combined_table: "pa.Table") -> None:
        """Complete APPEND mode write using PyIceberg's append API."""
        self._table.append(
            df=combined_table,
            snapshot_properties=self._snapshot_properties,
        )
        logger.info(
            f"Appended {combined_table.num_rows} rows to {self.table_identifier}"
        )

    def _complete_upsert(self, combined_table: "pa.Table") -> None:
        """Complete UPSERT mode write using PyIceberg's upsert API."""
        self._table.upsert(df=combined_table, **self._upsert_kwargs)

        join_cols = self._upsert_kwargs.get("join_cols")
        if join_cols:
            logger.info(
                f"Upserted {combined_table.num_rows} rows to {self.table_identifier} "
                f"using join columns: {join_cols}"
            )
        else:
            logger.info(
                f"Upserted {combined_table.num_rows} rows to {self.table_identifier} "
                f"using table-defined identifier-field-ids"
            )

    def _complete_overwrite(self, combined_table: "pa.Table") -> None:
        """Complete OVERWRITE mode write using PyIceberg's overwrite API."""
        # Warn if user passed overwrite_filter via overwrite_kwargs
        if "overwrite_filter" in self._overwrite_kwargs:
            self._overwrite_kwargs.pop("overwrite_filter")
            logger.warning(
                "Use Ray Data's Expressions for overwrite filter instead of passing "
                "it via PyIceberg's overwrite_filter parameter"
            )

        if self._overwrite_filter:
            # Partial overwrite with filter
            from ray.data._internal.datasource.iceberg_datasource import (
                _IcebergExpressionVisitor,
            )

            iceberg_filter = _IcebergExpressionVisitor().visit(self._overwrite_filter)
            self._table.overwrite(
                df=combined_table,
                overwrite_filter=iceberg_filter,
                snapshot_properties=self._snapshot_properties,
                **self._overwrite_kwargs,
            )
            logger.info(
                f"Overwrote {combined_table.num_rows} rows in {self.table_identifier} "
                f"matching filter: {self._overwrite_filter}"
            )
        else:
            # Full table overwrite
            self._table.overwrite(
                df=combined_table,
                snapshot_properties=self._snapshot_properties,
                **self._overwrite_kwargs,
            )
            logger.info(
                f"Overwrote entire table {self.table_identifier} "
                f"with {combined_table.num_rows} rows"
            )

    def on_write_complete(self, write_result: WriteResult[List["pa.Table"]]) -> None:
        """Complete the write operation based on the configured mode."""
        # Collect and concatenate all PyArrow tables
        combined_table = self._collect_and_concat_tables(write_result)
        if combined_table is None:
            return

        # Apply schema evolution for all modes (PyIceberg doesn't handle this automatically)
        self._update_schema(combined_table.schema)

        # Execute the appropriate write operation
        if self._mode == SaveMode.APPEND:
            self._complete_append(combined_table)
        elif self._mode == SaveMode.UPSERT:
            self._complete_upsert(combined_table)
        elif self._mode == SaveMode.OVERWRITE:
            self._complete_overwrite(combined_table)
        else:
            raise ValueError(
                f"Unsupported write mode: {self._mode}. "
                f"Supported modes are: APPEND, UPSERT, OVERWRITE"
            )
