"""
TurbopufferDatasink - Ray Data datasink for Turbopuffer vector database

Implementation following the pattern of MongoDatasink and Daft's Turbopuffer sink.
Supports multi-namespace mode for writing data from many workspaces to separate namespaces.

This is based on [Turbopuffer Write API](https://turbopuffer.com/docs/write)
"""

import logging
import os
import uuid
from typing import TYPE_CHECKING, Iterable, Optional, Union

import pyarrow as pa
import pyarrow.compute as pc

from ray._common.retry import call_with_retry
from ray.data._internal.arrow_block import ArrowBlockAccessor, ArrowBlockColumnAccessor
from ray.data._internal.arrow_ops import transform_pyarrow
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.planner.exchange.sort_task_spec import SortKey
from ray.data._internal.util import _check_import
from ray.data.block import Block, BlockAccessor
from ray.data.datasource.datasink import Datasink

if TYPE_CHECKING:
    import turbopuffer

logger = logging.getLogger(__name__)

# Reserved column names for Turbopuffer
_ID_COLUMN = "id"
_VECTOR_COLUMN = "vector"


class TurbopufferDatasink(Datasink):
    """Turbopuffer Ray Datasink.

    A Ray :class:`~ray.data.datasource.Datasink` for writing data into the
    `Turbopuffer <https://turbopuffer.com/>`_ vector database. It supports both:

    * **Single-namespace mode** – all rows are written into a single Turbopuffer
      namespace.
    * **Multi-namespace mode** – rows are grouped by a column and written into
      separate namespaces derived from that column.

    Args:
        namespace: Name of a single Turbopuffer namespace to write into.
            Mutually exclusive with ``namespace_column``.
        namespace_column: Name of the column whose values determine the target
            namespace for each row (multi-namespace mode). Mutually exclusive
            with ``namespace``. Must not be the same as ``id_column`` or
            ``vector_column``.
        namespace_format: Python format string used to construct namespace
            names in multi-namespace mode. It must contain the
            ``\"{namespace}\"`` placeholder, which is replaced with the raw
            column value (or its UUID/hex string representation).
        api_key: Turbopuffer API key. If omitted, the value is read from the
            ``TURBOPUFFER_API_KEY`` environment variable.
        region: Turbopuffer region identifier (for example, ``\"gcp-us-central1\"``).
        schema: Optional Turbopuffer schema definition to pass along with
            writes. If provided, it is forwarded as the ``schema`` argument
            to ``namespace.write``.
        id_column: Name of the column to treat as the document identifier.
            Rows with null IDs are dropped before writing. Defaults to ``\"id\"``.
        vector_column: Name of the column containing embedding vectors.
            If this differs from ``\"vector\"``, it is renamed to ``\"vector\"``
            before writing. Defaults to ``\"vector\"``.
        batch_size: Maximum number of rows to include in a single Turbopuffer
            write call (logical row batching; subject to Turbopuffer's
            256MiB request-size limit). Defaults to ``10000``.
        concurrency: Unused; Ray Data controls write parallelism via
            :meth:`~ray.data.Dataset.write_datasink` ``concurrency``.

    Examples:
        Single-namespace write:

        .. testcode::
           :skipif: True

           import ray
           from ray.data._internal.datasource.turbopuffer_datasink import (
               TurbopufferDatasink,
           )

           ds = ray.data.range(100)
           ds = ds.map_batches(lambda batch: {\"id\": batch[\"id\"], \"vector\": ...})

           ds.write_datasink(
               TurbopufferDatasink(
                   namespace=\"my-namespace\",
                   api_key=\"<YOUR_API_KEY>\",
                   region=\"gcp-us-central1\",
               )
           )

        Multi-namespace write (for example, multi-tenant workspaces):

        .. testcode::
           :skipif: True

           import ray
           from ray.data._internal.datasource.turbopuffer_datasink import (
               TurbopufferDatasink,
           )

           # Each row has a \"space_id\" and an embedding vector.
           ds = ...

           ds.write_datasink(
               TurbopufferDatasink(
                   namespace_column=\"space_id\",
                   namespace_format=\"space-{namespace}\",
                   api_key=\"<YOUR_API_KEY>\",
               )
           )
    """

    def __init__(
        self,
        region: str,
        namespace: Optional[str] = None,
        namespace_column: Optional[str] = None,
        namespace_format: str = "{namespace}",
        api_key: Optional[str] = None,
        schema: Optional[dict] = None,
        id_column: str = "id",
        vector_column: str = "vector",
        batch_size: int = 10000,
        distance_metric: str = "cosine_distance",
        concurrency: Optional[int] = None,
    ):

        _check_import(self, module="turbopuffer", package="turbopuffer")

        import turbopuffer

        self._turbopuffer = turbopuffer

        # Validate namespace configuration
        if namespace and namespace_column:
            raise ValueError(
                "Specify either 'namespace' OR 'namespace_column', not both"
            )

        if not namespace and not namespace_column:
            raise ValueError("Must specify either 'namespace' or 'namespace_column'")

        # Validate namespace format has placeholder if using namespace_column
        if namespace_column and "{namespace}" not in namespace_format:
            raise ValueError(
                "namespace_format must contain '{namespace}' placeholder "
                "when using namespace_column"
            )

        # Validate namespace_column doesn't overlap with id_column or vector_column
        if namespace_column and namespace_column == id_column:
            raise ValueError(
                f"namespace_column ('{namespace_column}') cannot be the same as "
                f"id_column ('{id_column}'). The namespace column identifies the "
                "target namespace, while id_column identifies documents within a namespace."
            )

        if namespace_column and namespace_column == vector_column:
            raise ValueError(
                f"namespace_column ('{namespace_column}') cannot be the same as "
                f"vector_column ('{vector_column}'). The namespace column must be a "
                "separate column from the embedding vector."
            )

        # Store configuration
        self.namespace = namespace
        self.namespace_column = namespace_column
        self.namespace_format = namespace_format
        self.api_key = api_key or os.getenv("TURBOPUFFER_API_KEY")
        self.region = region
        self.schema = schema
        self.id_column = id_column
        self.vector_column = vector_column
        self.batch_size = batch_size
        self.distance_metric = distance_metric

        # Validate column configuration
        if self.id_column == self.vector_column:
            raise ValueError(
                "id_column and vector_column refer to the same column "
                f"'{self.id_column}'. They must be distinct."
            )

        # Validate API key
        if not self.api_key:
            raise ValueError(
                "API key is required. Provide via api_key parameter or "
                "TURBOPUFFER_API_KEY environment variable"
            )

        # Initialize client
        self._client = None

    def __getstate__(self) -> dict:
        """Exclude `_client` and `_turbopuffer` during pickling."""
        state = self.__dict__.copy()
        state.pop("_client", None)
        state.pop("_turbopuffer", None)
        return state

    def __setstate__(self, state: dict) -> None:
        self.__dict__.update(state)
        self._client = None
        # Re-import turbopuffer module
        import turbopuffer
        self._turbopuffer = turbopuffer

    def _get_client(self):
        """Lazy initialize Turbopuffer client."""
        if self._client is None:
            self._client = self._turbopuffer.Turbopuffer(
                api_key=self.api_key, region=self.region
            )
        return self._client

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
    ) -> None:
        """
        Write blocks to Turbopuffer in a streaming fashion.

        For memory efficiency, blocks are processed one at a time rather than
        concatenating all blocks into a single large table. This follows the
        pattern used by ClickHouseDatasink.

        Single-namespace mode:
            Each block is prepared, then written in batches of `batch_size`.

        Multi-namespace mode:
            Each block is prepared, grouped by namespace column, and each
            namespace group is written in batches. This processes one block
            at a time to avoid memory blowup while still handling multi-tenant
            writes correctly.
        """
        # Initialize client
        client = self._get_client()

        total_rows_written = 0
        block_count = 0

        for block in blocks:
            block_count += 1
            accessor = BlockAccessor.for_block(block)
            table = accessor.to_arrow()

            if table.num_rows == 0:
                continue

            # Prepare table (rename columns, filter nulls)
            table = self._prepare_arrow_table(table)

            if table.num_rows == 0:
                continue

            # Multi-namespace mode: group by namespace column
            if self.namespace_column:
                self._write_multi_namespace(client, table)
            else:
                # Single namespace mode
                self._write_single_namespace(client, table, self.namespace)

            total_rows_written += table.num_rows

        if block_count == 0:
            logger.debug("No blocks to write")
        elif total_rows_written == 0:
            logger.debug("No rows written after filtering")
        else:
            logger.debug(
                f"Wrote {total_rows_written} rows from {block_count} blocks"
            )

    def _rename_column_if_needed(
        self,
        table: pa.Table,
        source_column: str,
        target_column: str,
        column_type: str,
    ) -> pa.Table:
        """
        Rename a column in the table if it differs from the target name.

        Args:
            table: The Arrow table to modify.
            source_column: The current column name in the table.
            target_column: The required column name for Turbopuffer.
            column_type: Human-readable type for error messages (e.g., "ID", "Vector").

        Returns:
            The table with the column renamed, or the original table if no rename needed.

        Raises:
            ValueError: If source column is missing or target column already exists.
        """
        if source_column == target_column:
            return table

        if source_column not in table.column_names:
            raise ValueError(
                f"{column_type} column '{source_column}' not found in table"
            )
        if target_column in table.column_names:
            raise ValueError(
                f"Table already has a '{target_column}' column; cannot also rename "
                f"'{source_column}' to '{target_column}'. Please disambiguate your schema."
            )

        return BlockAccessor.for_block(table).rename_columns({source_column: target_column})

    def _prepare_arrow_table(self, table: pa.Table) -> pa.Table:
        """
        Prepare Arrow table for Turbopuffer write.

        1. Rename ID column to "id" if needed
        2. Rename vector column to "vector" if needed
        3. Filter out rows with null IDs
        """
        table = self._rename_column_if_needed(
            table, self.id_column, _ID_COLUMN, "ID"
        )
        table = self._rename_column_if_needed(
            table, self.vector_column, _VECTOR_COLUMN, "Vector"
        )

        # Filter out rows with null IDs
        if _ID_COLUMN in table.column_names:
            table = table.filter(pc.is_valid(table.column(_ID_COLUMN)))

        return table

    def _write_multi_namespace(self, client, table: pa.Table):
        """
        Write table to multiple namespaces grouped by namespace_column.
        Uses PyArrow's group_by() for efficient grouping in a single pass.
        """
        # Validation in __init__ ensures namespace_column doesn't overlap with
        # id_column or vector_column, so no column name resolution is needed.
        group_col_name = self.namespace_column

        if group_col_name not in table.column_names:
            raise ValueError(
                f"Namespace column '{self.namespace_column}' not found in table. "
                f"Available columns: {table.column_names}"
            )

        # Disallow null namespace values before grouping.
        namespace_col = table.column(group_col_name)
        null_mask = pc.is_null(namespace_col)
        if pc.any(null_mask).as_py():
            raise ValueError(
                f"Namespace column '{self.namespace_column}' contains null values; "
                "fill or drop them before writing with namespace_column."
            )

        # Sort the table by the grouping column. This groups all rows with the
        # same namespace value together, enabling efficient slicing.
        sort_key = SortKey(key=group_col_name, descending=False)
        sorted_table = transform_pyarrow.sort(table, sort_key)

        block_accessor = ArrowBlockAccessor(sorted_table)
        group_col = sorted_table.column(group_col_name)
        col_accessor = ArrowBlockColumnAccessor(group_col)
        counts_dict = col_accessor.value_counts()

        if not counts_dict:
            logger.debug("No groups found in table")
            return

        groups = sorted(
            zip(counts_dict["values"], counts_dict["counts"]),
            key=lambda x: x[0],
        )

        num_groups = len(groups)
        logger.debug(f"Writing to {num_groups} namespaces")

        start_idx = 0
        for namespace_value, count in groups:
            group_table = block_accessor.slice(start_idx, start_idx + count)
            start_idx += count

            # Format namespace name
            # Convert bytes to appropriate string format
            if isinstance(namespace_value, bytes):
                if len(namespace_value) == 16:
                    # This is a UUID in binary format
                    namespace_str = str(uuid.UUID(bytes=namespace_value))
                else:
                    # Non-UUID bytes: convert to hex string for consistency
                    # with _transform_to_turbopuffer_format
                    namespace_str = namespace_value.hex()
            else:
                namespace_str = str(namespace_value)

            namespace_name = self.namespace_format.format(namespace=namespace_str)

            # Write this group
            self._write_single_namespace(client, group_table, namespace_name)

    def _write_single_namespace(self, client, table: pa.Table, namespace_name: str):
        """
        Write table to a single namespace with batching.
        """
        logger.debug(f"Writing {len(table)} rows to namespace '{namespace_name}'")

        # Get namespace
        ns = client.namespace(namespace_name)

        # Split into batches
        for batch in table.to_batches(max_chunksize=self.batch_size):
            # Transform to Turbopuffer format
            batch_data = self._transform_to_turbopuffer_format(batch)

            # Write with retry
            self._write_batch_with_retry(ns, batch_data, namespace_name)

    def _transform_to_turbopuffer_format(
        self, table: Union[pa.Table, pa.RecordBatch]
    ) -> dict:
        """Transform Arrow table to Turbopuffer column-based format.

        Uses upsert_columns format per Turbopuffer docs:
        "Bulk document operations should use a column-oriented layout for best performance."
        """
        if _ID_COLUMN not in table.column_names:
            raise ValueError(f"Table must have '{_ID_COLUMN}' column")

        columns = table.to_pydict()

        # Convert ID column bytes to native UUID per Turbopuffer performance docs
        id_col = columns[_ID_COLUMN]
        columns[_ID_COLUMN] = [
            uuid.UUID(bytes=v) if isinstance(v, bytes) and len(v) == 16 else v
            for v in id_col
        ]

        return columns

    def _write_batch_with_retry(
        self,
        namespace: "turbopuffer.Namespace",
        batch_data: dict,
        namespace_name: str,
    ):
        """
        Write a single batch with exponential backoff retry using Ray's common utility.
        """

        # Get row count from first column for logging
        num_rows = len(next(iter(batch_data.values()))) if batch_data else 0

        try:
            call_with_retry(
                lambda: namespace.write(
                    upsert_columns=batch_data,
                    schema=self.schema,
                    distance_metric=self.distance_metric,
                ),
                description=f"write batch to namespace '{namespace_name}'",
                max_attempts=5,
                max_backoff_s=32,
            )
            logger.debug(
                f"Successfully wrote {num_rows} rows to namespace '{namespace_name}'"
            )
        except Exception as e:
            logger.error(
                f"Write failed for namespace '{namespace_name}' after retries: {e}"
            )
            raise


