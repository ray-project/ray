"""
TurbopufferDatasink - Ray Data datasink for Turbopuffer vector database

Implementation following the pattern of MongoDatasink and Daft's Turbopuffer sink.
Supports multi-namespace mode for writing data from many workspaces to separate namespaces.
"""

import logging
import os
import uuid
from typing import TYPE_CHECKING, Iterable, Optional

import pyarrow as pa
import pyarrow.compute as pc

from ray._common.retry import call_with_retry
from ray.data._internal.execution.interfaces import TaskContext
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
            with ``namespace``.
        namespace_format: Python format string used to construct namespace
            names in multi-namespace mode. It must contain the
            ``\"{namespace}\"`` placeholder, which is replaced with the raw
            column value (or its UUID/hex string representation).
        api_key: Turbopuffer API key. If omitted, the value is read from the
            ``TURBOPUFFER_API_KEY`` environment variable.
        region: Turbopuffer region identifier (for example, ``\"gcp-us-central1\"``).
            If omitted, defaults to ``\"gcp-us-central1\"``.
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
        namespace: Optional[str] = None,
        namespace_column: Optional[str] = None,
        namespace_format: str = "{namespace}",
        api_key: Optional[str] = None,
        region: Optional[str] = None,
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
        """Lazy initialize Turbopuffer client"""
        if self._client is None:
            # Initialize with api_key and region (default to "gcp-us-central1" if not specified)
            region = self.region or "gcp-us-central1"
            # Use the new v0.5+ API: turbopuffer.Turbopuffer(api_key=..., region=...)
            self._client = self._turbopuffer.Turbopuffer(
                api_key=self.api_key, region=region
            )
        return self._client

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
    ) -> None:
        """
        Write blocks to Turbopuffer.

        Main write logic:
        1. Aggregate blocks into single Arrow table
        2. Prepare table (rename columns, filter nulls)
        3. Group by namespace if multi-namespace mode
        4. Write each group with batching and retry logic
        """
        # Initialize client
        client = self._get_client()

        # Aggregate all blocks into a single Arrow table
        blocks_list = list(blocks)
        if not blocks_list:
            logger.debug("No blocks to write")
            return

        # Convert blocks to Arrow and concatenate
        tables = []
        for block in blocks_list:
            accessor = BlockAccessor.for_block(block)
            table = accessor.to_arrow()
            tables.append(table)

        if not tables:
            logger.debug("No tables to write")
            return

        # Concatenate all tables
        table = pa.concat_tables(tables)

        if len(table) == 0:
            logger.debug("Empty table, skipping write")
            return

        logger.debug(f"Writing {len(table)} rows from {len(blocks_list)} blocks")

        # Prepare table (rename columns, filter nulls)
        table = self._prepare_arrow_table(table)

        if len(table) == 0:
            logger.debug("No rows after filtering null IDs")
            return

        # Multi-namespace mode: group by namespace column
        if self.namespace_column:
            self._write_multi_namespace(client, table)
        else:
            # Single namespace mode
            self._write_single_namespace(client, table, self.namespace)

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

        return table.rename_columns({source_column: target_column})

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
        # Determine the actual column name in the prepared table that should be
        # used for namespace grouping. The configured namespace_column may have
        # been renamed in _prepare_arrow_table when it overlaps with the id or
        # vector columns.
        group_col_name = self.namespace_column

        # If the namespace column is the configured id_column and that was
        # renamed to "id", use "id" for grouping.
        if (
            self.id_column != _ID_COLUMN
            and self.namespace_column == self.id_column
            and _ID_COLUMN in table.column_names
        ):
            group_col_name = _ID_COLUMN
        # Likewise for the vector column.
        elif (
            self.vector_column != _VECTOR_COLUMN
            and self.namespace_column == self.vector_column
            and _VECTOR_COLUMN in table.column_names
        ):
            group_col_name = _VECTOR_COLUMN

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
        sort_indices = pc.sort_indices(table, sort_keys=[(group_col_name, "ascending")])
        sorted_table = table.take(sort_indices)

        # Use PyArrow's group_by() to efficiently identify unique groups and their counts.
        # Note: group_by().aggregate() uses hash aggregation with use_threads=True by default,
        # which does NOT guarantee output order matches input order. We must sort the result
        # to ensure it matches the sorted table order for correct slicing.
        grouped = sorted_table.group_by(group_col_name)
        agg_result = grouped.aggregate([(_ID_COLUMN, "count")])

        # Sort the aggregate result by the group column to match the sorted table order.
        # This is critical: without this sort, rows could be assigned to wrong namespaces.
        agg_sort_indices = pc.sort_indices(
            agg_result, sort_keys=[(group_col_name, "ascending")]
        )
        agg_result = agg_result.take(agg_sort_indices)

        group_keys = agg_result.column(group_col_name)
        group_counts = agg_result.column(f"{_ID_COLUMN}_count")
        num_groups = len(group_keys)
        
        logger.debug(f"Writing to {num_groups} namespaces")

        # Process each group by slicing the sorted table at group boundaries
        start_idx = 0
        for i in range(num_groups):
            namespace_value = group_keys[i].as_py()
            count = group_counts[i].as_py()
            
            # Slice the sorted table to get this group's rows
            group_table = sorted_table.slice(start_idx, count)
            start_idx += count

            # Format namespace name
            # Convert bytes to UUID string if needed
            if isinstance(namespace_value, bytes) and len(namespace_value) == 16:
                # This is a UUID in binary format
                namespace_str = str(uuid.UUID(bytes=namespace_value))
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
            batch_table = pa.Table.from_batches([batch])

            # Transform to Turbopuffer format
            batch_data = self._transform_to_turbopuffer_format(batch_table)

            # Write with retry
            self._write_batch_with_retry(ns, batch_data, namespace_name)

    def _transform_to_turbopuffer_format(self, table: pa.Table) -> list:
        """
        Transform Arrow table to Turbopuffer row-based format.

        Turbopuffer expects upsert_rows as a list of row dictionaries:
        [
            {"id": "1", "vector": [...], "attr1": val1, ...},
            {"id": "2", "vector": [...], "attr1": val2, ...},
        ]

        Converts binary UUID fields (16 bytes) to string format.
        """
        # Validate table has ID column by checking schema before conversion.
        if _ID_COLUMN not in table.column_names:
            raise ValueError(f"Table must have '{_ID_COLUMN}' column")

        # Convert to list of row dictionaries
        rows = table.to_pylist()

        # Convert bytes to proper formats (e.g., UUIDs)
        for row in rows:
            for key, value in list(row.items()):
                if isinstance(value, bytes):
                    # Check if it's a 16-byte UUID
                    if len(value) == 16:
                        row[key] = str(uuid.UUID(bytes=value))
                    else:
                        # For other bytes, convert to hex string
                        row[key] = value.hex()
                elif isinstance(value, list) and len(value) > 0:
                    # Handle arrays of UUIDs (e.g., permissions_user_ids)
                    converted_list = []
                    for item in value:
                        if isinstance(item, bytes):
                            # Check if it's a 16-byte UUID
                            if len(item) == 16:
                                # Convert 16-byte UUID to string
                                converted_list.append(str(uuid.UUID(bytes=item)))
                            else:
                                # For other bytes, convert to hex string
                                converted_list.append(item.hex())
                        else:
                            # Keep non-bytes items as-is
                            converted_list.append(item)
                    row[key] = converted_list

        return rows

    def _write_batch_with_retry(
        self,
        namespace: "turbopuffer.Namespace",
        batch_data: list,
        namespace_name: str,
    ):
        """
        Write a single batch with exponential backoff retry using Ray's common utility.
        """

        def _write_fn():
            namespace.write(
                upsert_rows=batch_data,
                schema=self.schema,
                distance_metric=self.distance_metric,
            )
            return  # Success

        try:
            call_with_retry(
                _write_fn,
                description=f"write batch to namespace '{namespace_name}'",
                max_attempts=5,
                max_backoff_s=32,
            )
            logger.debug(
                f"Successfully wrote {len(batch_data)} rows to namespace '{namespace_name}'"
            )
        except Exception as e:
            logger.error(
                f"Write failed for namespace '{namespace_name}' after retries: {e}"
            )
            raise


