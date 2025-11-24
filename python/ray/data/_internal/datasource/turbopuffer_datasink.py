"""
TurbopufferDatasink - Ray Data datasink for Turbopuffer vector database

Implementation following the pattern of MongoDatasink and Daft's Turbopuffer sink.
Supports multi-namespace mode for writing data from many workspaces to separate namespaces.
"""

import logging
import os
import random
import time
import uuid
from typing import Any, Dict, Iterable, Optional

import pyarrow as pa
import pyarrow.compute as pc

from ray.data._internal.execution.interfaces import TaskContext
from ray.data.block import Block, BlockAccessor
from ray.data.datasource.datasink import Datasink

logger = logging.getLogger(__name__)


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
                   namespace_format=\"notion-{namespace}\",
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
        concurrency: Optional[int] = None,
    ):
        # Import validation
        try:
            import turbopuffer

            self._turbopuffer = turbopuffer
        except ImportError:
            raise ImportError(
                "turbopuffer package is required. Install with: pip install turbopuffer"
            )

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

        # Validate API key
        if not self.api_key:
            raise ValueError(
                "API key is required. Provide via api_key parameter or "
                "TURBOPUFFER_API_KEY environment variable"
            )

        # Initialize client
        self._client = None

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

    def _prepare_arrow_table(self, table: pa.Table) -> pa.Table:
        """
        Prepare Arrow table for Turbopuffer write.

        1. Rename ID column to "id" if needed
        2. Rename vector column to "vector" if needed
        3. Filter out rows with null IDs
        """
        # Rename ID column if needed
        if self.id_column != "id":
            if self.id_column not in table.column_names:
                raise ValueError(f"ID column '{self.id_column}' not found in table")

            # Rename by reconstructing table with new schema
            idx = table.column_names.index(self.id_column)
            new_names = list(table.column_names)
            new_names[idx] = "id"
            table = table.rename_columns(new_names)

        # Rename vector column if needed
        if self.vector_column != "vector" and self.vector_column in table.column_names:
            idx = table.column_names.index(self.vector_column)
            new_names = list(table.column_names)
            new_names[idx] = "vector"
            table = table.rename_columns(new_names)

        # Filter out rows with null IDs
        if "id" in table.column_names:
            id_col = table.column("id")
            mask = pc.invert(pc.is_null(id_col))
            table = table.filter(mask)

        return table

    def _write_multi_namespace(self, client, table: pa.Table):
        """
        Write table to multiple namespaces grouped by namespace_column.
        """
        if self.namespace_column not in table.column_names:
            raise ValueError(
                f"Namespace column '{self.namespace_column}' not found in table. "
                f"Available columns: {table.column_names}"
            )

        # Group by namespace column
        # Note: PyArrow doesn't have a built-in group_by for tables,
        # so we'll use a simpler approach: get unique values and filter
        namespace_col = table.column(self.namespace_column)

        # Get unique namespace values
        unique_namespaces = pc.unique(namespace_col)

        logger.debug(f"Writing to {len(unique_namespaces)} namespaces")

        # Process each namespace group
        for i in range(len(unique_namespaces)):
            namespace_value = unique_namespaces[i].as_py()

            # Filter table for this namespace
            mask = pc.equal(namespace_col, namespace_value)
            group_table = table.filter(mask)

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
        num_rows = len(table)
        for batch_start in range(0, num_rows, self.batch_size):
            batch_end = min(batch_start + self.batch_size, num_rows)
            batch_table = table.slice(batch_start, batch_end - batch_start)

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
        # Convert to list of row dictionaries
        rows = table.to_pylist()

        # Validate all rows have ID
        if rows and "id" not in rows[0]:
            raise ValueError("Table must have 'id' column")

        # Convert bytes to proper formats (e.g., UUIDs)
        import uuid as uuid_lib

        for row in rows:
            for key, value in list(row.items()):
                if isinstance(value, bytes):
                    # Check if it's a 16-byte UUID
                    if len(value) == 16:
                        row[key] = str(uuid_lib.UUID(bytes=value))
                    else:
                        # For other bytes, convert to hex string
                        row[key] = value.hex()
                elif isinstance(value, list) and len(value) > 0:
                    # Handle arrays of UUIDs (e.g., permissions_user_ids)
                    converted_list = []
                    for item in value:
                        if isinstance(item, bytes) and len(item) == 16:
                            # Convert 16-byte UUID to string
                            converted_list.append(str(uuid_lib.UUID(bytes=item)))
                        else:
                            # Keep non-UUID items as-is
                            converted_list.append(item)
                    row[key] = converted_list

        return rows

    def _write_batch_with_retry(
        self,
        namespace,
        batch_data: list,
        namespace_name: str,
    ):
        """
        Write a single batch with exponential backoff retry and jitter.

        Internal constants (not user-configurable):
        - MAX_RETRIES = 5
        - INITIAL_BACKOFF = 1.0 seconds
        - MAX_BACKOFF = 32.0 seconds
        - Jitter: 0-100% random
        """
        MAX_RETRIES = 5
        INITIAL_BACKOFF = 1.0
        MAX_BACKOFF = 32.0

        num_rows = len(batch_data)

        for attempt in range(MAX_RETRIES):
            try:
                # Write to Turbopuffer using upsert_rows (row-based format)
                namespace.write(
                    upsert_rows=batch_data,
                    schema=self.schema,
                    distance_metric="cosine_distance",
                )

                logger.debug(
                    f"Successfully wrote {num_rows} rows to namespace '{namespace_name}'"
                )
                return  # Success

            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    # Calculate exponential backoff with jitter
                    backoff = min(MAX_BACKOFF, INITIAL_BACKOFF * (2**attempt))
                    jitter = random.uniform(0, backoff)  # 0-100% jitter
                    wait_time = backoff + jitter

                    logger.warning(
                        f"Write failed for namespace '{namespace_name}' "
                        f"(attempt {attempt + 1}/{MAX_RETRIES}), "
                        f"retrying in {wait_time:.2f}s: {e}"
                    )
                    time.sleep(wait_time)
                else:
                    # All retries exhausted
                    logger.error(
                        f"Write failed for namespace '{namespace_name}' "
                        f"after {MAX_RETRIES} attempts: {e}"
                    )
                    raise


