"""
TurbopufferDatasink - Ray Data datasink for Turbopuffer vector database

Implementation following the pattern of MongoDatasink and Daft's Turbopuffer sink.

This is based on [Turbopuffer Write API](https://turbopuffer.com/docs/write)
"""

import logging
import os
from typing import TYPE_CHECKING, Iterable, Optional, Union

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
TURBOPUFFER_API_KEY_ENV_VAR = "TURBOPUFFER_API_KEY"


class TurbopufferDatasink(Datasink):
    """Turbopuffer Ray Datasink.

    A Ray :class:`~ray.data.datasource.Datasink` for writing data into the
    `Turbopuffer <https://turbopuffer.com/>`_ vector database.

    Args:
        namespace: Name of the Turbopuffer namespace to write into.
        region: Turbopuffer region identifier (for example,
            ``"gcp-us-central1"``).
        api_key: Turbopuffer API key. If omitted, the value is read from the
            ``TURBOPUFFER_API_KEY`` environment variable.
        schema: Optional Turbopuffer schema definition to pass along with
            writes. If provided, it is forwarded as the ``schema`` argument
            to ``namespace.write``.
        id_column: Name of the column to treat as the document identifier.
            Rows with null IDs are dropped before writing. Defaults to ``"id"``.
        vector_column: Name of the column containing embedding vectors.
            If this differs from ``"vector"``, it is renamed to ``"vector"``
            before writing. Defaults to ``"vector"``.
        batch_size: Maximum number of rows to include in a single Turbopuffer
            write call (logical row batching; subject to Turbopuffer's
            256MiB request-size limit). Defaults to ``10000``.
        distance_metric: Distance metric for the namespace. Passed to
            ``namespace.write`` as the ``distance_metric`` argument.
            Defaults to ``"cosine_distance"``.
        concurrency: Unused; Ray Data controls write parallelism via
            :meth:`~ray.data.Dataset.write_datasink` ``concurrency``.

    Examples:
        .. testcode::
           :skipif: True

           import ray
           from ray.data._internal.datasource.turbopuffer_datasink import (
               TurbopufferDatasink,
           )

           ds = ray.data.range(100)
           ds = ds.map_batches(lambda batch: {"id": batch["id"], "vector": ...})

           ds.write_datasink(
               TurbopufferDatasink(
                   namespace="my-namespace",
                   api_key="<YOUR_API_KEY>",
                   region="gcp-us-central1",
               )
           )
    """

    def __init__(
        self,
        namespace: str,
        region: str,
        api_key: Optional[str] = None,
        schema: Optional[dict] = None,
        id_column: str = "id",
        vector_column: str = "vector",
        batch_size: int = 10000,
        distance_metric: str = "cosine_distance",
        concurrency: Optional[int] = None,
    ):
        _check_import(self, module="turbopuffer", package="turbopuffer")

        if not namespace:
            raise ValueError("namespace is required")

        # Store configuration
        self.namespace = namespace
        self.api_key = api_key or os.getenv(TURBOPUFFER_API_KEY_ENV_VAR)
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
        """Exclude `_client` during pickling."""
        state = self.__dict__.copy()
        state.pop("_client", None)
        return state

    def __setstate__(self, state: dict) -> None:
        self.__dict__.update(state)
        self._client = None

    def _get_client(self):
        """Lazy initialize Turbopuffer client."""
        if self._client is None:
            import turbopuffer

            self._client = turbopuffer.Turbopuffer(
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

        Each block is prepared (columns renamed, null IDs filtered), then
        written in batches of `batch_size`.
        """
        client = self._get_client()
        ns = client.namespace(self.namespace)

        for block in blocks:
            accessor = BlockAccessor.for_block(block)
            table = accessor.to_arrow()

            if table.num_rows == 0:
                continue

            # Prepare table (rename columns, filter nulls)
            table = self._prepare_arrow_table(table)

            if table.num_rows == 0:
                continue

            # Write in batches
            for batch in table.to_batches(max_chunksize=self.batch_size):
                self._write_batch_with_retry(ns, batch)

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
        if source_column not in table.column_names:
            raise ValueError(
                f"{column_type} column '{source_column}' not found in table"
            )

        # No rename needed if source and target are the same
        if source_column == target_column:
            return table

        if target_column in table.column_names:
            raise ValueError(
                f"Table already has a '{target_column}' column; cannot also rename "
                f"'{source_column}' to '{target_column}'. Please disambiguate your schema."
            )

        return BlockAccessor.for_block(table).rename_columns(
            {source_column: target_column}
        )

    def _prepare_arrow_table(self, table: pa.Table) -> pa.Table:
        """
        Prepare Arrow table for Turbopuffer write.

        1. Rename ID column to "id" if needed
        2. Rename vector column to "vector" if needed
        3. Filter out rows with null IDs
        """
        table = self._rename_column_if_needed(table, self.id_column, _ID_COLUMN, "ID")
        table = self._rename_column_if_needed(
            table, self.vector_column, _VECTOR_COLUMN, "Vector"
        )

        # Filter out rows with null IDs
        if _ID_COLUMN in table.column_names:
            table = table.filter(pc.is_valid(table.column(_ID_COLUMN)))

        return table

    def _transform_to_turbopuffer_format(
        self, table: Union[pa.Table, pa.RecordBatch]
    ) -> dict:
        if _ID_COLUMN not in table.column_names:
            raise ValueError(f"Table must have '{_ID_COLUMN}' column")

        # Cast 16-byte binary ID column to native UUID type for Turbopuffer performance.
        # Native UUIDs are 16 bytes vs 36 bytes for string-encoded UUIDs.
        # See: https://turbopuffer.com/docs/performance
        id_col = table.column(_ID_COLUMN)
        if pa.types.is_fixed_size_binary(id_col.type) and id_col.type.byte_width == 16:
            # Cast fixed_size_binary(16) to uuid type
            uuid_col = id_col.cast(pa.uuid())
            table = table.set_column(
                table.schema.get_field_index(_ID_COLUMN), _ID_COLUMN, uuid_col
            )

        # to_pydict() on UuidArray automatically returns uuid.UUID objects
        return table.to_pydict()

    def _write_batch_with_retry(
        self,
        namespace: "turbopuffer.Namespace",
        batch: pa.Table,
    ):
        """
        Write a single batch with exponential backoff retry using Ray's common utility.
        """
        try:
            batch_data = self._transform_to_turbopuffer_format(batch)
            call_with_retry(
                lambda: namespace.write(
                    upsert_columns=batch_data,
                    schema=self.schema,
                    distance_metric=self.distance_metric,
                ),
                description=f"write batch to namespace '{self.namespace}'",
                max_attempts=5,
                max_backoff_s=32,
            )
        except Exception as e:
            logger.error(
                f"Write failed for namespace '{self.namespace}' after retries: {e}"
            )
            raise
