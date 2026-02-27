"""
TurbopufferDatasink - Ray Data datasink for Turbopuffer vector database

Implementation following the pattern of MongoDatasink and Daft's Turbopuffer sink.

This is based on [Turbopuffer Write API](https://turbopuffer.com/docs/write)
"""

import logging
import os
from typing import TYPE_CHECKING, Iterable, Literal, Optional, Union

import pyarrow as pa
import pyarrow.compute as pc

from ray._common.retry import call_with_retry
from ray.data._internal.arrow_block import ArrowBlockAccessor
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
TURBOPUFFER_API_KEY_ENV_VAR = "TURBOPUFFER_API_KEY"


class TurbopufferDatasink(Datasink):
    """Turbopuffer Ray Datasink.

    A Ray :class:`~ray.data.datasource.Datasink` for writing data into the
    `Turbopuffer <https://turbopuffer.com/>`_ vector database.

    Supports two modes of operation:

    * **Single namespace** -- provide ``namespace`` to write all rows into one
      Turbopuffer namespace.
    * **Multi-namespace** -- provide ``namespace_column`` to route each row to
      the namespace whose name is stored in that column.  The column is
      automatically dropped before the data is sent to Turbopuffer.

    Exactly one of ``namespace`` or ``namespace_column`` must be supplied.

    Args:
        namespace: Name of the Turbopuffer namespace to write into.
            Mutually exclusive with ``namespace_column``.
        namespace_column: Name of a column whose values determine the
            target namespace for each row.  Rows are grouped by this column
            and each group is written to its corresponding namespace.  The
            column is removed from the data before writing.  Mutually
            exclusive with ``namespace``.
        region: Turbopuffer region identifier (for example,
            ``"gcp-us-central1"``).  Mutually exclusive with ``base_url``.
            Exactly one of ``region`` or ``base_url`` must be supplied.
        base_url: Base URL for the Turbopuffer API (for example,
            ``"https://gcp-us-central1.turbopuffer.com"``).  Mutually
            exclusive with ``region``.  Exactly one of ``region`` or
            ``base_url`` must be supplied.
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
        Write to a single namespace using a region:

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

        Write using a base URL instead of a region:

        .. testcode::
           :skipif: True

           ds.write_datasink(
               TurbopufferDatasink(
                   namespace="my-namespace",
                   api_key="<YOUR_API_KEY>",
                   base_url="https://gcp-us-central1.turbopuffer.com",
               )
           )

        Write to multiple namespaces driven by a column:

        .. testcode::
           :skipif: True

           ds.write_datasink(
               TurbopufferDatasink(
                   namespace_column="tenant",
                   api_key="<YOUR_API_KEY>",
                   region="gcp-us-central1",
               )
           )
    """

    def __init__(
        self,
        namespace: Optional[str] = None,
        *,
        namespace_column: Optional[str] = None,
        region: Optional[str] = None,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        schema: Optional[dict] = None,
        id_column: str = "id",
        vector_column: str = "vector",
        batch_size: int = 10000,
        distance_metric: Literal[
            "cosine_distance", "euclidean_distance"
        ] = "cosine_distance",
        concurrency: Optional[int] = None,
    ):
        _check_import(self, module="turbopuffer", package="turbopuffer")

        # Validate namespace / namespace_column mutual exclusivity.
        if namespace and namespace_column:
            raise ValueError(
                "Specify exactly one of 'namespace' or 'namespace_column', " "not both."
            )
        if not namespace and not namespace_column:
            raise ValueError(
                "Either 'namespace' or 'namespace_column' must be provided."
            )

        # Validate region / base_url mutual exclusivity.
        if region is not None and base_url is not None:
            raise ValueError("Specify exactly one of 'region' or 'base_url', not both.")
        if region is None and base_url is None:
            raise ValueError("Either 'region' or 'base_url' must be provided.")

        # Store configuration
        self.namespace = namespace
        self.namespace_column = namespace_column
        self.api_key = api_key or os.getenv(TURBOPUFFER_API_KEY_ENV_VAR)
        self.region = region
        self.base_url = base_url
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

        if self.namespace_column and self.namespace_column in (
            self.id_column,
            self.vector_column,
        ):
            raise ValueError(
                f"namespace_column '{self.namespace_column}' must not be the "
                f"same as id_column ('{self.id_column}') or vector_column "
                f"('{self.vector_column}')."
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

            kwargs = {"api_key": self.api_key}
            if self.region is not None:
                kwargs["region"] = self.region
            else:
                kwargs["base_url"] = self.base_url
            self._client = turbopuffer.Turbopuffer(**kwargs)
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
        written in batches of ``batch_size``.

        When ``namespace_column`` is set, each block is grouped by the
        namespace column and each group is written to its corresponding
        Turbopuffer namespace.
        """
        client = self._get_client()

        for block in blocks:
            accessor = BlockAccessor.for_block(block)
            table = accessor.to_arrow()

            if table.num_rows == 0:
                continue

            if self.namespace_column:
                # Multi-namespace: group by namespace column, write to each.
                self._write_multi_namespace(client, table)
            else:
                # Single namespace.
                table = self._prepare_arrow_table(table)

                if table.num_rows == 0:
                    continue

                ns = client.namespace(self.namespace)
                for batch in table.to_batches(max_chunksize=self.batch_size):
                    self._write_batch_with_retry(ns, batch, self.namespace)

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

    def _write_multi_namespace(
        self, client: "turbopuffer.Turbopuffer", table: pa.Table
    ) -> None:
        """Group rows by ``namespace_column`` and write each group to its namespace.

        Uses :meth:`BlockAccessor._iter_groups_sorted` for efficient
        zero-copy slicing by group.
        """
        group_col_name = self.namespace_column

        if group_col_name not in table.column_names:
            raise ValueError(
                f"Namespace column '{group_col_name}' not found in table. "
                f"Available columns: {table.column_names}"
            )

        # Reject null namespace values early -- we cannot route them.
        ns_col = table.column(group_col_name)
        if pc.any(pc.is_null(ns_col)).as_py():
            raise ValueError(
                f"Namespace column '{group_col_name}' contains null values; "
                "fill or drop them before writing with namespace_column."
            )

        # Sort by the namespace column so _iter_groups_sorted can yield
        # contiguous zero-copy slices for each unique namespace value.
        sort_key = SortKey(key=group_col_name, descending=False)
        sorted_table = transform_pyarrow.sort(table, sort_key)
        block_accessor = ArrowBlockAccessor.for_block(sorted_table)

        for (namespace_name,), group_table in block_accessor._iter_groups_sorted(
            sort_key
        ):
            # Drop the namespace column -- it is routing metadata, not data.
            group_table = group_table.drop(group_col_name)

            # Prepare (rename id/vector columns, filter null IDs).
            group_table = self._prepare_arrow_table(group_table)
            if group_table.num_rows == 0:
                continue

            ns = client.namespace(namespace_name)
            for batch in group_table.to_batches(max_chunksize=self.batch_size):
                self._write_batch_with_retry(ns, batch, namespace_name)

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
        namespace_name: Optional[str] = None,
    ):
        """Write a single batch with exponential backoff retry.

        Args:
            namespace: The Turbopuffer namespace object to write to.
            batch: Arrow table or record-batch to write.
            namespace_name: Human-readable namespace name for log messages.
                Falls back to ``self.namespace`` when not provided.
        """
        ns_label = namespace_name or self.namespace
        try:
            batch_data = self._transform_to_turbopuffer_format(batch)
            call_with_retry(
                lambda: namespace.write(
                    upsert_columns=batch_data,
                    schema=self.schema,
                    distance_metric=self.distance_metric,
                ),
                description=f"write batch to namespace '{ns_label}'",
                max_attempts=5,
                max_backoff_s=32,
            )
        except Exception as e:
            logger.error(f"Write failed for namespace '{ns_label}' after retries: {e}")
            raise
