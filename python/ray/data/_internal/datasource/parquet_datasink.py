import logging
import posixpath
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterable, List, Optional

from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.util import call_with_retry
from ray.data.block import Block, BlockAccessor
from ray.data.context import DataContext
from ray.data.datasource.file_based_datasource import _resolve_kwargs
from ray.data.datasource.file_datasink import _FileDatasink
from ray.data.datasource.filename_provider import FilenameProvider

if TYPE_CHECKING:
    import pyarrow

WRITE_FILE_MAX_ATTEMPTS = 10
WRITE_FILE_RETRY_MAX_BACKOFF_SECONDS = 32

logger = logging.getLogger(__name__)


class ParquetDatasink(_FileDatasink):
    def __init__(
        self,
        path: str,
        *,
        arrow_parquet_args_fn: Optional[Callable[[], Dict[str, Any]]] = None,
        arrow_parquet_args: Optional[Dict[str, Any]] = None,
        num_rows_per_file: Optional[int] = None,
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        try_create_dir: bool = True,
        open_stream_args: Optional[Dict[str, Any]] = None,
        filename_provider: Optional[FilenameProvider] = None,
        dataset_uuid: Optional[str] = None,
    ):
        if arrow_parquet_args_fn is None:
            arrow_parquet_args_fn = lambda: {}  # noqa: E731

        if arrow_parquet_args is None:
            arrow_parquet_args = {}

        self.arrow_parquet_args_fn = arrow_parquet_args_fn
        self.arrow_parquet_args = arrow_parquet_args
        self.num_rows_per_file = num_rows_per_file

        super().__init__(
            path,
            filesystem=filesystem,
            try_create_dir=try_create_dir,
            open_stream_args=open_stream_args,
            filename_provider=filename_provider,
            dataset_uuid=dataset_uuid,
            file_format="parquet",
        )

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
    ) -> None:
        import pyarrow.parquet as pq

        blocks = list(blocks)

        if all(BlockAccessor.for_block(block).num_rows() == 0 for block in blocks):
            return

        filename = self.filename_provider.get_filename_for_block(
            blocks[0], ctx.task_idx, 0
        )
        write_path = posixpath.join(self.path, filename)
        write_kwargs = _resolve_kwargs(
            self.arrow_parquet_args_fn, **self.arrow_parquet_args
        )

        def write_blocks_to_path():
            with self.open_output_stream(write_path) as file:
                tables = [BlockAccessor.for_block(block).to_arrow() for block in blocks]
                schema = self._try_merge_nullable_fields(tables)
                with pq.ParquetWriter(file, schema, **write_kwargs) as writer:
                    for table in tables:
                        if not table.schema.equals(schema):
                            table = table.cast(schema)
                        writer.write_table(table)

        logger.debug(f"Writing {write_path} file.")
        call_with_retry(
            write_blocks_to_path,
            description=f"write '{write_path}'",
            match=DataContext.get_current().retried_io_errors,
            max_attempts=WRITE_FILE_MAX_ATTEMPTS,
            max_backoff_s=WRITE_FILE_RETRY_MAX_BACKOFF_SECONDS,
        )

    @property
    def num_rows_per_write(self) -> Optional[int]:
        return self.num_rows_per_file

    def _try_merge_nullable_fields(
        self, tables: List["pyarrow.Table"]
    ) -> "pyarrow.lib.Schema":
        """
        Merge the nullable fields of the list of tables from multiple blocks.

        If blocks's schema differ only by nullable status on a field,
        we will make a "relaxed" schema that's compatible.

        NOTE that this function only merges on nullable fields, not
        on anything else.

        Raises:
            ValueError: If the schemas differ on anything other than nullable fields.

        Returns:
            The merged schema.
        """
        merged_schema = tables[0].schema
        import pyarrow

        for table in tables[1:]:
            table_schema = table.schema
            if merged_schema.equals(table_schema):
                continue

            # Schema mismatch found. If fields only differ by nullable status,
            # we can continue.
            n_merged_schema = len(merged_schema.names)
            n_table_schema = len(table_schema.names)
            if n_merged_schema != n_table_schema:
                raise ValueError(
                    f"Schema mismatch found: {merged_schema} vs {table_schema}"
                )

            for field_idx in range(n_merged_schema):
                field = merged_schema.field(field_idx)
                table_field = table_schema.field(field_idx)

                if field.equals(table_field):
                    continue

                if field.name != table_field.name:
                    raise ValueError(
                        f"Schema mismatch found: {merged_schema} vs {table_schema}"
                    )

                # Check if fields only differ by nullable status.
                if field.type == pyarrow.null() and table_field.nullable:
                    merged_schema = merged_schema.set(
                        field_idx, field.with_type(table_field.type)
                    )

                if table_field.type == pyarrow.null() and field.nullable:
                    # Make the table schema nullable on the field.
                    table_schema = table_schema.set(
                        field_idx, table_field.with_type(field.type)
                    )

            # This makes sure we are only merging on nullable fields.
            if not merged_schema.equals(table_schema):
                raise ValueError(
                    f"Schema mismatch found: {merged_schema} vs {table_schema}"
                )

        return merged_schema
