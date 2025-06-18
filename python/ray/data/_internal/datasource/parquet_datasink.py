import logging
import posixpath
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterable, List, Optional

from ray.data._internal.arrow_ops.transform_pyarrow import concat
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.savemode import SaveMode
from ray.data._internal.util import call_with_retry
from ray.data.block import Block, BlockAccessor
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
        partition_cols: Optional[List[str]] = None,
        arrow_parquet_args_fn: Optional[Callable[[], Dict[str, Any]]] = None,
        arrow_parquet_args: Optional[Dict[str, Any]] = None,
        min_rows_per_file: Optional[int] = None,
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        try_create_dir: bool = True,
        open_stream_args: Optional[Dict[str, Any]] = None,
        filename_provider: Optional[FilenameProvider] = None,
        dataset_uuid: Optional[str] = None,
        mode: SaveMode = SaveMode.APPEND,
    ):
        if arrow_parquet_args_fn is None:
            arrow_parquet_args_fn = lambda: {}  # noqa: E731

        if arrow_parquet_args is None:
            arrow_parquet_args = {}

        self.arrow_parquet_args_fn = arrow_parquet_args_fn
        self.arrow_parquet_args = arrow_parquet_args
        self.min_rows_per_file = min_rows_per_file
        self.partition_cols = partition_cols

        super().__init__(
            path,
            filesystem=filesystem,
            try_create_dir=try_create_dir,
            open_stream_args=open_stream_args,
            filename_provider=filename_provider,
            dataset_uuid=dataset_uuid,
            file_format="parquet",
            mode=mode,
        )

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
    ) -> None:
        import pyarrow as pa

        blocks = list(blocks)

        if all(BlockAccessor.for_block(block).num_rows() == 0 for block in blocks):
            return

        blocks = [
            block for block in blocks if BlockAccessor.for_block(block).num_rows() > 0
        ]

        filename = self.filename_provider.get_filename_for_block(
            blocks[0], ctx.task_idx, 0
        )
        write_kwargs = _resolve_kwargs(
            self.arrow_parquet_args_fn, **self.arrow_parquet_args
        )
        user_schema = write_kwargs.pop("schema", None)

        def write_blocks_to_path():
            tables = [BlockAccessor.for_block(block).to_arrow() for block in blocks]
            if user_schema is None:
                output_schema = pa.unify_schemas([table.schema for table in tables])
            else:
                output_schema = user_schema

            if not self.partition_cols:
                self._write_single_file(
                    self.path, tables, filename, output_schema, write_kwargs
                )
            else:  # partition writes
                self._write_partition_files(
                    tables, filename, output_schema, write_kwargs
                )

        logger.debug(f"Writing {filename} file to {self.path}.")

        call_with_retry(
            write_blocks_to_path,
            description=f"write '{filename}' to '{self.path}'",
            match=self._data_context.retried_io_errors,
            max_attempts=WRITE_FILE_MAX_ATTEMPTS,
            max_backoff_s=WRITE_FILE_RETRY_MAX_BACKOFF_SECONDS,
        )

    def _write_single_file(
        self,
        path: str,
        tables: List["pyarrow.Table"],
        filename: str,
        output_schema: "pyarrow.Schema",
        write_kwargs: Dict[str, Any],
    ) -> None:
        import pyarrow.parquet as pq

        # We extract 'row_group_size' for write_table() and
        # keep the rest for ParquetWriter()
        row_group_size = write_kwargs.pop("row_group_size", None)

        write_path = posixpath.join(path, filename)
        with self.open_output_stream(write_path) as file:
            with pq.ParquetWriter(file, output_schema, **write_kwargs) as writer:
                for table in tables:
                    table = table.cast(output_schema)
                    writer.write_table(table, row_group_size=row_group_size)

    def _write_partition_files(
        self,
        tables: List["pyarrow.Table"],
        filename: str,
        output_schema: "pyarrow.Schema",
        write_kwargs: Dict[str, Any],
    ) -> None:
        import pyarrow as pa
        import pyarrow.compute as pc

        table = concat(tables, promote_types=False)
        # Create unique combinations of the partition columns
        partition_col_values: List[Dict[str, Any]] = (
            table.select(self.partition_cols)
            .group_by(self.partition_cols)
            .aggregate([])
        ).to_pylist()
        table_fields = [
            field for field in output_schema if field.name not in self.partition_cols
        ]
        non_partition_cols = [f.name for f in table_fields]
        output_schema = pa.schema(
            [field for field in output_schema if field.name not in self.partition_cols]
        )

        for combo in partition_col_values:
            filters = [pc.equal(table[col], value) for col, value in combo.items()]
            combined_filter = filters[0]
            for filter_ in filters[1:]:
                combined_filter = pc.and_(combined_filter, filter_)
            group_table = table.filter(combined_filter).select(non_partition_cols)
            partition_path = "/".join(
                [f"{col}={value}" for col, value in combo.items()]
            )
            write_path = posixpath.join(self.path, partition_path)
            self._create_dir(write_path)

            # Handle min_rows_per_file for partitioned data
            if (
                self.min_rows_per_file is not None
                and group_table.num_rows > self.min_rows_per_file
            ):
                # Split the partition into multiple files
                self._write_partition_with_row_limit(
                    group_table, write_path, filename, output_schema, write_kwargs
                )
            else:
                # Write as single file (existing behavior when min_rows_per_file is None
                # or when partition has fewer rows than min_rows_per_file)
                self._write_single_file(
                    write_path,
                    [group_table],
                    filename,
                    output_schema,
                    write_kwargs,
                )

    def _write_partition_with_row_limit(
        self,
        table: "pyarrow.Table",
        write_path: str,
        base_filename: str,
        output_schema: "pyarrow.Schema",
        write_kwargs: Dict[str, Any],
    ) -> None:
        """Write a partition table to multiple files respecting min_rows_per_file."""
        from ray.data._internal.execution.operators.map_transformer import _splitrange
        from ray.data.block import BlockAccessor

        total_rows = table.num_rows

        # Calculate how many files we need based on min_rows_per_file
        # Each file should have at least min_rows_per_file rows (except possibly the last)
        num_files = max(
            1, (total_rows + self.min_rows_per_file - 1) // self.min_rows_per_file
        )

        # Split the table into chunks, distributing rows as evenly as possible
        split_sizes = _splitrange(total_rows, num_files)

        block_accessor = BlockAccessor.for_block(table)
        offset = 0

        for file_idx, chunk_size in enumerate(split_sizes):
            if chunk_size == 0:
                continue

            # Create a slice of the table
            chunk_table = block_accessor.slice(offset, offset + chunk_size)

            # Generate filename with index suffix
            name_parts = base_filename.rsplit(".", 1)
            if len(name_parts) == 2:
                chunk_filename = f"{name_parts[0]}_{file_idx:06d}.{name_parts[1]}"
            else:
                chunk_filename = f"{base_filename}_{file_idx:06d}"

            # Write the chunk
            self._write_single_file(
                write_path,
                [chunk_table],
                chunk_filename,
                output_schema,
                write_kwargs,
            )

            offset += chunk_size

    @property
    def min_rows_per_write(self) -> Optional[int]:
        return self.min_rows_per_file
