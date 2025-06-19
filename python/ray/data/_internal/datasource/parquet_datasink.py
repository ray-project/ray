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
        max_rows_per_file: Optional[int] = None,
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
        self.max_rows_per_file = max_rows_per_file
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
                # Handle row limits for non-partitioned writes
                combined_table = concat(tables, promote_types=False)
                self._write_with_row_limits(
                    self.path, combined_table, filename, output_schema, write_kwargs
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

    def _write_with_row_limits(
        self,
        path: str,
        table: "pyarrow.Table",
        filename: str,
        output_schema: "pyarrow.Schema",
        write_kwargs: Dict[str, Any],
    ) -> None:
        """Write table respecting both min and max rows per file limits."""
        total_rows = table.num_rows

        # If no limits are set, write as single file
        if self.min_rows_per_file is None and self.max_rows_per_file is None:
            self._write_single_file(
                path, [table], filename, output_schema, write_kwargs
            )
            return

        # Determine the effective row limit based on priority: max takes precedence
        if self.max_rows_per_file is not None:
            # Split based on max_rows_per_file
            if total_rows <= self.max_rows_per_file:
                # Single file is sufficient
                self._write_single_file(
                    path, [table], filename, output_schema, write_kwargs
                )
            else:
                # Need to split into multiple files
                self._split_and_write_table(
                    table,
                    path,
                    filename,
                    output_schema,
                    write_kwargs,
                )
        elif self.min_rows_per_file is not None:
            # Only min_rows_per_file is set
            if total_rows >= self.min_rows_per_file:
                # Single file meets minimum requirement
                self._write_single_file(
                    path, [table], filename, output_schema, write_kwargs
                )
            else:
                # This case should be handled at a higher level by combining blocks
                # For now, write as single file
                self._write_single_file(
                    path, [table], filename, output_schema, write_kwargs
                )

    def _split_and_write_table(
        self,
        table: "pyarrow.Table",
        path: str,
        base_filename: str,
        output_schema: "pyarrow.Schema",
        write_kwargs: Dict[str, Any],
    ) -> None:
        """Split a table into multiple files with specified rows per file."""
        from ray.data.block import BlockAccessor

        total_rows = table.num_rows
        block_accessor = BlockAccessor.for_block(table)

        # Estimate max rows based on target_max_block_size
        def _estimate_max_rows_from_block_size() -> int:
            """Estimate the maximum number of rows based on target_max_block_size."""
            if total_rows == 0:
                return 1

            # Calculate bytes per row
            table_size_bytes = table.nbytes
            if table_size_bytes == 0:
                return 1

            bytes_per_row = table_size_bytes / total_rows
            target_max_block_size = self._data_context.target_max_block_size

            # Estimate max rows that would fit in target_max_block_size
            estimated_max_rows = max(1, int(target_max_block_size / bytes_per_row))
            return estimated_max_rows

        file_idx = 0
        offset = 0
        estimated_max_rows = _estimate_max_rows_from_block_size()
        while offset < total_rows:
            remaining_rows = total_rows - offset

            # Apply the new logic for determining chunk_size to minimize number of files
            if self.max_rows_per_file is None and self.min_rows_per_file is None:
                # If max_size is None, and min_size is None default to max_block_size
                chunk_size = min(estimated_max_rows, remaining_rows)
            elif self.max_rows_per_file is None and self.min_rows_per_file is not None:
                # If max_size is None, and min_size is defined default to max(min_size, remaining_num_rows, max_block_size)
                chunk_size = min(
                    remaining_rows,
                    max(
                        self.min_rows_per_file, min(remaining_rows, estimated_max_rows)
                    ),
                )
            else:
                # If max_rows_per_file is defined, it acts as a hard upper bound
                # Try to maximize chunk size within this constraint to minimize file count
                if self.min_rows_per_file is not None:
                    # Both min and max are defined
                    # Use the larger of min_rows_per_file and estimated_max_rows, but don't exceed max_rows_per_file
                    chunk_size = min(
                        self.max_rows_per_file,
                        max(self.min_rows_per_file, estimated_max_rows),
                        remaining_rows,
                    )
                else:
                    # Only max is defined
                    # Use the smaller of max_rows_per_file and estimated_max_rows to respect the constraint
                    chunk_size = min(
                        self.max_rows_per_file, estimated_max_rows, remaining_rows
                    )

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
                path,
                [chunk_table],
                chunk_filename,
                output_schema,
                write_kwargs,
            )

            offset += chunk_size
            file_idx += 1

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

            # Handle row limits for partitioned data
            self._write_with_row_limits(
                write_path, group_table, filename, output_schema, write_kwargs
            )

    @property
    def min_rows_per_write(self) -> Optional[int]:
        return self.min_rows_per_file
