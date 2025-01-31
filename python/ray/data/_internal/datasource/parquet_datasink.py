import logging
import posixpath
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterable, List, Optional

from ray.data._internal.arrow_ops.transform_pyarrow import concat
from ray.data._internal.execution.interfaces import TaskContext
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
                self._write_single_file(tables, filename, output_schema, write_kwargs)
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
        tables: List["pyarrow.Table"],
        filename: str,
        output_schema: "pyarrow.Schema",
        write_kwargs: Dict[str, Any],
    ) -> None:
        import pyarrow.parquet as pq

        write_path = posixpath.join(self.path, filename)
        with self.open_output_stream(write_path) as file:
            with pq.ParquetWriter(file, output_schema, **write_kwargs) as writer:
                for table in tables:
                    table = table.cast(output_schema)
                    writer.write_table(table)

    def _write_partition_files(
        self,
        tables: List["pyarrow.Table"],
        filename: str,
        output_schema: "pyarrow.Schema",
        write_kwargs: Dict[str, Any],
    ) -> None:
        import pyarrow as pa
        import pyarrow.parquet as pq

        table = concat(tables)
        # Create unique combinations of the partition columns
        table_fields = [
            field for field in output_schema if field.name not in self.partition_cols
        ]
        non_partition_cols = [f.name for f in table_fields]
        output_schema = pa.schema(
            [field for field in output_schema if field.name not in self.partition_cols]
        )
        # Group the table by partition keys
        # For each partition key combination fetch list of values
        # for the non partition columns
        # Ex: Here original table contain
        # two columns (a, b). We are paritioning by column a. The schema
        # of `groups` grouped Table is as follows
        # b_list: [[[0,0],[1,1],[2,2]]]
        # a: [[1,2,3]]
        groups = table.group_by(self.partition_cols).aggregate(
            [(col_name, "list") for col_name in non_partition_cols]
        )
        grouped_keys = [groups.column(k) for k in self.partition_cols]

        for i in range(groups.num_rows):
            # See https://github.com/apache/arrow/issues/14882 for recommended approach
            values = [
                groups.column(f"{col.name}_list")[i].values for col in table_fields
            ]
            group_table = pa.Table.from_arrays(values, names=non_partition_cols)
            partition_path = "/".join(
                [
                    f"{col}={values[i]}"
                    for col, values in zip(self.partition_cols, grouped_keys)
                ]
            )
            write_path = posixpath.join(self.path, partition_path)
            self._create_dir(write_path)
            write_path = posixpath.join(write_path, filename)
            with self.open_output_stream(write_path) as file:
                with pq.ParquetWriter(file, output_schema, **write_kwargs) as writer:
                    writer.write_table(group_table)

    @property
    def min_rows_per_write(self) -> Optional[int]:
        return self.min_rows_per_file
