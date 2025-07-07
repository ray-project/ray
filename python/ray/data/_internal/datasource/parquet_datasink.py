import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterable, List, Optional

from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.planner.plan_write_op import WRITE_UUID_KWARG_NAME
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

# Map Ray Data's SaveMode to pyarrow's existing_data_behavior property which is exposed via the
# `pyarrow.dataset.write_dataset` function.
# Docs: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.write_dataset.html
EXISTING_DATA_BEHAVIOR_MAP = {
    SaveMode.APPEND: "overwrite_or_ignore",
    SaveMode.OVERWRITE: "delete_matching",
    SaveMode.IGNORE: "overwrite_or_ignore",
    SaveMode.ERROR: "error",
}

FILE_FORMAT = "parquet"

# These args are part of https://arrow.apache.org/docs/python/generated/pyarrow.fs.FileSystem.html#pyarrow.fs.FileSystem.open_output_stream
# and are not supported by ParquetDatasink.
UNSUPPORTED_OPEN_STREAM_ARGS = {"path", "buffer", "metadata"}

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

        if open_stream_args is not None:
            intersecting_keys = UNSUPPORTED_OPEN_STREAM_ARGS.intersection(
                set(open_stream_args.keys())
            )
            if intersecting_keys:
                logger.warning(
                    "open_stream_args contains unsupported arguments: %s. These arguments "
                    "are not supported by ParquetDatasink. They will be ignored.",
                    intersecting_keys,
                )

            if "compression" in open_stream_args:
                self.arrow_parquet_args["compression"] = open_stream_args["compression"]

        super().__init__(
            path,
            filesystem=filesystem,
            try_create_dir=try_create_dir,
            open_stream_args=open_stream_args,
            filename_provider=filename_provider,
            dataset_uuid=dataset_uuid,
            file_format=FILE_FORMAT,
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
            blocks[0], ctx.kwargs[WRITE_UUID_KWARG_NAME], ctx.task_idx, 0
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

            self._write_parquet_files(
                tables,
                filename,
                output_schema,
                ctx.kwargs[WRITE_UUID_KWARG_NAME],
                write_kwargs,
            )

        logger.debug(f"Writing {filename} file to {self.path}.")

        call_with_retry(
            write_blocks_to_path,
            description=f"write '{filename}' to '{self.path}'",
            match=self._data_context.retried_io_errors,
            max_attempts=WRITE_FILE_MAX_ATTEMPTS,
            max_backoff_s=WRITE_FILE_RETRY_MAX_BACKOFF_SECONDS,
        )

    def _get_basename_template(self, filename: str, write_uuid: str) -> str:
        # Check if write_uuid is present in filename, add if missing
        if write_uuid not in filename and self.mode == SaveMode.APPEND:
            raise ValueError(
                f"Write UUID '{write_uuid}' missing from filename template '{filename}'. This could result in files being overwritten."
                f"Modify your FileNameProvider implementation to include the `write_uuid` into the filename template or change your write mode to SaveMode.OVERWRITE. "
            )
        # Check if filename is already templatized
        if "{i}" in filename:
            # Filename is already templatized, but may need file extension
            if FILE_FORMAT not in filename:
                # Add file extension to templatized filename
                basename_template = f"{filename}.{FILE_FORMAT}"
            else:
                # Already has extension, use as-is
                basename_template = filename
        elif FILE_FORMAT not in filename:
            # No extension and not templatized, add extension and template
            basename_template = f"{filename}-{{i}}.{FILE_FORMAT}"
        else:
            # Has extension but not templatized, add template while preserving extension
            logger.warning(
                "FilenameProvider have to provide proper filename template including '{{i}}' "
                "macro to ensure unique filenames when writing multiple files. Appending '{{i}}' "
                "macro to the end of the file. For more details on the expected filename template checkout "
                "PyArrow's `write_to_dataset` API"
            )
            # Use pathlib.Path to properly handle filenames with dots
            filename_path = Path(filename)
            stem = filename_path.stem  # filename without extension
            assert "." not in stem, "Filename should not contain a dot"
            suffix = filename_path.suffix  # extension including the dot
            basename_template = f"{stem}-{{i}}{suffix}"
        return basename_template

    def _write_parquet_files(
        self,
        tables: List["pyarrow.Table"],
        filename: str,
        output_schema: "pyarrow.Schema",
        write_uuid: str,
        write_kwargs: Dict[str, Any],
    ) -> None:
        import pyarrow.dataset as ds

        # Make every incoming batch conform to the final schema *before* writing
        for idx, table in enumerate(tables):
            if output_schema and not table.schema.equals(output_schema):
                table = table.cast(output_schema)
            tables[idx] = table

        row_group_size = write_kwargs.pop("row_group_size", None)

        existing_data_behavior = EXISTING_DATA_BEHAVIOR_MAP.get(
            self.mode, "overwrite_or_ignore"
        )

        # Set default row group size if not provided. Defaults are set by pyarrow.
        min_rows_per_group = row_group_size if row_group_size else 0
        max_rows_per_group = row_group_size if row_group_size else 1024 * 1024

        basename_template = self._get_basename_template(filename, write_uuid)

        ds.write_dataset(
            data=tables,
            base_dir=self.path,
            schema=output_schema,
            basename_template=basename_template,
            filesystem=self.filesystem,
            partitioning=self.partition_cols,
            format=FILE_FORMAT,
            existing_data_behavior=existing_data_behavior,
            partitioning_flavor="hive",
            use_threads=True,
            min_rows_per_group=min_rows_per_group,
            max_rows_per_group=max_rows_per_group,
            file_options=ds.ParquetFileFormat().make_write_options(**write_kwargs),
        )

    @property
    def min_rows_per_write(self) -> Optional[int]:
        return self.min_rows_per_file
