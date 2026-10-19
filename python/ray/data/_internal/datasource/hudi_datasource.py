import logging
import os
from enum import Enum
from typing import TYPE_CHECKING, Dict, Iterator, List, Optional, Tuple

from ray.data._internal.object_extensions.arrow import raise_on_pickle_object_columns
from ray.data._internal.util import _check_import
from ray.data.block import BlockMetadata
from ray.data.datasource.datasource import Datasource, ReadTask

if TYPE_CHECKING:
    from ray.data.context import DataContext

logger = logging.getLogger(__name__)


class HudiQueryType(Enum):
    SNAPSHOT = "snapshot"
    INCREMENTAL = "incremental"

    @classmethod
    def supported_types(cls) -> List[str]:
        return [e.value for e in cls]


class HudiDatasource(Datasource):
    """Hudi datasource, for reading Apache Hudi table."""

    def __init__(
        self,
        table_uri: str,
        query_type: str,
        filters: Optional[List[Tuple[str, str, str]]] = None,
        hudi_options: Optional[Dict[str, str]] = None,
        storage_options: Optional[Dict[str, str]] = None,
    ):
        _check_import(self, module="hudi", package="hudi")

        self._table_uri = table_uri
        self._query_type = HudiQueryType(query_type.lower())
        self._filters = filters or []
        self._hudi_options = hudi_options or {}
        self._storage_options = storage_options or {}

    def get_read_tasks(
        self,
        parallelism: int,
        per_task_row_limit: Optional[int] = None,
        data_context: Optional["DataContext"] = None,
    ) -> List["ReadTask"]:
        import numpy as np
        import pyarrow
        from hudi import HudiQueryType as HudiRsQueryType, HudiReadOptions

        table_uri = self._table_uri
        storage_options = self._storage_options
        hudi_options = {
            **self._hudi_options,
            # Restrain reading only on parquet files (read optimized mode) for
            # now. This won't affect reading COW.
            "hoodie.read.use.read_optimized.mode": "true",
        }

        def _build_table():
            from hudi import HudiTableBuilder

            return (
                HudiTableBuilder.from_base_uri(table_uri)
                .with_hudi_options(hudi_options)
                .with_storage_options(storage_options)
                .build()
            )

        def _perform_read(
            file_slice_paths: List[Tuple[str, List[str]]],
            read_hudi_options: Dict[str, str],
        ) -> Iterator["pyarrow.Table"]:
            from hudi import HudiReadOptions

            # hudi-rs objects can't be pickled, so rebuild the table here. A
            # reader created from the table carries its data schema and the
            # resolved incremental range, which a standalone
            # HudiFileGroupReader doesn't have.
            read_options = HudiReadOptions(hudi_options=read_hudi_options)
            file_group_reader = _build_table().create_file_group_reader_with_options(
                read_options
            )
            for base_file_path, log_file_paths in file_slice_paths:
                batch = file_group_reader.read_file_slice_from_paths(
                    base_file_path, log_file_paths, read_options
                )
                table = pyarrow.Table.from_batches([batch])
                # Unpickling untrusted data can execute arbitrary code. Reject object
                # columns unless the user has explicitly opted in.
                raise_on_pickle_object_columns(table)
                yield table

        hudi_table = _build_table()

        logger.info("Collecting file slices for Hudi table at: %s", self._table_uri)

        if self._query_type == HudiQueryType.SNAPSHOT:
            read_options = HudiReadOptions(filters=self._filters)
        elif self._query_type == HudiQueryType.INCREMENTAL:
            start_ts = self._hudi_options.get("hoodie.read.file_group.start_timestamp")
            end_ts = self._hudi_options.get("hoodie.read.file_group.end_timestamp")
            read_options = HudiReadOptions().with_query_type(
                HudiRsQueryType.Incremental
            )
            if start_ts is not None:
                read_options = read_options.with_start_timestamp(start_ts)
            if end_ts is not None:
                read_options = read_options.with_end_timestamp(end_ts)
        else:
            raise ValueError(
                f"Unsupported query type: {self._query_type}. Supported types are: {HudiQueryType.supported_types()}."
            )

        file_slices = hudi_table.get_file_slices(read_options)
        file_slices_splits = [
            split for split in np.array_split(file_slices, parallelism) if len(split)
        ]

        logger.info("Creating read tasks for Hudi table at: %s", self._table_uri)

        read_hudi_options = read_options.hudi_options
        schema = hudi_table.get_schema_with_meta_fields()
        read_tasks = []

        for file_slices_split in file_slices_splits:
            file_slice_paths = []
            input_files = []
            size_bytes = 0
            for file_slice in file_slices_split:
                # A file slice in a Hudi table is a logical group of data files
                # within a physical partition. Records stored in a file slice
                # are associated with a commit on the Hudi table's timeline.
                # For more info, see https://hudi.apache.org/docs/file_layouts
                base_file_path = file_slice.base_file_relative_path()
                log_file_paths = file_slice.log_files_relative_paths()
                file_slice_paths.append((base_file_path, log_file_paths))
                # hudi-rs returns an empty base file path for log-only slices.
                for relative_path in [base_file_path, *log_file_paths]:
                    if relative_path:
                        input_files.append(os.path.join(self._table_uri, relative_path))
                size_bytes += file_slice.total_size_bytes()

            if self._query_type == HudiQueryType.SNAPSHOT:
                # hudi-rs 0.5 estimates `num_records` from file size and one
                # sampled parquet footer (0.4 read every footer).
                # `Dataset.count()` trusts `num_rows`, so leave it unset.
                metadata = BlockMetadata(
                    num_rows=None,
                    input_files=input_files,
                    size_bytes=size_bytes,
                    exec_stats=None,
                )
            elif self._query_type == HudiQueryType.INCREMENTAL:
                # need the check due to
                # https://github.com/apache/hudi-rs/issues/401
                metadata = BlockMetadata(
                    num_rows=None,
                    input_files=input_files,
                    size_bytes=None,
                    exec_stats=None,
                )

            read_task = ReadTask(
                read_fn=lambda paths=file_slice_paths: _perform_read(
                    paths, read_hudi_options
                ),
                metadata=metadata,
                schema=schema,
                per_task_row_limit=per_task_row_limit,
            )
            read_tasks.append(read_task)

        return read_tasks

    def estimate_inmemory_data_size(self) -> Optional[int]:
        # TODO(xushiyan) add APIs to provide estimated in-memory size
        return None
