import logging
import os
from enum import Enum
from typing import Dict, Iterator, List, Optional, Tuple

from ray.data._internal.util import _check_import
from ray.data.block import BlockMetadata
from ray.data.datasource.datasource import Datasource, ReadTask

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
        _check_import(self, module="hudi", package="hudi-python")

        self._table_uri = table_uri
        self._query_type = HudiQueryType(query_type.lower())
        self._filters = filters or []
        self._hudi_options = hudi_options or {}
        self._storage_options = storage_options or {}

    def get_read_tasks(
        self, parallelism: int, per_task_row_limit: Optional[int] = None
    ) -> List["ReadTask"]:
        import numpy as np
        import pyarrow
        from hudi import HudiTableBuilder

        def _perform_read(
            table_uri: str,
            base_file_paths: List[str],
            options: Dict[str, str],
        ) -> Iterator["pyarrow.Table"]:
            from hudi import HudiFileGroupReader

            for p in base_file_paths:
                file_group_reader = HudiFileGroupReader(table_uri, options)
                batch = file_group_reader.read_file_slice_by_base_file_path(p)
                yield pyarrow.Table.from_batches([batch])

        hudi_table = (
            HudiTableBuilder.from_base_uri(self._table_uri)
            .with_hudi_options(self._hudi_options)
            .with_storage_options(self._storage_options)
            # Although hudi-rs supports MOR snapshot, we need to add an API in
            # the next release to allow file group reader to take in a list of
            # files. Hence, setting this config for now to restrain reading
            # only on parquet files (read optimized mode).
            # This won't affect reading COW.
            .with_hudi_option("hoodie.read.use.read_optimized.mode", "true")
            .build()
        )

        logger.info("Collecting file slices for Hudi table at: %s", self._table_uri)

        if self._query_type == HudiQueryType.SNAPSHOT:
            file_slices_splits = hudi_table.get_file_slices_splits(
                parallelism, self._filters
            )
        elif self._query_type == HudiQueryType.INCREMENTAL:
            start_ts = self._hudi_options.get("hoodie.read.file_group.start_timestamp")
            end_ts = self._hudi_options.get("hoodie.read.file_group.end_timestamp")
            # TODO(xushiyan): add table API to return splits of file slices
            file_slices = hudi_table.get_file_slices_between(start_ts, end_ts)
            file_slices_splits = np.array_split(file_slices, parallelism)
        else:
            raise ValueError(
                f"Unsupported query type: {self._query_type}. Supported types are: {HudiQueryType.supported_types()}."
            )

        logger.info("Creating read tasks for Hudi table at: %s", self._table_uri)

        reader_options = {
            **hudi_table.storage_options(),
            **hudi_table.hudi_options(),
        }

        schema = hudi_table.get_schema()
        read_tasks = []

        for file_slices_split in file_slices_splits:
            num_rows = 0
            relative_paths = []
            input_files = []
            size_bytes = 0
            for file_slice in file_slices_split:
                # A file slice in a Hudi table is a logical group of data files
                # within a physical partition. Records stored in a file slice
                # are associated with a commit on the Hudi table's timeline.
                # For more info, see https://hudi.apache.org/docs/file_layouts
                num_rows += file_slice.num_records
                relative_path = file_slice.base_file_relative_path()
                relative_paths.append(relative_path)
                full_path = os.path.join(self._table_uri, relative_path)
                input_files.append(full_path)
                size_bytes += file_slice.base_file_size

            if self._query_type == HudiQueryType.SNAPSHOT:
                metadata = BlockMetadata(
                    num_rows=num_rows,
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
                read_fn=lambda paths=relative_paths: _perform_read(
                    self._table_uri, paths, reader_options
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
