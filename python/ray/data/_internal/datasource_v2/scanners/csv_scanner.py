from dataclasses import dataclass, field
from typing import Any, Dict, Optional

import pyarrow as pa
from pyarrow import csv
from pyarrow.fs import FileSystem

from ray.data._internal.datasource_v2.readers.csv_file_reader import CSVFileReader
from ray.data._internal.datasource_v2.scanners.file_scanner import FileScanner
from ray.data.datasource.partitioning import Partitioning
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
@dataclass(frozen=True)
class CSVScanner(FileScanner):
    """Configured scanner for streaming CSV reads."""

    schema: pa.Schema
    physical_schema: pa.Schema
    filesystem: Optional[FileSystem] = None
    partitioning: Optional[Partitioning] = None
    include_paths: bool = False
    read_options: Optional[csv.ReadOptions] = None
    parse_options: Optional[csv.ParseOptions] = None
    arrow_csv_args: Dict[str, Any] = field(default_factory=dict)
    open_stream_args: Dict[str, Any] = field(default_factory=dict)

    def read_schema(self) -> pa.Schema:
        return self.schema

    def create_reader(self) -> CSVFileReader:
        return CSVFileReader(
            schema=self.schema,
            physical_schema=self.physical_schema,
            filesystem=self.filesystem,
            partitioning=self.partitioning,
            include_paths=self.include_paths,
            read_options=self.read_options,
            parse_options=self.parse_options,
            arrow_csv_args=dict(self.arrow_csv_args),
            open_stream_args=dict(self.open_stream_args),
        )
