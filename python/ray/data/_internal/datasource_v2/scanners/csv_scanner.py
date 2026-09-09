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
    filesystem: Optional[FileSystem] = None
    partitioning: Optional[Partitioning] = None
    include_paths: bool = False
    read_options: Optional[csv.ReadOptions] = None
    parse_options: Optional[csv.ParseOptions] = None
    arrow_csv_args: Dict[str, Any] = field(default_factory=dict)
    open_stream_args: Dict[str, Any] = field(default_factory=dict)

    def read_schema(self) -> Optional[pa.Schema]:
        # CSV schemas are inferred from a bounded sample. Files outside that
        # sample can legitimately add columns, as they do on the V1 path, so a
        # sampled schema isn't a complete description of this scanner's output.
        # Returning ``None`` keeps the logical schema dynamic instead of
        # advertising a schema that later blocks can exceed. ``self.schema`` is
        # only a planning hint; the reader takes each file's column names and
        # types from the file's own header block.
        return None

    def create_reader(self) -> CSVFileReader:
        return CSVFileReader(
            filesystem=self.filesystem,
            partitioning=self.partitioning,
            include_paths=self.include_paths,
            read_options=self.read_options,
            parse_options=self.parse_options,
            arrow_csv_args=dict(self.arrow_csv_args),
            open_stream_args=dict(self.open_stream_args),
        )
