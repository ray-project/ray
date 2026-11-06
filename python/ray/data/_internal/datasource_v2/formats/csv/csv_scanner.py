from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple

import pyarrow as pa
from pyarrow import csv
from pyarrow.fs import FileSystem
from typing_extensions import override

from ray.data._internal.datasource_v2.common.file_scanner import FileScanner
from ray.data._internal.datasource_v2.formats.csv.csv_file_reader import CSVFileReader
from ray.data._internal.datasource_v2.interfaces.synthesized_columns import (
    SynthesizedColumn,
)
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
@dataclass(frozen=True)
class CSVScanner(FileScanner):
    """Configured scanner for streaming CSV reads.

    Extends ``FileScanner`` rather than ``ArrowFileScanner``: the reader parses
    with ``pyarrow.csv`` instead of ``pyarrow.dataset`` and enforces no column,
    filter or limit pushdown, so none of those mixins is promised.
    """

    schema: pa.Schema
    filesystem: Optional[FileSystem] = None
    synthesized_columns: Tuple[SynthesizedColumn, ...] = ()
    read_options: Optional[csv.ReadOptions] = None
    parse_options: Optional[csv.ParseOptions] = None
    arrow_csv_args: Dict[str, Any] = field(default_factory=dict)
    open_stream_args: Dict[str, Any] = field(default_factory=dict)

    @override
    def read_schema(self) -> Optional[pa.Schema]:
        # CSV schemas are inferred from a bounded sample. Files outside that
        # sample can legitimately add columns, as they do on the V1 path, so a
        # sampled schema isn't a complete description of this scanner's output.
        # Returning ``None`` keeps the logical schema dynamic instead of
        # advertising a schema that later blocks can exceed. ``self.schema`` is
        # only a planning hint; the reader takes each file's column names and
        # types from the file's own header block.
        return None

    @override
    def create_reader(self) -> CSVFileReader:
        return CSVFileReader(
            filesystem=self.filesystem,
            partitioning=self.partitioning,
            synthesized_columns=self.synthesized_columns,
            read_options=self.read_options,
            parse_options=self.parse_options,
            arrow_csv_args=dict(self.arrow_csv_args),
            open_stream_args=dict(self.open_stream_args),
        )
