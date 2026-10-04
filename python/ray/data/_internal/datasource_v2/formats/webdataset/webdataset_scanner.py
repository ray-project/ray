from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple

import pyarrow as pa

from ray.data._internal.datasource_v2.common.file_scanner import FileScanner
from ray.data._internal.datasource_v2.formats.webdataset.webdataset_reader import (
    WebDatasetReader,
)
from ray.data._internal.datasource_v2.interfaces.synthesized_columns import SynthesizedColumn
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from pyarrow.fs import FileSystem


@DeveloperAPI
@dataclass(frozen=True)
class WebDatasetScanner(FileScanner):
    """Scanner for WebDataset tar shards.

    Tar archives are not an Arrow dataset format, so this extends
    ``FileScanner`` rather than ``ArrowFileScanner`` and supports no filter,
    projection or limit pushdown, matching the legacy reader. File shuffle and
    partition pruning come from ``FileScanner``.
    """

    schema: pa.Schema
    filesystem: "FileSystem"
    decoder: Optional[Any] = True
    fileselect: Optional[Any] = None
    filerename: Optional[Any] = None
    suffixes: Optional[Any] = None
    verbose_open: bool = False
    expand_json: bool = False
    allow_unsafe_deserialization: bool = False
    open_stream_args: Dict[str, Any] = field(default_factory=dict)
    synthesized_columns: Tuple[SynthesizedColumn, ...] = ()

    def read_schema(self) -> pa.Schema:
        return self.schema

    def create_reader(self) -> WebDatasetReader:
        return WebDatasetReader(
            filesystem=self.filesystem,
            decoder=self.decoder,
            fileselect=self.fileselect,
            filerename=self.filerename,
            suffixes=self.suffixes,
            verbose_open=self.verbose_open,
            expand_json=self.expand_json,
            allow_unsafe_deserialization=self.allow_unsafe_deserialization,
            open_stream_args=dict(self.open_stream_args),
            synthesized_columns=self.synthesized_columns,
        )
