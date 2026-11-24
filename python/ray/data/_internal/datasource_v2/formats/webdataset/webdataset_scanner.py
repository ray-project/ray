"""The WebDataset ``Scanner``: the read's options, sent to every read task.

``WebDatasetScanner`` is a frozen dataclass that creates one
``WebDatasetReader`` per read task. It takes no filter, projection or limit
pushdown.
"""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Tuple, Union

import pyarrow as pa

from ray.data._internal.datasource_v2.common.file_scanner import FileScanner
from ray.data._internal.datasource_v2.formats.webdataset.webdataset_reader import (
    WebDatasetReader,
)
from ray.data._internal.datasource_v2.interfaces.synthesized_columns import (
    SynthesizedColumn,
)
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from pyarrow.fs import FileSystem


@DeveloperAPI
@dataclass(frozen=True)
class WebDatasetScanner(FileScanner):
    """Scanner for WebDataset tar shards.

    Supports no filter, projection or limit pushdown.
    """

    schema: Optional[pa.Schema]
    filesystem: "FileSystem"
    decoder: Optional[Union[bool, str, Callable, list]] = True
    fileselect: Optional[Union[list, Callable]] = None
    filerename: Optional[Union[list, Callable]] = None
    suffixes: Optional[Union[list, Callable]] = None
    verbose_open: bool = False
    expand_json: bool = False
    allow_unsafe_deserialization: bool = False
    open_stream_args: Dict[str, Any] = field(default_factory=dict)
    synthesized_columns: Tuple[SynthesizedColumn, ...] = ()
    target_block_size: Optional[int] = None

    def read_schema(self) -> Optional[pa.Schema]:
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
            target_block_size=self.target_block_size,
        )
