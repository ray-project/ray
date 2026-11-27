"""The text ``Scanner``: the read's options plus the optimizer's pushdowns."""

from dataclasses import dataclass

from typing_extensions import override

from ray.data._internal.datasource_v2.common.file_stream_scanner import (
    FileStreamScanner,
)
from ray.data._internal.datasource_v2.formats.text.text_reader import TextReader
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
@dataclass(frozen=True)
class TextScanner(FileStreamScanner):
    """Scanner for plain-text files on DataSource V2.

    Supports column pruning, a per-task row limit and partition pruning.
    Filter pushdown is not claimed.
    """

    encoding: str = "utf-8"
    drop_empty_lines: bool = True

    @override
    def create_reader(self) -> TextReader:
        """Create a reader with this scanner's projection, limit and partition filter."""
        return TextReader(
            encoding=self.encoding,
            drop_empty_lines=self.drop_empty_lines,
            **self._reader_kwargs(),
        )
