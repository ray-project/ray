"""``TextReader``: reads whole text files into one ``text`` column of lines.

``pyarrow.dataset`` has no plain-text format, so this builds on
``FileStreamReader``. Line semantics match V1 ``TextDatasource``:
``str.splitlines()`` splits, and ``drop_empty_lines`` drops whitespace-only lines.
"""

from typing import Iterator

import pyarrow as pa
from typing_extensions import override

from ray.data._internal.arrow_block import _BATCH_SIZE_PRESERVING_STUB_COL_NAME
from ray.data._internal.datasource.text_datasource import TEXT_COLUMN_NAME
from ray.data._internal.datasource_v2.common.file_stream_reader import (
    FileStreamReader,
)
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class TextReader(FileStreamReader):
    """Reads the text files a manifest assigns to one task, one table per file."""

    def __init__(
        self,
        *,
        encoding: str = "utf-8",
        drop_empty_lines: bool = True,
        **kwargs,
    ):
        """Initialize the reader.

        Args:
            encoding: Codec the file bytes are decoded with.
            drop_empty_lines: Whether to drop lines that are empty or
                whitespace only.
            **kwargs: See ``FileStreamReader``.
        """
        super().__init__(**kwargs)
        self._encoding = encoding
        self._drop_empty_lines = drop_empty_lines

    @override
    def _read_stream(self, stream: pa.NativeFile, path: str) -> Iterator[pa.Table]:
        lines = stream.readall().decode(self._encoding).splitlines()
        if self._drop_empty_lines:
            lines = [line for line in lines if line.strip() != ""]
        if not lines:
            return
        if not self._wants(TEXT_COLUMN_NAME):
            # ``text`` is projected away (``count()``): keep only the row count.
            yield pa.table({_BATCH_SIZE_PRESERVING_STUB_COL_NAME: pa.nulls(len(lines))})
            return
        yield pa.table({TEXT_COLUMN_NAME: pa.array(lines, type=pa.string())})
