"""Concrete ``DataSourceV2`` for plain-text files, behind ``read_text``."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, List, Literal, Optional, Union

import pyarrow as pa
from typing_extensions import override

from ray.data._internal.datasource_v2.common.file_stream_datasource_v2 import (
    FileStreamDatasourceV2,
)
from ray.data._internal.datasource_v2.formats.text.text_reader import (
    TEXT_COLUMN_NAME,
)
from ray.data._internal.datasource_v2.formats.text.text_scanner import TextScanner
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data.datasource.file_based_datasource import FileShuffleConfig
from ray.data.datasource.partitioning import Partitioning
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from pyarrow.fs import FileSystem

TEXT_SCHEMA = pa.schema([pa.field(TEXT_COLUMN_NAME, pa.string())])


@DeveloperAPI
class TextDatasourceV2(FileStreamDatasourceV2):
    """V2 datasource that lists text files and reads them with ``TextScanner``.

    Each file is one read unit, read whole. The schema is fixed: ``text``, the
    partition columns found in the sampled paths, and ``path`` for ``include_paths``.
    """

    def __init__(
        self,
        paths: List[str],
        *,
        encoding: str = "utf-8",
        drop_empty_lines: bool = True,
        filesystem: Optional["FileSystem"] = None,
        partitioning: Optional[Partitioning] = None,
        file_extensions: Optional[List[str]] = None,
        ignore_missing_paths: bool = False,
        include_paths: bool = False,
        shuffle: Optional[Union[Literal["files"], FileShuffleConfig]] = None,
    ):
        super().__init__(
            paths,
            name="TextV2",
            filesystem=filesystem,
            partitioning=partitioning,
            file_extensions=file_extensions,
            ignore_missing_paths=ignore_missing_paths,
            include_paths=include_paths,
            shuffle=shuffle,
        )
        self._encoding = encoding
        self._drop_empty_lines = drop_empty_lines

    @override
    def _infer_file_schema(self, sample: Optional[FileManifest]) -> pa.Schema:
        return TEXT_SCHEMA

    @override
    def create_scanner(
        self,
        schema: pa.Schema,
        filesystem: Optional["FileSystem"] = None,
        **options: Any,
    ) -> TextScanner:
        return TextScanner(
            encoding=self._encoding,
            drop_empty_lines=self._drop_empty_lines,
            **self._scanner_kwargs(schema, filesystem, options),
        )
