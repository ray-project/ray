"""Concrete ``DataSourceV2`` for binary files, behind ``read_binary_files``."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Literal, Optional, Union

import pyarrow as pa
from typing_extensions import override

from ray.data._internal.datasource_v2.common.file_stream_datasource_v2 import (
    FileStreamDatasourceV2,
)
from ray.data._internal.datasource_v2.formats.binary.binary_reader import (
    BINARY_SCHEMA,
)
from ray.data._internal.datasource_v2.formats.binary.binary_scanner import (
    BinaryScanner,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data.datasource.file_based_datasource import FileShuffleConfig
from ray.data.datasource.partitioning import Partitioning
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from pyarrow.fs import FileSystem


@DeveloperAPI
class BinaryDatasourceV2(FileStreamDatasourceV2):
    """V2 datasource that lists files and reads each whole into a ``bytes`` row.

    The schema is fixed: ``bytes``, the partition columns found in the sampled
    paths, and ``path`` for ``include_paths``.
    """

    def __init__(
        self,
        paths: List[str],
        *,
        filesystem: Optional["FileSystem"] = None,
        open_stream_args: Optional[Dict[str, Any]] = None,
        partitioning: Optional[Partitioning] = None,
        file_extensions: Optional[List[str]] = None,
        ignore_missing_paths: bool = False,
        include_paths: bool = False,
        shuffle: Optional[Union[Literal["files"], FileShuffleConfig]] = None,
    ):
        super().__init__(
            paths,
            name="BinaryV2",
            filesystem=filesystem,
            open_stream_args=open_stream_args,
            partitioning=partitioning,
            file_extensions=file_extensions,
            ignore_missing_paths=ignore_missing_paths,
            include_paths=include_paths,
            shuffle=shuffle,
        )

    @override
    def _infer_file_schema(self, sample: Optional[FileManifest]) -> pa.Schema:
        return BINARY_SCHEMA

    @override
    def create_scanner(
        self,
        schema: pa.Schema,
        filesystem: Optional["FileSystem"] = None,
        **options: Any,
    ) -> BinaryScanner:
        return BinaryScanner(**self._scanner_kwargs(schema, filesystem, options))
