"""Concrete ``DataSourceV2`` for WebDataset tar shards.

A tar is read sequentially and cannot be split,
so each tar shard is read by one read task.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Dict, List, Literal, Optional, Union

import numpy as np
import pyarrow as pa

from ray._common.utils import env_bool
from ray.data._internal.datasource_v2.common.non_sampling_file_indexer import (
    NonSamplingFileIndexer,
)
from ray.data._internal.datasource_v2.common.round_robin_partitioner import (
    RoundRobinPartitioner,
)
from ray.data._internal.datasource_v2.common.synthesized_columns import PathColumn
from ray.data._internal.datasource_v2.formats.webdataset.webdataset_reader import (
    WebDatasetReader,
)
from ray.data._internal.datasource_v2.formats.webdataset.webdataset_scanner import (
    WebDatasetScanner,
)
from ray.data._internal.datasource_v2.formats.webdataset.webdataset_utils import (
    ALLOW_UNSAFE_DESERIALIZATION_ENV_VAR,
)
from ray.data._internal.datasource_v2.interfaces.datasource_v2 import (
    DatasourceCategory,
    FileDataSourceV2,
)
from ray.data._internal.datasource_v2.interfaces.file_indexer import FileIndexer
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.datasource_v2.interfaces.in_memory_size_estimator import (
    InMemorySizeEstimator,
)
from ray.data._internal.datasource_v2.interfaces.synthesized_columns import (
    SynthesizedColumn,
)
from ray.data._internal.util import RetryingPyFileSystem, _is_local_scheme
from ray.data.context import DataContext
from ray.data.datasource.path_util import _resolve_paths_and_filesystem
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from pyarrow.fs import FileSystem

    from ray.data._internal.datasource_v2.interfaces.file_partitioner import (
        PartitionHints,
    )
    from ray.data.datasource.file_based_datasource import FileShuffleConfig

# In-memory size / tar file size, measured on Imagenette shards.
WEBDATASET_ENCODING_RATIO_RAW = 0.7
WEBDATASET_ENCODING_RATIO_DECODED = 8.9  # Measured with default decoder.


@DeveloperAPI
class WebDatasetInMemorySizeEstimator(InMemorySizeEstimator):
    """Estimates in-memory sizes of tar shards with a fixed encoding ratio."""

    def __init__(self, encoding_ratio: float):
        self._encoding_ratio = encoding_ratio

    def estimate_in_memory_sizes(self, manifest: FileManifest) -> np.ndarray:
        return self._encoding_ratio * manifest.file_sizes


@DeveloperAPI
class WebDatasetDatasourceV2(FileDataSourceV2):
    """V2 datasource that lists tar shards and reads them with WebDatasetScanner.

    RoundRobinPartitioner groups shards into read tasks by estimated
    in-memory size, and the schema comes from decoding the first chunk of
    the first shard.
    """

    def __init__(
        self,
        paths: List[str],
        *,
        filesystem: Optional["FileSystem"] = None,
        decoder: Optional[Union[bool, str, Callable, list]] = True,
        fileselect: Optional[Union[list, Callable]] = None,
        filerename: Optional[Union[list, Callable]] = None,
        suffixes: Optional[Union[list, Callable]] = None,
        verbose_open: bool = False,
        expand_json: bool = False,
        include_paths: bool = False,
        file_extensions: Optional[List[str]] = None,
        shuffle: Optional[Union[Literal["files"], "FileShuffleConfig"]] = None,
        open_stream_args: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(name="WebDatasetV2", category=DatasourceCategory.FILE_BASED)
        # Check ``local://`` against the original paths.
        self._supports_distributed_reads = not _is_local_scheme(paths)
        resolved_paths, resolved_filesystem = _resolve_paths_and_filesystem(
            paths, filesystem
        )
        self._paths: List[str] = resolved_paths
        # Retry ``DataContext.retried_io_errors`` on every filesystem call.
        self._filesystem = RetryingPyFileSystem.wrap(
            resolved_filesystem,
            retryable_errors=DataContext.get_current().retried_io_errors,
        )
        self._decoder = decoder
        self._fileselect = fileselect
        self._filerename = filerename
        self._suffixes = suffixes
        self._verbose_open = verbose_open
        self._expand_json = expand_json
        self._file_extensions = file_extensions
        self._shuffle = shuffle
        self._open_stream_args = open_stream_args or {}
        self._allow_unsafe_deserialization = env_bool(
            ALLOW_UNSAFE_DESERIALIZATION_ENV_VAR, False
        )
        synthesized_columns: List[SynthesizedColumn] = []
        if include_paths:
            synthesized_columns.append(PathColumn())
        self._synthesized_columns = tuple(synthesized_columns)

    @property
    def paths(self) -> List[str]:
        return self._paths

    @property
    def filesystem(self) -> "FileSystem":
        return self._filesystem

    @property
    def file_extensions(self) -> Optional[List[str]]:
        return self._file_extensions

    @property
    def shuffle(self) -> Optional[Union[Literal["files"], "FileShuffleConfig"]]:
        return self._shuffle

    def _get_file_indexer(self) -> FileIndexer:
        # One listing row per tar file, or shard.
        return NonSamplingFileIndexer(ignore_missing_paths=False)

    def get_file_partitioner(
        self, *, hints: Optional["PartitionHints"] = None
    ) -> Optional[RoundRobinPartitioner]:
        # Group shards into read tasks by estimated in-memory size.
        if hints is None:
            return None
        encoding_ratio = (
            WEBDATASET_ENCODING_RATIO_RAW
            if self._decoder is None
            else WEBDATASET_ENCODING_RATIO_DECODED
        )
        return RoundRobinPartitioner(
            WebDatasetInMemorySizeEstimator(encoding_ratio), hints=hints
        )

    @property
    def schema_needs_file_sample(self) -> bool:
        return True

    def infer_schema(self, sample: Optional[FileManifest]) -> pa.Schema:
        """Read the first chunk of samples from the first shard and use the
        schema of the resulting table.
        """
        assert sample is not None, "WebDataset always receives a sample"

        schema = pa.schema([])
        if len(sample) > 0:
            reader = self._create_reader(synthesized_columns=())
            first_shard = FileManifest.construct_manifest(
                paths=sample.paths.tolist()[:1],
                sizes=sample.file_sizes.tolist()[:1],
                chunk_metadatas=[None],
            )
            first_table = next(iter(reader.read(first_shard)), None)
            if first_table is not None:
                schema = first_table.schema

        for column in self._synthesized_columns:
            idx = schema.get_field_index(column.name)
            if idx == -1:
                schema = schema.append(pa.field(column.name, column.type))
            elif schema.field(idx).type != column.type:
                schema = schema.set(idx, pa.field(column.name, column.type))
        return schema

    def create_scanner(
        self,
        schema: pa.Schema,
        filesystem: Optional["FileSystem"] = None,
        **options: Any,
    ) -> WebDatasetScanner:
        return WebDatasetScanner(
            schema=schema,
            filesystem=filesystem or self._filesystem,
            decoder=self._decoder,
            fileselect=self._fileselect,
            filerename=self._filerename,
            suffixes=self._suffixes,
            verbose_open=self._verbose_open,
            expand_json=self._expand_json,
            allow_unsafe_deserialization=self._allow_unsafe_deserialization,
            open_stream_args=dict(self._open_stream_args),
            synthesized_columns=self._synthesized_columns,
            partitioning=options.get("partitioning"),
            shuffle=self._shuffle,
        )

    def _create_reader(self, synthesized_columns) -> WebDatasetReader:
        return WebDatasetReader(
            filesystem=self._filesystem,
            decoder=self._decoder,
            fileselect=self._fileselect,
            filerename=self._filerename,
            suffixes=self._suffixes,
            verbose_open=self._verbose_open,
            expand_json=self._expand_json,
            allow_unsafe_deserialization=self._allow_unsafe_deserialization,
            open_stream_args=dict(self._open_stream_args),
            synthesized_columns=synthesized_columns,
        )
