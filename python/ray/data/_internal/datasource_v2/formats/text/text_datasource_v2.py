"""Concrete ``DataSourceV2`` for plain-text files, behind ``read_text``."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Iterable, List, Literal, Optional, Union

import numpy as np
import pyarrow as pa
from typing_extensions import override

from ray.data._internal.datasource_v2.common.non_sampling_file_indexer import (
    NonSamplingFileIndexer,
)
from ray.data._internal.datasource_v2.common.partitioning_utils import (
    resolve_partitioning,
)
from ray.data._internal.datasource_v2.common.round_robin_partitioner import (
    RoundRobinPartitioner,
)
from ray.data._internal.datasource_v2.common.synthesized_columns import PathColumn
from ray.data._internal.datasource_v2.formats.text.text_reader import TEXT_COLUMN_NAME
from ray.data._internal.datasource_v2.formats.text.text_scanner import TextScanner
from ray.data._internal.datasource_v2.interfaces.datasource_v2 import (
    DatasourceCategory,
    FileDataSourceV2,
)
from ray.data._internal.datasource_v2.interfaces.file_indexer import (
    FileIndexer,
    FileInfo,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.datasource_v2.interfaces.file_partitioner import (
    FilePartitioner,
    PartitionHints,
)
from ray.data._internal.datasource_v2.interfaces.file_pruner import FilePruner
from ray.data._internal.datasource_v2.interfaces.in_memory_size_estimator import (
    InMemorySizeEstimator,
)
from ray.data._internal.util import _is_local_scheme, _normalize_paths_to_strings
from ray.data.datasource.file_based_datasource import FileShuffleConfig
from ray.data.datasource.partitioning import (
    Partitioning,
    PathPartitionParser,
    _partition_field_types_to_pa_schema,
)
from ray.data.datasource.path_util import _resolve_paths_and_filesystem
from ray.util.annotations import DeveloperAPI

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from pyarrow.fs import FileSystem

    from ray.data.block import BlockColumn


class _TextFileIndexer(NonSamplingFileIndexer):
    """``NonSamplingFileIndexer`` that keeps empty files.

    The base indexer skips zero-size files because an empty Parquet or ORC
    file is corrupt. An empty text file is a valid file with no lines, and
    V1 reads it as such; skipping it would turn a directory of empty logs
    into a "no files found" error. Files whose size the filesystem does not
    report are still skipped: task sizing needs a size.
    """

    @override
    def as_whole_file_indexer(self) -> "_TextFileIndexer":
        """Keep the empty-file policy for metadata-only listings too."""
        return _TextFileIndexer(
            ignore_missing_paths=self._ignore_missing_paths,
            skip_paths=self._skip_paths,
            num_workers=self._num_workers,
            max_paths_per_output=self._max_paths_per_output,
        )

    @override
    def list_file_infos(
        self,
        paths: "BlockColumn",
        *,
        filesystem: Optional["FileSystem"],
        pruners: Optional[List[FilePruner]] = None,
        preserve_order: bool = False,
    ) -> Iterable[FileInfo]:
        if filesystem is None:
            raise ValueError(
                f"{type(self).__name__} lists files through a PyArrow filesystem, "
                "but the datasource returned `filesystem=None`."
            )
        pruners = pruners or []
        for file_info in self._get_file_info_iterator(
            paths, filesystem, preserve_order
        ):
            if file_info.size is None:
                logger.warning(f"Skipping file of unknown size: {file_info.path!r}")
                continue
            if not all(pruner.should_include(file_info.path) for pruner in pruners):
                continue
            yield file_info


class _OnDiskSizeEstimator(InMemorySizeEstimator):
    """Size text files by their bytes on disk, without opening any.

    UTF-8 text decodes to an Arrow string column of about the same size, so
    the on-disk size is the estimate, as V1 sizes text reads. Sampling a
    file instead would read one whole text file per listing task, and text
    files are routinely gigabytes. A compressed file is under-estimated by
    its compression ratio; that only makes its read task larger, as in V1.
    """

    def estimate_in_memory_sizes(self, manifest: FileManifest) -> np.ndarray:
        return manifest.file_sizes


@DeveloperAPI
class TextDatasourceV2(FileDataSourceV2):
    """V2 datasource that lists text files and reads them with ``TextScanner``.

    Every file is one read unit, read whole, as in V1, and empty files are
    listed rather than skipped. The schema is fixed:
    one ``text`` string column, plus the partition columns found in the
    sampled paths and the ``path`` column when ``include_paths`` is set. No
    file is opened while planning.
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
        super().__init__(name="TextV2", category=DatasourceCategory.FILE_BASED)
        self._supports_distributed_reads = not _is_local_scheme(paths)
        resolved_paths, resolved_filesystem = _resolve_paths_and_filesystem(
            paths, filesystem
        )
        self._paths = resolved_paths
        # The resolved paths above lose their URI scheme; lineage tracking
        # reads the caller's paths through ``source_paths``.
        self._source_paths = _normalize_paths_to_strings(paths)
        self._filesystem = resolved_filesystem
        self._encoding = encoding
        self._drop_empty_lines = drop_empty_lines
        self._partitioning = partitioning
        self._file_extensions = file_extensions
        self._ignore_missing_paths = ignore_missing_paths
        self._shuffle = shuffle
        self._synthesized_columns = (PathColumn(),) if include_paths else ()

    @property
    @override
    def paths(self) -> List[str]:
        return self._paths

    @property
    @override
    def source_paths(self) -> List[str]:
        """Override when :attr:`paths` is resolved and loses URI schemes."""
        return self._source_paths

    @property
    @override
    def filesystem(self) -> "FileSystem":
        return self._filesystem

    @property
    @override
    def file_extensions(self) -> Optional[List[str]]:
        return self._file_extensions

    @property
    @override
    def shuffle(self) -> Optional[Union[Literal["files"], FileShuffleConfig]]:
        return self._shuffle

    @override
    def _get_file_indexer(self) -> FileIndexer:
        return _TextFileIndexer(ignore_missing_paths=self._ignore_missing_paths)

    @override
    def get_file_partitioner(
        self, *, hints: Optional[PartitionHints] = None
    ) -> Optional[FilePartitioner]:
        if hints is None:
            return None
        return RoundRobinPartitioner(_OnDiskSizeEstimator(), hints=hints)

    @property
    @override
    def schema_needs_file_sample(self) -> bool:
        # The schema is fixed, but the sampled paths decide the partition
        # columns, and sampling is what reports "no files found".
        return True

    @override
    def infer_schema(self, sample: Optional[FileManifest]) -> pa.Schema:
        schema = pa.schema([pa.field(TEXT_COLUMN_NAME, pa.string())])
        sample_paths: List[str] = sample.paths.tolist() if sample is not None else []

        resolved_partitioning = self.resolve_partitioning(sample)
        if resolved_partitioning is not None:
            parser = PathPartitionParser(resolved_partitioning)
            partition_names = list(
                dict.fromkeys(
                    name for path in sample_paths for name in parser(path).keys()
                )
            )
            if partition_names:
                partition_schema = _partition_field_types_to_pa_schema(
                    field_names=partition_names,
                    field_types=resolved_partitioning.field_types or {},
                )
                for name in partition_names:
                    if schema.get_field_index(name) == -1:
                        schema = schema.append(partition_schema.field(name))

        for column in self._synthesized_columns:
            index = schema.get_field_index(column.name)
            field = pa.field(column.name, column.type)
            if index == -1:
                schema = schema.append(field)
            elif schema.field(index).type != column.type:
                schema = schema.set(index, field)

        return schema

    @override
    def resolve_partitioning(
        self, sample: Optional[FileManifest]
    ) -> Optional[Partitioning]:
        paths = sample.paths.tolist() if sample is not None else []
        return resolve_partitioning(self._partitioning, paths)

    @override
    def create_scanner(
        self,
        schema: pa.Schema,
        filesystem: Optional["FileSystem"] = None,
        **options,
    ) -> TextScanner:
        return TextScanner(
            schema=schema,
            encoding=self._encoding,
            drop_empty_lines=self._drop_empty_lines,
            filesystem=filesystem or self._filesystem,
            partitioning=options.get("partitioning", self._partitioning),
            synthesized_columns=self._synthesized_columns,
        )
