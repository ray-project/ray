from typing import TYPE_CHECKING, List, Literal, Optional, Union

import numpy as np

import ray
from ray.data._internal.readers import FileReader
from ray.data._internal.util import _check_pyarrow_version, _is_local_scheme
from ray.data.datasource.datasource import Datasource, ReadTask
from ray.data.datasource.file_based_datasource import (
    _list_files,
    _shuffle_files,
    _validate_shuffle_arg,
)
from ray.data.datasource.file_meta_provider import BaseFileMetadataProvider
from ray.data.datasource.partitioning import Partitioning, PathPartitionFilter
from ray.data.datasource.path_util import _resolve_paths_and_filesystem

if TYPE_CHECKING:
    import pyarrow


class FileDatasource(Datasource):
    def __init__(
        self,
        paths: Union[str, List[str]],
        *,
        reader: FileReader,
        filesystem: Optional["pyarrow.fs.FileSystem"],
        schema: Optional[Union[type, "pyarrow.lib.Schema"]],
        meta_provider: BaseFileMetadataProvider,
        partition_filter: PathPartitionFilter,
        partitioning: Partitioning,
        ignore_missing_paths: bool,
        shuffle: Union[Literal["files"], None],
        file_extensions: Optional[List[str]],
    ):
        _check_pyarrow_version()

        _validate_shuffle_arg(shuffle)

        self._supports_distributed_reads = not _is_local_scheme(paths)
        if not self._supports_distributed_reads and ray.util.client.ray.is_connected():
            raise ValueError(
                "Because you're using Ray Client, read tasks scheduled on the Ray "
                "cluster can't access your local files. To fix this issue, store "
                "files in cloud storage or a distributed filesystem like NFS."
            )

        self._reader = reader
        self._meta_provider = meta_provider
        self._shuffle = shuffle
        self._schema = schema

        paths, self._filesystem = _resolve_paths_and_filesystem(paths, filesystem)
        self._paths, self._file_sizes = _list_files(
            paths,
            filesystem=self._filesystem,
            meta_provider=meta_provider,
            partition_filter=partition_filter,
            partitioning=partitioning,
            file_extensions=file_extensions,
            ignore_missing_paths=ignore_missing_paths,
        )

    def get_read_tasks(self, parallelism: int) -> List[ReadTask]:
        # Create local variables to avoid serializing 'self' in read tasks.
        paths, file_sizes = self._paths, self._file_sizes
        file_reader = self._reader
        filesystem = self._filesystem

        if self._shuffle == "files":
            paths, file_sizes = _shuffle_files(paths, file_sizes)

        # Fix https://github.com/ray-project/ray/issues/24296.
        parallelism = min(parallelism, len(paths))

        read_tasks = []
        for paths_split, file_sizes_split in zip(
            np.array_split(paths, parallelism),
            np.array_split(file_sizes, parallelism),
        ):
            if len(paths_split) <= 0:
                continue

            def create_read_fn(paths):
                def read_fn():
                    yield from file_reader.read_paths(paths, filesystem=filesystem)

                return read_fn

            if file_reader.supports_count_rows():
                num_rows = file_reader.count_rows(paths_split, filesystem=filesystem)
                num_rows_per_file = num_rows // len(paths_split)
            else:
                num_rows_per_file = None

            metadata = self._meta_provider(
                paths_split,
                self._schema,
                rows_per_file=num_rows_per_file,
                file_sizes=file_sizes_split,
            )

            read_task = ReadTask(create_read_fn(paths_split), metadata)
            read_tasks.append(read_task)

        return read_tasks

    def estimate_inmemory_data_size(self) -> Optional[int]:
        return sum(
            file_size if file_size is not None else 0 for file_size in self._file_sizes
        )

    @property
    def supports_distributed_reads(self) -> bool:
        return self._supports_distributed_reads
