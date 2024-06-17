import warnings
from typing import TYPE_CHECKING, Any, Dict, List, Literal, Optional, Tuple, Union

from ray.anyscale.data._internal.logical.operators.expand_paths_operator import (
    ExpandPaths,
)
from ray.anyscale.data._internal.logical.operators.read_files_operator import ReadFiles
from ray.anyscale.data.datasource.file_reader import FileReader
from ray.data._internal.logical.interfaces import LogicalPlan
from ray.data._internal.plan import ExecutionPlan
from ray.data._internal.stats import DatasetStats
from ray.data.dataset import Dataset
from ray.data.datasource import ImageDatasource, Partitioning, PathPartitionFilter
from ray.data.datasource.file_meta_provider import BaseFileMetadataProvider
from ray.data.datasource.path_util import _resolve_paths_and_filesystem

if TYPE_CHECKING:
    import pyarrow.fs


def read_images(
    paths: Union[str, List[str]],
    *,
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    parallelism: int = -1,
    meta_provider: Optional[BaseFileMetadataProvider] = None,
    ray_remote_args: Dict[str, Any] = None,
    arrow_open_file_args: Optional[Dict[str, Any]] = None,
    partition_filter: Optional[PathPartitionFilter] = None,
    partitioning: Partitioning = None,
    size: Optional[Tuple[int, int]] = None,
    mode: Optional[str] = None,
    include_paths: bool = False,
    ignore_missing_paths: bool = False,
    shuffle: Union[Literal["files"], None] = None,
    file_extensions: Optional[List[str]] = ImageDatasource._FILE_EXTENSIONS,
    concurrency: Optional[int] = None,
    override_num_blocks: Optional[int] = None,
) -> Dataset:
    from ray.anyscale.data.datasource.image_reader import ImageReader
    from ray.data.read_api import read_images as read_images_fallback

    if (
        parallelism != -1
        or meta_provider is not None
        or override_num_blocks is not None
        or shuffle is not None
    ):
        if meta_provider is not None:
            # TODO(@bveeramani): Update this warning message.
            warnings.warn("You don't need to use `meta_provider` blah blah blah.")

        return read_images_fallback(
            paths,
            filesystem=filesystem,
            parallelism=parallelism,
            meta_provider=meta_provider,
            ray_remote_args=ray_remote_args,
            arrow_open_file_args=arrow_open_file_args,
            partition_filter=partition_filter,
            partitioning=partitioning,
            size=size,
            mode=mode,
            include_paths=include_paths,
            ignore_missing_paths=ignore_missing_paths,
            shuffle=shuffle,
            file_extensions=file_extensions,
            concurrency=concurrency,
            override_num_blocks=override_num_blocks,
        )

    reader = ImageReader(
        size=size,
        mode=mode,
        include_paths=include_paths,
        partitioning=partitioning,
        open_args=arrow_open_file_args,
    )
    return read_files(
        paths,
        reader,
        filesystem=filesystem,
        partition_filter=partition_filter,
        ignore_missing_paths=ignore_missing_paths,
        file_extensions=file_extensions,
        concurrency=concurrency,
        ray_remote_args=ray_remote_args,
    )


def read_files(
    paths: Union[str, List[str]],
    reader: FileReader,
    *,
    filesystem: Optional["pyarrow.fs.FileSystem"],
    partition_filter: Optional[PathPartitionFilter],
    ignore_missing_paths: bool,
    file_extensions: Optional[List[str]],
    concurrency: Optional[int],
    ray_remote_args: Dict[str, Any],
) -> Dataset:
    paths, filesystem = _resolve_paths_and_filesystem(paths, None)
    expand_paths_op = ExpandPaths(
        paths=paths,
        reader=reader,
        filesystem=filesystem,
        ignore_missing_paths=ignore_missing_paths,
        file_extensions=file_extensions,
        partition_filter=partition_filter,
    )
    read_files_op = ReadFiles(
        expand_paths_op,
        paths=paths,
        reader=reader,
        filesystem=filesystem,
        ray_remote_args=ray_remote_args,
        concurrency=concurrency,
    )
    logical_plan = LogicalPlan(read_files_op)
    return Dataset(
        plan=ExecutionPlan(
            DatasetStats(metadata={"ReadFiles": []}, parent=None),
            run_by_consumer=False,
        ),
        logical_plan=logical_plan,
    )
