from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union

from ray.data.dataset import Dataset
from ray.data.datasource import (
    BaseFileMetadataProvider,
    ImageDatasource,
    PathPartitionFilter,
)
from ray.data.datasource._default_metadata_providers import get_image_metadata_provider
from ray.data.datasource.partitioning import Partitioning
from ray.data.read_api import read_datasource
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    import pyarrow


@PublicAPI(stability="beta")
def read_images(
    paths: Union[str, List[str]],
    *,
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    parallelism: int = -1,
    meta_provider: Optional[BaseFileMetadataProvider] = None,
    ray_remote_args: Dict[str, Any] = None,
    arrow_open_file_args: Optional[Dict[str, Any]] = None,
    partition_filter: Optional[
        PathPartitionFilter
    ] = ImageDatasource.file_extension_filter(),
    partitioning: Partitioning = None,
    size: Optional[Tuple[int, int]] = None,
    mode: Optional[str] = None,
    include_paths: bool = False,
    ignore_missing_paths: bool = False,
    shuffle: bool = False,
    shuffle_seed: Optional[int] = None,
) -> Dataset:
    """Creates a :class:`~ray.data.Dataset` from image files.

    .. note::
        Compare to :meth:`~ray.data.read_images`, this API supports random
        shuffle the image files order before read. This is useful for model
        training which requires to shuffle samples per epoch.

    Examples:
        >>> import ray
        >>> path = "s3://anonymous@ray-example-data/batoidea/JPEGImages/"
        >>> ds = ray.data.read_images(path, shuffle=True, shuffle_seed=9176)

    Args:
        shuffle: If ``True``, random shuffle image files order before read.
            Defaults to ``False``.
        shuffle_seed: The shuffle seed to use. Default to ``None``.
    """
    if meta_provider is None:
        meta_provider = get_image_metadata_provider()
    return read_datasource(
        ImageDatasource(),
        paths=paths,
        filesystem=filesystem,
        parallelism=parallelism,
        meta_provider=meta_provider,
        ray_remote_args=ray_remote_args,
        open_stream_args=arrow_open_file_args,
        partition_filter=partition_filter,
        partitioning=partitioning,
        size=size,
        mode=mode,
        include_paths=include_paths,
        ignore_missing_paths=ignore_missing_paths,
        shuffle=shuffle,
        shuffle_seed=shuffle_seed,
    )
