import pathlib
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Union

import imageio as iio
import numpy as np
from ray.data.block import Block
from ray.data.datasource.binary_datasource import BinaryDatasource
from ray.data.datasource.datasource import ReadTask
from ray.data.datasource.file_based_datasource import _resolve_paths_and_filesystem
from ray.data.datasource.file_meta_provider import (
    BaseFileMetadataProvider,
    DefaultFileMetadataProvider,
    FastFileMetadataProvider,
)
from ray.data.datasource.partitioning import PathPartitionFilter

if TYPE_CHECKING:
    import pyarrow

IMAGE_EXTENSIONS = [".png", ".jpg", ".jpeg", ".tiff", ".bmp", ".gif"]


class ImageFolderDatasource(BinaryDatasource):
    """A datasource that lets you read datasets like `ImageNet <https://www.image-net.org/>`_.

    This datasource works with any dataset where images are arranged in this way:

    .. code-block::

        root/dog/xxx.png
        root/dog/xxy.png
        root/dog/[...]/xxz.png

        root/cat/123.png
        root/cat/nsdf3.png
        root/cat/[...]/asd932_.png

    Datasets read with ``ImageFolderDatasource`` contain two columns: ``'image'`` and
    ``'label'``. The ``'image'`` column contains ``ndarray`` objects of shape 
    :math:`(H, W, C)`, and the ``label`` column contains strings corresponding to
    labels.

    Examples:
        >>> import ray
        >>> from ray.data.datasource import ImageFolderDatasource
        >>>
        >>> ds = ray.data.read_datasource(  # doctest: +SKIP
        ...     ImageFolderDatasource(),
        ...     paths=["/data/imagenet/train"]
        ... )
        >>> sample = ds.take(1)[0]  # doctest: +SKIP
        >>> sample["image"].shape  # doctest: +SKIP
        (469, 387, 3)
        >>> sample["label"]  # doctest: +SKIP
        'n01443537'

    Raises:
        ValueError: if more than one path is provided. You should only provide the path
            to the dataset root.
    """

    def prepare_read(
        self,
        parallelism: int,
        paths: Union[str, List[str]],
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        schema: Optional[Union[type, "pyarrow.lib.Schema"]] = None,
        open_stream_args: Optional[Dict[str, Any]] = None,
        meta_provider: BaseFileMetadataProvider = DefaultFileMetadataProvider(),
        partition_filter: PathPartitionFilter = None,
        # TODO(ekl) deprecate this once read fusion is available.
        _block_udf: Optional[Callable[[Block], Block]] = None,
        **reader_args,
    ) -> List[ReadTask]:
        if len(paths) != 1:
            raise ValueError(
                "`ImageFolderDatasource` expects 1 path representing the dataset "
                f"root, but it got {len(paths)} paths instead. To fix this error, "
                "pass in a single-element list containing the dataset root (for "
                'example, `paths=["s3://imagenet/train"]`)'
            )

        try:
            import imageio  # noqa: F401
        except ImportError:
            raise ValueError(
                "`ImageFolderDatasource` depends on 'imageio', but 'imageio' couldn't "
                "be imported. You can install 'imageio' by running "
                "`pip install imageio`."
            )

        # We call `_resolve_paths_and_filesystem` so that the dataset root is formatted
        # in the same way as the paths passed to `_get_class_from_path`.
        paths, filesystem = _resolve_paths_and_filesystem(paths, filesystem)
        self.root = paths[0]

        paths, _ = meta_provider.expand_paths(paths, filesystem)
        paths = [path for path in paths if _is_image_file(path)]

        return super().prepare_read(
            parallelism=parallelism,
            paths=paths,
            filesystem=filesystem,
            schema=schema,
            open_stream_args=open_stream_args,
            meta_provider=FastFileMetadataProvider(),
            partition_filter=partition_filter,
            _block_udf=_block_udf,
        )

    def _read_file(self, f: "pyarrow.NativeFile", path: str, **reader_args):
        import pandas as pd

        records = super()._read_file(f, path, include_paths=True)
        assert len(records) == 1
        path, data = records[0]

        image = iio.imread(data)
        label = _get_class_from_path(path, self.root)

        return pd.DataFrame({"image": [np.array(image)], "label": [label]})


def _is_image_file(path: str) -> bool:
    return any(path.lower().endswith(extension) for extension in IMAGE_EXTENSIONS)


def _get_class_from_path(path: str, root: str) -> str:
    # The class is the name of the first directory after the root. For example, if
    # the root is "/data/imagenet/train" and the path is
    # "/data/imagenet/train/n01443537/images/n01443537_0.JPEG", then the class is
    # "n01443537".
    path, root = pathlib.PurePath(path), pathlib.PurePath(root)
    assert root in path.parents
    return path.parts[len(root.parts) :][0]
