import pathlib
from typing import TYPE_CHECKING, List, Optional, Union

import numpy as np
from ray.data.datasource.binary_datasource import BinaryDatasource
from ray.data.datasource.datasource import Reader
from ray.data.datasource.file_based_datasource import (
    _resolve_paths_and_filesystem,
    FileExtensionFilter,
)
from ray.data.datasource.partitioning import PathPartitionFilter

if TYPE_CHECKING:
    import pyarrow
    from ray.data.block import T

IMAGE_EXTENSIONS = ["png", "jpg", "jpeg", "tiff", "bmp", "gif"]


class ImageFolderDatasource(BinaryDatasource):
    """A datasource that lets you read datasets like `ImageNet <https://www.image-net.org/>`_."""  # noqa: E501

    def create_reader(
        self,
        paths: Union[str, List[str]],
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        partition_filter: PathPartitionFilter = None,
        **kwargs,
    ) -> "Reader[T]":
        if len(paths) != 1:
            raise ValueError(
                "`ImageFolderDatasource` expects 1 path representing the dataset "
                f"root, but it got {len(paths)} paths instead. To fix this "
                "error, pass in a single-element list containing the dataset root "
                '(for example, `paths=["s3://imagenet/train"]`)'
            )

        try:
            import imageio  # noqa: F401
        except ImportError:
            raise ImportError(
                "`ImageFolderDatasource` depends on 'imageio', but 'imageio' couldn't "
                "be imported. You can install 'imageio' by running "
                "`pip install imageio`."
            )

        if partition_filter is None:
            partition_filter = FileExtensionFilter(file_extensions=IMAGE_EXTENSIONS)

        # We call `_resolve_paths_and_filesystem` so that the dataset root is formatted
        # in the same way as the paths passed to `_get_class_from_path`.
        paths, filesystem = _resolve_paths_and_filesystem(paths, filesystem)
        self.root = paths[0]

        return super().create_reader(
            paths=paths,
            filesystem=filesystem,
            partition_filter=partition_filter,
            **kwargs,
        )

    def _read_file(self, f: "pyarrow.NativeFile", path: str, **reader_args):
        import imageio as iio
        import pandas as pd
        from ray.data.extensions import TensorArray

        records = super()._read_file(f, path, include_paths=True)
        assert len(records) == 1
        path, data = records[0]

        image = iio.imread(data)
        label = _get_class_from_path(path, self.root)

        return pd.DataFrame(
            {
                "image": TensorArray([np.array(image)]),
                "label": [label],
            }
        )


def _get_class_from_path(path: str, root: str) -> str:
    # The class is the name of the first directory after the root. For example, if
    # the root is "/data/imagenet/train" and the path is
    # "/data/imagenet/train/n01443537/images/n01443537_0.JPEG", then the class is
    # "n01443537".
    path, root = pathlib.PurePath(path), pathlib.PurePath(root)
    assert root in path.parents
    return path.parts[len(root.parts) :][0]
