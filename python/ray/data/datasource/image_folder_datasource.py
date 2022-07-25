import importlib
import pathlib
from typing import TYPE_CHECKING, Tuple

import numpy as np
from ray.data.datasource.binary_datasource import BinaryDatasource
from ray.data.datasource.datasource import Reader
from ray.data.datasource.file_based_datasource import (
    _resolve_paths_and_filesystem,
    FileExtensionFilter,
)
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    import pyarrow
    from ray.data.block import T

IMAGE_EXTENSIONS = ["png", "jpg", "jpeg", "tiff", "bmp", "gif"]


@DeveloperAPI
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

    Datasets read with this datasource contain two columns: ``'image'`` and ``'label'``.

    * The ``'image'`` column is of type
      :py:class:`~ray.air.util.tensor_extensions.pandas.TensorDtype` and contains
      tensors of shape :math:`(H, W, C)`.
    * The ``'label'`` column contains strings representing class names (e.g., 'cat').

    Examples:
        >>> import ray
        >>> from ray.data.datasource import ImageFolderDatasource
        >>> ds = ray.data.read_datasource(  # doctest: +SKIP
        ...     ImageFolderDatasource(),
        ...     root="/data/imagenet/train",
        ...     size=(224, 224)
        ... )
        >>> sample = ds.take(1)[0]  # doctest: +SKIP
        >>> sample["image"].to_numpy().shape  # doctest: +SKIP
        (224, 224, 3)
        >>> sample["label"]  # doctest: +SKIP
        'n01443537'

        To convert class labels to integer-valued targets, use
        :py:class:`~ray.data.preprocessors.OrdinalEncoder`.

        >>> import ray
        >>> from ray.data.preprocessors import OrdinalEncoder
        >>> ds = ray.data.read_datasource(  # doctest: +SKIP
        ...     ImageFolderDatasource(),
        ...     root="/data/imagenet/train",
        ...     size=(224, 224)
        ... )
        >>> oe = OrdinalEncoder(columns=["label"])  # doctest: +SKIP
        >>> ds = oe.fit_transform(ds)  # doctest: +SKIP
        >>> sample = ds.take(1)[0]  # doctest: +SKIP
        >>> sample["label"]  # doctest: +SKIP
        71
    """  # noqa: E501

    def create_reader(
        self,
        root: str,
        size: Tuple[int, int],
    ) -> "Reader[T]":
        """Return a :py:class:`Reader` that reads images.

        Args:
            root: Path to the dataset root.
            size: The desired height and width of loaded images.

        Raises:
            ValueError: if ``size`` contains non-positive numbers.
        """
        if size[0] < 0 or size[1] < 0:
            raise ValueError("Expected `size` to contain positive integers, but got {size}.")

        self._check_import(module="imageio", package="imagio")
        self._check_import(module="skimage", package="scikit-image")

        # We call `_resolve_paths_and_filesystem` so that the dataset root is formatted
        # in the same way as the paths passed to `_get_class_from_path`.
        paths, _ = _resolve_paths_and_filesystem([root])
        root = paths[0]

        return super().create_reader(
            paths=paths,
            partition_filter=FileExtensionFilter(file_extensions=IMAGE_EXTENSIONS),
            root=root,
            size=size,
        )

    def _read_file(
        self, f: "pyarrow.NativeFile", path: str, root: str, size: Tuple[int, int]
    ):
        import imageio as iio
        import pandas as pd
        from ray.data.extensions import TensorArray
        import skimage

        records = super()._read_file(f, path, include_paths=True)
        assert len(records) == 1
        path, data = records[0]

        image = iio.imread(data)
        image = skimage.transform.resize(image, size)
        image = skimage.util.img_as_ubyte(image)
        label = _get_class_from_path(path, root)

        return pd.DataFrame(
            {
                "image": TensorArray([np.array(image)]),
                "label": [label],
            }
        )

    def _check_import(self, *, module: str, package: str) -> None:
        """Check if a required dependency is installed.

        If `module` can't be imported, this function raises an `ImportError` instructing
        the user to install `package` from PyPI.

        Args:
            module: The name of the module to import.
            package: The name of the package on PyPI.
        """
        try:
            importlib.import_module(module)
        except ImportError:
            raise ImportError(
                f"`{self.__class__.__name__}` depends on '{package}', but '{package}' "
                f"couldn't be imported. You can install '{package}' by running `pip "
                f"install {package}`."
            )


def _get_class_from_path(path: str, root: str) -> str:
    # The class is the name of the first directory after the root. For example, if
    # the root is "/data/imagenet/train" and the path is
    # "/data/imagenet/train/n01443537/images/n01443537_0.JPEG", then the class is
    # "n01443537".
    path, root = pathlib.PurePath(path), pathlib.PurePath(root)
    assert root in path.parents
    return path.parts[len(root.parts) :][0]
