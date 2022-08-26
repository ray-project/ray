import io
import logging
import pathlib
import time
from typing import TYPE_CHECKING, List, Optional, Tuple, Union

import numpy as np

from ray.data._internal.util import _check_import
from ray.data.block import Block, BlockMetadata
from ray.data.datasource.binary_datasource import BinaryDatasource
from ray.data.datasource.datasource import Reader
from ray.data.datasource.file_based_datasource import (
    _FileBasedDatasourceReader,
    FileBasedDatasource,
    _resolve_paths_and_filesystem,
    FileExtensionFilter,
)
from ray.data.datasource.file_meta_provider import DefaultFileMetadataProvider
from ray.data.datasource.partitioning import PathPartitionFilter
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    import pyarrow
    from ray.data.block import T


logger = logging.getLogger(__name__)

IMAGE_EXTENSIONS = ["png", "jpg", "jpeg", "tiff", "bmp", "gif"]

# The default size multiplier for reading image data source.
# This essentially is using image on-disk file size to estimate
# in-memory data size.
IMAGE_ENCODING_RATIO_ESTIMATE_DEFAULT = 1

# The lower bound value to estimate image encoding ratio.
IMAGE_ENCODING_RATIO_ESTIMATE_LOWER_BOUND = 0.5


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
      :py:class:`~ray.air.util.tensor_extensions.pandas.TensorDtype`. The shape of the
      tensors are :math:`(H, W)` if the images are grayscale and :math:`(H, W, C)`
      otherwise.
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
        size: Optional[Tuple[int, int]] = None,
        mode: Optional[str] = None,
    ) -> "Reader[T]":
        """Return a :py:class:`~ray.data.datasource.Reader` that reads images.

        .. warning::
            If your dataset contains images of varying sizes and you don't specify
            ``size``, this datasource will error. To prevent errors, specify ``size``
            or :ref:`disable tensor extension casting <disable_tensor_extension_casting>`.

        Args:
            root: Path to the dataset root.
            size: The desired height and width of loaded images. If unspecified, images
                retain their original shape.
            mode: A `Pillow mode <https://pillow.readthedocs.io/en/stable/handbook/concepts.html#modes>`_
                describing the desired type and depth of pixels. If unspecified, image
                modes are inferred by
                `Pillow <https://pillow.readthedocs.io/en/stable/index.html>`_.

        Raises:
            ValueError: if ``size`` contains non-positive numbers.
            ValueError: if ``mode`` is unsupported.
        """  # noqa: E501
        if size is not None and len(size) != 2:
            raise ValueError(
                "Expected `size` to contain 2 integers for height and width, "
                f"but got {len(size)} integers instead."
            )
        if size is not None and (size[0] < 0 or size[1] < 0):
            raise ValueError(
                f"Expected `size` to contain positive integers, but got {size} instead."
            )

        _check_import(self, module="PIL", package="Pillow")
        _check_import(self, module="pandas", package="pandas")

        # We call `_resolve_paths_and_filesystem` so that the dataset root is formatted
        # in the same way as the paths passed to `_get_class_from_path`.
        paths, filesystem = _resolve_paths_and_filesystem([root])
        root = paths[0]

        return _ImageFolderDatasourceReader(
            delegate=self,
            paths=paths,
            filesystem=filesystem,
            partition_filter=FileExtensionFilter(file_extensions=IMAGE_EXTENSIONS),
            root=root,
            size=size,
            mode=mode,
        )

    def _read_file(
        self,
        f: "pyarrow.NativeFile",
        path: str,
        root: str,
        size: Optional[Tuple[int, int]],
        mode: Optional[str],
    ) -> Block:
        import pandas as pd
        from PIL import Image

        records = super()._read_file(f, path, include_paths=True)
        assert len(records) == 1
        path, data = records[0]

        image = Image.open(io.BytesIO(data))
        if size is not None:
            height, width = size
            image = image.resize((width, height))
        if mode is not None:
            image = image.convert(mode)

        label = _get_class_from_path(path, root)

        return pd.DataFrame(
            {
                "image": [np.array(image)],
                "label": [label],
            }
        )


class _ImageFileMetadataProvider(DefaultFileMetadataProvider):
    def _set_encoding_ratio(self, encoding_ratio: int):
        """Set image file encoding ratio, to provide accurate size in bytes metadata."""
        self._encoding_ratio = encoding_ratio

    def _get_block_metadata(
        self,
        paths: List[str],
        schema: Optional[Union[type, "pyarrow.lib.Schema"]],
        *,
        rows_per_file: Optional[int],
        file_sizes: List[Optional[int]],
    ) -> BlockMetadata:
        metadata = super()._get_block_metadata(
            paths, schema, rows_per_file=rows_per_file, file_sizes=file_sizes
        )
        if metadata.size_bytes is not None:
            metadata.size_bytes = int(metadata.size_bytes * self._encoding_ratio)
        return metadata


class _ImageFolderDatasourceReader(_FileBasedDatasourceReader):
    def __init__(
        self,
        delegate: FileBasedDatasource,
        paths: List[str],
        filesystem: "pyarrow.fs.FileSystem",
        partition_filter: PathPartitionFilter,
        meta_provider: _ImageFileMetadataProvider = _ImageFileMetadataProvider(),
        **reader_args,
    ):
        super().__init__(
            delegate=delegate,
            paths=paths,
            filesystem=filesystem,
            schema=None,
            open_stream_args=None,
            meta_provider=meta_provider,
            partition_filter=partition_filter,
            **reader_args,
        )
        self._encoding_ratio = self._estimate_files_encoding_ratio()
        meta_provider._set_encoding_ratio(self._encoding_ratio)

    def estimate_inmemory_data_size(self) -> Optional[int]:
        return sum(self._file_sizes) * self._encoding_ratio

    def _estimate_files_encoding_ratio(self) -> float:
        """Return an estimate of the image files encoding ratio."""
        start_time = time.perf_counter()
        # Filter out empty file to avoid noise.
        non_empty_path_and_size = list(
            filter(lambda p: p[1] > 0, zip(self._paths, self._file_sizes))
        )
        num_files = len(non_empty_path_and_size)
        if num_files == 0:
            logger.warn(
                "All input image files are empty. "
                "Use on-disk file size to estimate images in-memory size."
            )
            return IMAGE_ENCODING_RATIO_ESTIMATE_DEFAULT

        size = self._reader_args.get("size")
        mode = self._reader_args.get("mode")
        if size is not None and mode is not None:
            # Use image size and mode to calculate data size for all images,
            # because all images are homogeneous with same size after resizing.
            # Resizing is enforced when reading every image in ImageFolderDatasource
            # when `size` argument is provided.
            if mode in ["1", "L", "P"]:
                dimension = 1
            elif mode in ["RGB", "YCbCr", "LAB", "HSV"]:
                dimension = 3
            elif mode in ["RGBA", "CMYK", "I", "F"]:
                dimension = 4
            else:
                logger.warn(f"Found unknown image mode: {mode}.")
                return IMAGE_ENCODING_RATIO_ESTIMATE_DEFAULT
            height, width = size
            single_image_size = height * width * dimension
            total_estimated_size = single_image_size * num_files
            total_file_size = sum(p[1] for p in non_empty_path_and_size)
            ratio = total_estimated_size / total_file_size
        else:
            # TODO(chengsu): sample images to estimate data size
            ratio = IMAGE_ENCODING_RATIO_ESTIMATE_DEFAULT

        sampling_duration = time.perf_counter() - start_time
        if sampling_duration > 5:
            logger.warn(
                "Image input size estimation took "
                f"{round(sampling_duration, 2)} seconds."
            )
        logger.debug(f"Estimated image encoding ratio from sampling is {ratio}.")
        return max(ratio, IMAGE_ENCODING_RATIO_ESTIMATE_LOWER_BOUND)


def _get_class_from_path(path: str, root: str) -> str:
    # The class is the name of the first directory after the root. For example, if
    # the root is "/data/imagenet/train" and the path is
    # "/data/imagenet/train/n01443537/images/n01443537_0.JPEG", then the class is
    # "n01443537".
    path, root = pathlib.PurePath(path), pathlib.PurePath(root)
    assert root in path.parents
    return path.parts[len(root.parts) :][0]
