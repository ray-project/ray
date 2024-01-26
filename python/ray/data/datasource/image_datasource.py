import io
import logging
import time
from typing import TYPE_CHECKING, Iterator, List, Optional, Tuple, Union

import numpy as np

from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.util import _check_import
from ray.data.block import Block, BlockMetadata
from ray.data.datasource.file_based_datasource import FileBasedDatasource
from ray.data.datasource.file_meta_provider import DefaultFileMetadataProvider
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    import pyarrow


logger = logging.getLogger(__name__)

# The default size multiplier for reading image data source.
# This essentially is using image on-disk file size to estimate
# in-memory data size.
IMAGE_ENCODING_RATIO_ESTIMATE_DEFAULT = 1

# The lower bound value to estimate image encoding ratio.
IMAGE_ENCODING_RATIO_ESTIMATE_LOWER_BOUND = 0.5

@DeveloperAPI
def _tiff_to_np(tiff_path: bytes):
    """Helper function to read in multi-frame TIFF files and convert them to NumPy arrays.

    This function reads a multi-frame TIFF file from the given path and converts each frame into a NumPy array. It then stacks these arrays along a new axis (axis=0), effectively creating a 3D NumPy array where each 2D array represents a frame from the TIFF file.

    Examples:

        >>> from PIL import Image
        >>> import numpy as np
        >>> tiff_path = b'path/to/multiframe.tiff'
        >>> np_stack = _tiff_to_np(tiff_path)
        >>> np_stack.shape
        (num_frames, height, width)
        >>> type(np_stack)
        <class 'numpy.ndarray'>

    Args:
        tiff_path: The file path to the multi-frame TIFF file. The path should be provided as a byte string.

    Returns:
        A 3D NumPy array where each 2D array along axis 0 represents a frame from the TIFF file.
    """
    from PIL import Image

    with Image.open(tiff_path) as img:
        np_arrays = []

        for i in range(img.n_frames):
            img.seek(i)

            np_arrays.append(np.array(img))
        np_stack = np.stack(np_arrays, axis=0)

    return np_stack


@DeveloperAPI
class ImageDatasource(FileBasedDatasource):
    """A datasource that lets you read images."""

    _WRITE_FILE_PER_ROW = True
    _FILE_EXTENSIONS = ["png", "jpg", "jpeg", "tif", "tiff", "bmp", "gif"]
    # Use 8 threads per task to read image files.
    _NUM_THREADS_PER_TASK = 8

    def __init__(
        self,
        paths: Union[str, List[str]],
        size: Optional[Tuple[int, int]] = None,
        mode: Optional[str] = None,
        **file_based_datasource_kwargs,
    ):
        super().__init__(paths, **file_based_datasource_kwargs)

        _check_import(self, module="PIL", package="Pillow")
        _check_import(
            self, module="tifffile", package="tifffile"
        )  # imageio supports both tiff and Pillow.

        if size is not None and len(size) != 2:
            raise ValueError(
                "Expected `size` to contain two integers for height and width, "
                f"but got {len(size)} integers instead."
            )

        if size is not None and (size[0] < 0 or size[1] < 0):
            raise ValueError(
                f"Expected `size` to contain positive integers, but got {size} instead."
            )

        self.size = size
        self.mode = mode

        meta_provider = file_based_datasource_kwargs.get("meta_provider", None)
        if isinstance(meta_provider, _ImageFileMetadataProvider):
            self._encoding_ratio = self._estimate_files_encoding_ratio()
            meta_provider._set_encoding_ratio(self._encoding_ratio)
        else:
            self._encoding_ratio = IMAGE_ENCODING_RATIO_ESTIMATE_DEFAULT

    def _read_stream(self, f: "pyarrow.NativeFile", path: str) -> Iterator[Block]:
        from PIL import Image, UnidentifiedImageError

        print(path)
        data = f.readall()
        if path.lower().endswith((".tif", ".tiff")):
            image = _tiff_to_np(io.BytesIO(data))
        else:
            try:
                image = Image.open(io.BytesIO(data))
            except UnidentifiedImageError as e:
                raise ValueError(
                    f"PIL couldn't load image file at path '{path}'."
                ) from e

            if self.size is not None:
                height, width = self.size
                image = image.resize((width, height))
            if self.mode is not None:
                image = image.convert(self.mode)

        builder = DelegatingBlockBuilder()
        array = np.array(image)
        item = {"image": array}
        builder.add(item)
        block = builder.build()

        yield block

    def _rows_per_file(self):
        return 1

    def estimate_inmemory_data_size(self) -> Optional[int]:
        total_size = 0
        for file_size in self._file_sizes():
            # NOTE: check if file size is not None, because some metadata provider
            # such as FastFileMetadataProvider does not provide file size information.
            if file_size is not None:
                total_size += file_size
        return total_size * self._encoding_ratio

    def _estimate_files_encoding_ratio(self) -> float:
        """Return an estimate of the image files encoding ratio."""
        start_time = time.perf_counter()
        # Filter out empty file to avoid noise.
        non_empty_path_and_size = list(
            filter(lambda p: p[1] > 0, zip(self._paths(), self._file_sizes()))
        )
        num_files = len(non_empty_path_and_size)
        if num_files == 0:
            logger.warn(
                "All input image files are empty. "
                "Use on-disk file size to estimate images in-memory size."
            )
            return IMAGE_ENCODING_RATIO_ESTIMATE_DEFAULT

        if self.size is not None and self.mode is not None:
            # Use image size and mode to calculate data size for all images,
            # because all images are homogeneous with same size after resizing.
            # Resizing is enforced when reading every image in `ImageDatasource`
            # when `size` argument is provided.
            if self.mode in ["1", "L", "P"]:
                dimension = 1
            elif self.mode in ["RGB", "YCbCr", "LAB", "HSV"]:
                dimension = 3
            elif self.mode in ["RGBA", "CMYK", "I", "F"]:
                dimension = 4
            else:
                logger.warn(f"Found unknown image mode: {self.mode}.")
                return IMAGE_ENCODING_RATIO_ESTIMATE_DEFAULT
            height, width = self.size
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
