import io
import logging
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
)
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data.datasource.file_meta_provider import DefaultFileMetadataProvider
from ray.data.datasource.partitioning import Partitioning, PathPartitionFilter
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    import pyarrow
    from ray.data.block import T


logger = logging.getLogger(__name__)

# The default size multiplier for reading image data source.
# This essentially is using image on-disk file size to estimate
# in-memory data size.
IMAGE_ENCODING_RATIO_ESTIMATE_DEFAULT = 1

# The lower bound value to estimate image encoding ratio.
IMAGE_ENCODING_RATIO_ESTIMATE_LOWER_BOUND = 0.5


@DeveloperAPI
class ImageDatasource(BinaryDatasource):
    """A datasource that lets you read images."""

    _FILE_EXTENSION = ["png", "jpg", "jpeg", "tiff", "bmp", "gif"]

    def create_reader(
        self,
        size: Optional[Tuple[int, int]] = None,
        mode: Optional[str] = None,
        include_paths: bool = False,
        **kwargs,
    ) -> "Reader[T]":
        if size is not None and len(size) != 2:
            raise ValueError(
                "Expected `size` to contain two integers for height and width, "
                f"but got {len(size)} integers instead."
            )
        if size is not None and (size[0] < 0 or size[1] < 0):
            raise ValueError(
                f"Expected `size` to contain positive integers, but got {size} instead."
            )

        _check_import(self, module="PIL", package="Pillow")

        return _ImageDatasourceReader(
            self, size=size, mode=mode, include_paths=include_paths, **kwargs
        )

    def _convert_block_to_tabular_block(
        self, block: Block, column_name: Optional[str] = None
    ) -> Block:
        import pandas as pd
        import pyarrow as pa

        assert isinstance(block, (pa.Table, pd.DataFrame))
        return block

    def _read_file(
        self,
        f: "pyarrow.NativeFile",
        path: str,
        size: Optional[Tuple[int, int]],
        mode: Optional[str],
        include_paths: bool,
    ) -> "pyarrow.Table":
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

        builder = DelegatingBlockBuilder()
        array = np.array(image)
        if include_paths:
            item = {"image": array, "path": path}
        else:
            item = {"image": array}
        builder.add(item)
        block = builder.build()

        return block


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


class _ImageDatasourceReader(_FileBasedDatasourceReader):
    def __init__(
        self,
        delegate: FileBasedDatasource,
        paths: List[str],
        filesystem: "pyarrow.fs.FileSystem",
        partition_filter: PathPartitionFilter,
        partitioning: Partitioning,
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
            partitioning=partitioning,
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
            # Resizing is enforced when reading every image in `ImageDatasource`
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
