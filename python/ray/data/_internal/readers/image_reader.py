import io
from typing import Iterable, List, Optional, Tuple

import numpy as np
import pyarrow

from .native_file_reader import NativeFileReader
from ray.data._internal.util import _check_import
from ray.data.block import DataBatch


class ImageReader(NativeFileReader):
    _NUM_THREADS_PER_TASK = 8

    def __init__(
        self,
        size: Optional[Tuple[int, int]] = None,
        mode: Optional[str] = None,
        **file_reader_kwargs,
    ):
        super().__init__(**file_reader_kwargs)

        _check_import(self, module="PIL", package="Pillow")

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

    def read_stream(self, file: "pyarrow.NativeFile", path: str) -> Iterable[DataBatch]:
        from PIL import Image, UnidentifiedImageError

        data = file.readall()

        try:
            image = Image.open(io.BytesIO(data))
        except UnidentifiedImageError as e:
            raise ValueError(f"PIL couldn't load image file at path '{path}'.") from e

        if self.size is not None:
            height, width = self.size
            image = image.resize((width, height), resample=Image.BILINEAR)
        if self.mode is not None:
            image = image.convert(self.mode)

        batch = np.expand_dims(np.array(image), axis=0)
        yield {"image": batch}

    def count_rows(self, paths: List[str], *, filesystem) -> int:
        return len(paths)

    def supports_count_rows(self) -> bool:
        return True
