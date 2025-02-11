from io import BytesIO
from typing import TYPE_CHECKING, Any, Dict, Iterator, Optional

import numpy as np

from .native_file_reader import NativeFileReader
from ray.data.block import Block

if TYPE_CHECKING:
    import pyarrow


class NumpyReader(NativeFileReader):
    def __init__(
        self,
        *,
        numpy_load_args: Optional[Dict[str, Any]],
        **file_reader_kwargs,
    ):
        super().__init__(**file_reader_kwargs)

        if numpy_load_args is None:
            numpy_load_args = {}

        self._numpy_load_args = numpy_load_args

    def read_stream(self, file: "pyarrow.NativeFile", path: str) -> Iterator[Block]:
        # TODO(ekl) Ideally numpy can read directly from the file, but it
        # seems like it requires the file to be seekable.
        buf = BytesIO()
        data = file.readall()
        buf.write(data)
        buf.seek(0)
        yield {"data": np.load(buf, allow_pickle=True, **self._numpy_load_args)}
