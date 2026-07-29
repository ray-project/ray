import warnings
from io import BytesIO
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Union

import numpy as np

from ray.data.block import Block, BlockAccessor
from ray.data.datasource.file_based_datasource import FileBasedDatasource

if TYPE_CHECKING:
    import pyarrow


class NumpyDatasource(FileBasedDatasource):
    """Numpy datasource, for reading and writing Numpy files."""

    _COLUMN_NAME = "data"
    _FILE_EXTENSIONS = ["npy"]

    def __init__(
        self,
        paths: Union[str, List[str]],
        numpy_load_args: Optional[Dict[str, Any]] = None,
        allow_pickle: bool = False,
        **file_based_datasource_kwargs,
    ):
        super().__init__(paths, **file_based_datasource_kwargs)

        if numpy_load_args is None:
            numpy_load_args = {}

        if "allow_pickle" in numpy_load_args:
            warnings.warn(
                "`allow_pickle` in `numpy_load_args` is ignored. "
                "Use `read_numpy(..., allow_pickle=True)` instead "
                "if you want to load object-dtype .npy files.",
                stacklevel=2,
            )
            numpy_load_args = {
                k: v for k, v in numpy_load_args.items() if k != "allow_pickle"
            }
        self.allow_pickle = allow_pickle
        self.numpy_load_args = numpy_load_args

    def _read_stream(self, f: "pyarrow.NativeFile", path: str) -> Iterator[Block]:
        # TODO(ekl) Ideally numpy can read directly from the file, but it
        # seems like it requires the file to be seekable.
        buf = BytesIO()
        data = f.readall()
        buf.write(data)
        buf.seek(0)
        yield BlockAccessor.batch_to_block(
            {
                "data": np.load(
                    buf,
                    allow_pickle=self.allow_pickle,
                    **self.numpy_load_args,
                )
            }
        )
