from io import BytesIO
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Union

import numpy as np

from ray.data.block import BlockAccessor
from ray.data.datasource.file_based_datasource import FileBasedDatasource
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    import pyarrow


@PublicAPI
class NumpyDatasource(FileBasedDatasource):
    """Numpy datasource, for reading and writing Numpy files."""

    _COLUMN_NAME = "data"
    _FILE_EXTENSION = "npy"

    def __init__(
        self,
        paths: Union[str, List[str]],
        numpy_load_args: Optional[Dict[str, Any]] = None,
        **file_based_datasource_kwargs,
    ):
        super().__init__(paths, **file_based_datasource_kwargs)

        if numpy_load_args is None:
            numpy_load_args = {}

        self.numpy_load_args = numpy_load_args

    def _read_file(self, f: "pyarrow.NativeFile", path: str):
        # TODO(ekl) Ideally numpy can read directly from the file, but it
        # seems like it requires the file to be seekable.
        buf = BytesIO()
        data = f.readall()
        buf.write(data)
        buf.seek(0)
        return BlockAccessor.batch_to_block(
            {"data": np.load(buf, allow_pickle=True, **self.numpy_load_args)}
        )

    def _write_block(
        self,
        f: "pyarrow.NativeFile",
        block: BlockAccessor,
        column: str,
        writer_args_fn: Callable[[], Dict[str, Any]] = lambda: {},
        **writer_args,
    ):
        value = block.to_numpy(column)
        np.save(f, value)
