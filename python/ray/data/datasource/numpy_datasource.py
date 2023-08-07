from io import BytesIO
from typing import TYPE_CHECKING, Any, Callable, Dict

import numpy as np

from ray.data.block import BlockAccessor
from ray.data.datasource.file_based_datasource import FileBasedDatasource
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    import pyarrow


@PublicAPI
class NumpyDatasource(FileBasedDatasource):
    """Numpy datasource, for reading and writing Numpy files.

    Examples:
        >>> import ray
        >>> from ray.data.datasource import NumpyDatasource
        >>> source = NumpyDatasource() # doctest: +SKIP
        >>> ray.data.read_datasource( # doctest: +SKIP
        ...     source, paths="/path/to/dir").take()
        [array([0., 1., 2.]), ...]

    """

    _COLUMN_NAME = "data"
    _FILE_EXTENSION = "npy"

    def _read_file(self, f: "pyarrow.NativeFile", path: str, **reader_args):
        # TODO(ekl) Ideally numpy can read directly from the file, but it
        # seems like it requires the file to be seekable.
        buf = BytesIO()
        data = f.readall()
        buf.write(data)
        buf.seek(0)
        return BlockAccessor.batch_to_block({"data": np.load(buf, allow_pickle=True)})

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
