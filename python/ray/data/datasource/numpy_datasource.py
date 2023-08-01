from io import BytesIO
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterator

import numpy as np

from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data.block import Block, BlockAccessor
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

    def _write_blocks(
        self,
        f: "pyarrow.NativeFile",
        blocks: Iterator[Block],
        column: str,
        writer_args_fn: Callable[[], Dict[str, Any]] = lambda: {},
        **writer_args,
    ):
        # NOTE(swang): numpy does not have a streaming/append interface.
        builder = DelegatingBlockBuilder()
        for block in blocks:
            builder.add_block(block)
        # TODO(swang): We should add an out-of-memory warning if the block size
        # exceeds the max block size in the DataContext.
        block = builder.build()
        value = block.to_numpy(column)
        np.save(f, value)
