from typing import TYPE_CHECKING, Any, Callable, Dict, Iterator

from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data.block import Block, BlockAccessor
from ray.data.datasource.file_based_datasource import (
    FileBasedDatasource,
    _resolve_kwargs,
)
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    import pyarrow


@PublicAPI
class JSONDatasource(FileBasedDatasource):
    """JSON datasource, for reading and writing JSON and JSONL files.

    Examples:
        >>> import ray
        >>> from ray.data.datasource import JSONDatasource
        >>> source = JSONDatasource() # doctest: +SKIP
        >>> ray.data.read_datasource( # doctest: +SKIP
        ...     source, paths="/path/to/dir").take()
        [{"a": 1, "b": "foo"}, ...]
    """

    _FILE_EXTENSION = ["json", "jsonl"]

    # TODO(ekl) The PyArrow JSON reader doesn't support streaming reads.
    def _read_file(self, f: "pyarrow.NativeFile", path: str, **reader_args):
        from pyarrow import json

        read_options = reader_args.pop(
            "read_options", json.ReadOptions(use_threads=False)
        )
        return json.read_json(f, read_options=read_options, **reader_args)

    def _write_blocks(
        self,
        f: "pyarrow.NativeFile",
        blocks: Iterator[Block],
        writer_args_fn: Callable[[], Dict[str, Any]] = lambda: {},
        **writer_args,
    ):
        writer_args = _resolve_kwargs(writer_args_fn, **writer_args)
        orient = writer_args.pop("orient", "records")
        lines = writer_args.pop("lines", True)

        if orient == "records" and lines:
            # If we are writing record-by-record and records are delimited by
            # line, then it is safe to append.
            for block in blocks:
                block = BlockAccessor.for_block(block)
                block.to_pandas().to_json(f, orient=orient, lines=lines, **writer_args)
        else:
            builder = DelegatingBlockBuilder()
            for block in blocks:
                builder.add_block(block)
            # TODO(swang): We should add an out-of-memory warning if the block size
            # exceeds the max block size in the DataContext.
            block = builder.build()
            block = BlockAccessor.for_block(block)
            block.to_pandas().to_json(f, orient=orient, lines=lines, **writer_args)
