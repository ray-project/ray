from typing import Any, Callable, Dict, Optional

import pyarrow

from ray.data.block import BlockAccessor
from ray.data.datasource.file_based_datasource import _resolve_kwargs
from ray.data.datasource.file_datasink import BlockBasedFileDatasink


class JSONDatasink(BlockBasedFileDatasink):
    def __init__(
        self,
        path: str,
        *,
        pandas_json_args_fn: Optional[Callable[[], Dict[str, Any]]] = None,
        pandas_json_args: Optional[Dict[str, Any]] = None,
        file_format: str = "json",
        **file_datasink_kwargs,
    ):
        super().__init__(path, file_format=file_format, **file_datasink_kwargs)

        if pandas_json_args_fn is None:
            pandas_json_args_fn = lambda: {}  # noqa: E731

        if pandas_json_args is None:
            pandas_json_args = {}

        self.pandas_json_args_fn = pandas_json_args_fn
        self.pandas_json_args = pandas_json_args

    def write_block_to_file(self, block: BlockAccessor, file: "pyarrow.NativeFile"):
        writer_args = _resolve_kwargs(self.pandas_json_args_fn, **self.pandas_json_args)
        orient = writer_args.pop("orient", "records")
        lines = writer_args.pop("lines", True)

        block.to_pandas().to_json(file, orient=orient, lines=lines, **writer_args)
