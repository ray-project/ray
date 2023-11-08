from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Union

from ray.data.block import BlockAccessor
from ray.data.datasource.file_based_datasource import (
    FileBasedDatasource,
    _resolve_kwargs,
)
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    import pyarrow


@PublicAPI
class JSONDatasource(FileBasedDatasource):
    """JSON datasource, for reading and writing JSON and JSONL files."""

    _FILE_EXTENSION = ["json", "jsonl"]

    def __init__(
        self,
        paths: Union[str, List[str]],
        *,
        arrow_json_args: Optional[Dict[str, Any]] = None,
        **file_based_datasource_kwargs,
    ):
        from pyarrow import json

        super().__init__(paths, **file_based_datasource_kwargs)

        if arrow_json_args is None:
            arrow_json_args = {}

        self.read_options = arrow_json_args.pop(
            "read_options", json.ReadOptions(use_threads=False)
        )
        self.arrow_json_args = arrow_json_args

    # TODO(ekl) The PyArrow JSON reader doesn't support streaming reads.
    def _read_file(self, f: "pyarrow.NativeFile", path: str):
        from pyarrow import json

        return json.read_json(f, read_options=self.read_options, **self.arrow_json_args)

    def _write_block(
        self,
        f: "pyarrow.NativeFile",
        block: BlockAccessor,
        writer_args_fn: Callable[[], Dict[str, Any]] = lambda: {},
        **writer_args,
    ):
        writer_args = _resolve_kwargs(writer_args_fn, **writer_args)
        orient = writer_args.pop("orient", "records")
        lines = writer_args.pop("lines", True)
        block.to_pandas().to_json(f, orient=orient, lines=lines, **writer_args)
