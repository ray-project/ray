from typing import Any, Callable, Dict, Optional

import pyarrow

from ray.data._internal.arrow_block import _is_user_visible_column
from ray.data.block import BlockAccessor
from ray.data.datasource.file_based_datasource import _resolve_kwargs
from ray.data.datasource.file_datasink import BlockBasedFileDatasink


class ORCDatasink(BlockBasedFileDatasink):
    def __init__(
        self,
        path: str,
        *,
        arrow_orc_args_fn: Optional[Callable[[], Dict[str, Any]]] = None,
        arrow_orc_args: Optional[Dict[str, Any]] = None,
        file_format: str = "orc",
        **file_datasink_kwargs,
    ):
        open_stream_args = file_datasink_kwargs.get("open_stream_args")
        if open_stream_args is not None and "compression" in open_stream_args:
            raise ValueError(
                "Stream compression isn't supported for ORC files. Pass an ORC "
                "codec with write_orc(..., compression=...)."
            )

        super().__init__(path, file_format=file_format, **file_datasink_kwargs)

        if arrow_orc_args_fn is None:
            arrow_orc_args_fn = lambda: {}  # noqa: E731

        if arrow_orc_args is None:
            arrow_orc_args = {}

        self.arrow_orc_args_fn = arrow_orc_args_fn
        self.arrow_orc_args = arrow_orc_args

    # ``BlockBasedFileDatasink`` lacks an explicit return annotation, so Pyrefly
    # infers ``Never`` from its ``raise NotImplementedError`` implementation.
    # pyrefly: ignore[bad-override]
    def write_block_to_file(self, block: BlockAccessor, file: "pyarrow.NativeFile"):
        from pyarrow import orc

        table = block.to_arrow()
        user_columns = [
            name for name in table.schema.names if _is_user_visible_column(name)
        ]
        if not user_columns:
            raise ValueError("write_orc requires at least one column.")
        table = table.select(user_columns)

        writer_args = _resolve_kwargs(self.arrow_orc_args_fn, **self.arrow_orc_args)
        orc.write_table(table, file, **writer_args)
