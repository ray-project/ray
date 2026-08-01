from typing import Any, Callable, Dict, Optional

import pyarrow

from ray.data.block import BlockAccessor
from ray.data.datasource.file_based_datasource import _resolve_kwargs
from ray.data.datasource.file_datasink import BlockBasedFileDatasink


class CSVDatasink(BlockBasedFileDatasink):
    def __init__(
        self,
        path: str,
        *,
        arrow_csv_args_fn: Optional[Callable[[], Dict[str, Any]]] = None,
        arrow_csv_args: Optional[Dict[str, Any]] = None,
        file_format="csv",
        **file_datasink_kwargs,
    ):
        super().__init__(path, file_format=file_format, **file_datasink_kwargs)

        if arrow_csv_args_fn is None:
            arrow_csv_args_fn = lambda: {}  # noqa: E731

        if arrow_csv_args is None:
            arrow_csv_args = {}

        self.arrow_csv_args_fn = arrow_csv_args_fn
        self.arrow_csv_args = arrow_csv_args

    def write_block_to_file(self, block: BlockAccessor, file: "pyarrow.NativeFile"):
        from pyarrow import csv

        writer_args = _resolve_kwargs(self.arrow_csv_args_fn, **self.arrow_csv_args)
        write_options = writer_args.pop("write_options", None)
        csv.write_csv(block.to_arrow(), file, write_options, **writer_args)
