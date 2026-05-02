import posixpath
from typing import Any, Callable, Dict, Optional

import pyarrow

from ray._common.retry import call_with_retry
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.planner.plan_write_op import WRITE_UUID_KWARG_NAME
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

    def write_block(
        self, block: BlockAccessor, block_index: int, ctx: TaskContext
    ) -> None:
        # Convert the Arrow block to pandas ONCE, outside the retry loop.
        # ``ArrowBlockAccessor.to_pandas`` releases the underlying Arrow
        # buffers in place (``self_destruct=True``); a second invocation
        # on the same accessor — which would happen if we let the
        # destructive call run inside ``call_with_retry`` — would access
        # freed memory and segfault on transient I/O retries.
        df = block.to_pandas()

        writer_args = _resolve_kwargs(self.pandas_json_args_fn, **self.pandas_json_args)
        orient = writer_args.pop("orient", "records")
        lines = writer_args.pop("lines", True)

        filename = self.filename_provider.get_filename_for_task(
            ctx.kwargs[WRITE_UUID_KWARG_NAME], ctx.task_idx
        )
        write_path = posixpath.join(self.path, filename)

        def write_df_to_path():
            with self.open_output_stream(write_path) as file:
                df.to_json(file, orient=orient, lines=lines, **writer_args)

        call_with_retry(
            write_df_to_path,
            description=f"write '{write_path}'",
            match=self._data_context.retried_io_errors,
        )

    def write_block_to_file(self, block: BlockAccessor, file: "pyarrow.NativeFile"):
        # Retained for backward compatibility with subclasses that may
        # call into the legacy hook directly. ``write_block`` above is
        # the retry-safe entry point and bypasses this method.
        writer_args = _resolve_kwargs(self.pandas_json_args_fn, **self.pandas_json_args)
        orient = writer_args.pop("orient", "records")
        lines = writer_args.pop("lines", True)

        block.to_pandas().to_json(file, orient=orient, lines=lines, **writer_args)
