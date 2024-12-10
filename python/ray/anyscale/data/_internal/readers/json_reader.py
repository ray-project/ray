import json
import logging
from collections import defaultdict
from io import BytesIO
from typing import TYPE_CHECKING, Any, Dict, Iterable, Optional

from .native_file_reader import NativeFileReader
from ray.data.block import DataBatch

if TYPE_CHECKING:
    import pyarrow

logger = logging.getLogger(__name__)


class JSONReader(NativeFileReader):
    def __init__(
        self, arrow_json_args: Optional[Dict[str, Any]] = None, **file_reader_kwargs
    ):
        super().__init__(**file_reader_kwargs)

        from pyarrow import json

        if arrow_json_args is None:
            arrow_json_args = {}

        self.read_options = arrow_json_args.pop(
            "read_options", json.ReadOptions(use_threads=False)
        )
        self.arrow_json_args = arrow_json_args

    # TODO(ekl) The PyArrow JSON reader doesn't support streaming reads.
    def read_stream(self, f: "pyarrow.NativeFile", path: str) -> Iterable[DataBatch]:
        import pyarrow as pa

        buffer: pa.lib.Buffer = f.read_buffer()

        # Check if the buffer is empty
        if buffer.size == 0:
            return

        try:
            yield from self._read_with_pyarrow_read_json(buffer)
        except pa.ArrowInvalid as e:
            # If read with PyArrow fails, try falling back to native json.load().
            logger.warning(
                f"Error reading with pyarrow.json.read_json(). "
                f"Falling back to native json.load(), which may be slower. "
                f"PyArrow error was:\n{e}"
            )
            yield from self._read_with_python_json(buffer)

    def _read_with_pyarrow_read_json(
        self, buffer: "pyarrow.lib.Buffer"
    ) -> Iterable[DataBatch]:
        """Read with PyArrow JSON reader, trying to auto-increase the
        read block size in the case of the read object
        straddling block boundaries."""
        import pyarrow as pa

        # When reading large files, the default block size configured in PyArrow can be
        # too small, resulting in the following error: `pyarrow.lib.ArrowInvalid:
        # straddling object straddles two block boundaries (try to increase block
        # size?)`. More information on this issue can be found here:
        # https://github.com/apache/arrow/issues/25674
        # The read will be retried with geometrically increasing block size
        # until the size reaches `DataContext.target_max_block_size`.
        # The initial block size will start at the PyArrow default block size
        # or it can be manually set through the `read_options` parameter as follows.
        # >>> import pyarrow.json as pajson
        # >>> block_size = 10 << 20 # Set block size to 10MB
        # >>> ray.data.read_json(  # doctest: +SKIP
        # ...     "s3://anonymous@ray-example-data/log.json",
        # ...     read_options=pajson.ReadOptions(block_size=block_size)
        # ... )

        init_block_size = self.read_options.block_size
        max_block_size = self.data_context.target_max_block_size
        while True:
            try:
                table = pa.json.read_json(
                    BytesIO(buffer),
                    read_options=self.read_options,
                    **self.arrow_json_args,
                )
                yield table
                self.read_options.block_size = init_block_size
                break
            except pa.ArrowInvalid as e:
                if "straddling object straddles two block boundaries" in str(e):
                    if self.read_options.block_size < max_block_size:
                        # Increase the block size in case it was too small.
                        logger.debug(
                            f"JSONDatasource read failed with "
                            f"block_size={self.read_options.block_size}. Retrying with "
                            f"block_size={self.read_options.block_size * 2}."
                        )
                        self.read_options.block_size *= 2
                    else:
                        raise pa.ArrowInvalid(
                            f"{e} - Auto-increasing block size to "
                            f"{self.read_options.block_size} bytes failed. "
                            f"Please try manually increasing the block size through "
                            f"the `read_options` parameter to a larger size. "
                            f"For example: `read_json(..., read_options="
                            f"pyarrow.json.ReadOptions(block_size=10 << 25))`"
                            f"More information on this issue can be found here: "
                            f"https://github.com/apache/arrow/issues/25674"
                        )
                else:
                    # unrelated error, simply reraise
                    raise e

    def _read_with_python_json(
        self, buffer: "pyarrow.lib.Buffer"
    ) -> Iterable[DataBatch]:
        """Fallback method to read JSON files with Python's native json.load(),
        in case the default pyarrow json reader fails."""
        parsed_json = json.load(BytesIO(buffer))
        batch = defaultdict(list)
        for row in parsed_json:
            for k, v in row.items():
                batch[k].append(v)
        yield batch
