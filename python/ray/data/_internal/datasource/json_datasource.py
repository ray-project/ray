import logging
from io import BytesIO
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

import pandas as pd

from ray.air.util.tensor_extensions.arrow import pyarrow_table_from_pydict
from ray.data._internal.pandas_block import PandasBlockAccessor
from ray.data.context import DataContext
from ray.data.datasource.file_based_datasource import FileBasedDatasource

if TYPE_CHECKING:
    import pyarrow

logger = logging.getLogger(__name__)

JSON_FILE_EXTENSIONS = [
    "json",
    "jsonl",
    # gzip-compressed files
    "json.gz",
    "jsonl.gz",
    # Brotli-compressed fi;es
    "json.br",
    "jsonl.br",
    # Zstandard-compressed files
    "json.zst",
    "jsonl.zst",
    # lz4-compressed files
    "json.lz4",
    "jsonl.lz4",
]


class ArrowJSONDatasource(FileBasedDatasource):
    """JSON datasource, for reading and writing JSON and JSONL files."""

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

    def _read_with_pyarrow_read_json(self, buffer: "pyarrow.lib.Buffer"):
        """Read with PyArrow JSON reader, trying to auto-increase the
        read block size in the case of the read object
        straddling block boundaries."""
        import pyarrow as pa
        import pyarrow.json as pajson

        # When reading large files, the default block size configured in PyArrow can be
        # too small, resulting in the following error: `pyarrow.lib.ArrowInvalid:
        # straddling object straddles two block boundaries (try to increase block
        # size?)`. More information on this issue can be found here:
        # https://github.com/apache/arrow/issues/25674
        # The read will be retried with geometrically increasing block size
        # until the size reaches `DataContext.get_current().target_max_block_size`.
        # The initial block size will start at the PyArrow default block size
        # or it can be manually set through the `read_options` parameter as follows.
        # >>> import pyarrow.json as pajson
        # >>> block_size = 10 << 20 # Set block size to 10MB
        # >>> ray.data.read_json(  # doctest: +SKIP
        # ...     "s3://anonymous@ray-example-data/log.json",
        # ...     read_options=pajson.ReadOptions(block_size=block_size)
        # ... )

        init_block_size = self.read_options.block_size
        max_block_size = DataContext.get_current().target_max_block_size
        while True:
            try:
                yield pajson.read_json(
                    BytesIO(buffer),
                    read_options=self.read_options,
                    **self.arrow_json_args,
                )
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

    def _read_with_python_json(self, buffer: "pyarrow.lib.Buffer"):
        """Fallback method to read JSON files with Python's native json.load(),
        in case the default pyarrow json reader fails."""
        import json

        import pyarrow as pa

        # Check if the buffer is empty
        if buffer.size == 0:
            return

        parsed_json = json.load(BytesIO(buffer))
        try:
            yield pa.Table.from_pylist(parsed_json)
        except AttributeError as e:
            # For PyArrow < 7.0.0, `pa.Table.from_pylist()` is not available.
            # Construct a dict from the list and call
            # `pa.Table.from_pydict()` instead.
            assert "no attribute 'from_pylist'" in str(e), str(e)
            from collections import defaultdict

            dct = defaultdict(list)
            for row in parsed_json:
                for k, v in row.items():
                    dct[k].append(v)
            yield pyarrow_table_from_pydict(dct)

    # TODO(ekl) The PyArrow JSON reader doesn't support streaming reads.
    def _read_stream(self, f: "pyarrow.NativeFile", path: str):
        import pyarrow as pa

        buffer: pa.lib.Buffer = f.read_buffer()

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


class PandasJSONDatasource(FileBasedDatasource):
    def __init__(
        self,
        paths: Union[str, List[str]],
        target_output_size_bytes: int,
        **file_based_datasource_kwargs,
    ):
        super().__init__(paths, **file_based_datasource_kwargs)

        self._target_output_size_bytes = target_output_size_bytes

    def _read_stream(self, f: "pyarrow.NativeFile", path: str):
        chunksize = self._estimate_chunksize(f)
        with pd.read_json(f, chunksize=chunksize, lines=True) as reader:
            for df in reader:
                yield _cast_range_index_to_string(df)

    def _estimate_chunksize(self, f: "pyarrow.NativeFile") -> int:
        assert f.tell() == 0, "File pointer must be at the beginning"

        with pd.read_json(f, chunksize=1, lines=True) as reader:
            df = _cast_range_index_to_string(next(reader))

        block_accessor = PandasBlockAccessor.for_block(df)
        if block_accessor.num_rows() == 0:
            return 1

        bytes_per_row = block_accessor.size_bytes() / block_accessor.num_rows()
        chunksize = max(round(self._target_output_size_bytes / bytes_per_row), 1)

        # Reset file pointer to the beginning.
        f.seek(0)

        return chunksize

    def _open_input_source(
        self,
        filesystem: "pyarrow.fs.FileSystem",
        path: str,
        **open_args,
    ) -> "pyarrow.NativeFile":
        # Use seekable file to ensure we can correctly sample the first row.
        file = filesystem.open_input_file(path, **open_args)
        assert file.seekable(), "File must be seekable"
        return file


def _cast_range_index_to_string(df: pd.DataFrame):
    # NOTE: PandasBlockAccessor doesn't support RangeIndex, so we need to convert
    # to string.
    if isinstance(df.columns, pd.RangeIndex):
        df.columns = df.columns.astype(str)
    return df
