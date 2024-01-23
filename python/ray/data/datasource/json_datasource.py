from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from ray.data._internal.dataset_logger import DatasetLogger
from ray.data.context import DataContext
from ray.data.datasource.file_based_datasource import FileBasedDatasource
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    import pyarrow

logger = DatasetLogger(__name__)


@PublicAPI
class JSONDatasource(FileBasedDatasource):
    """JSON datasource, for reading and writing JSON and JSONL files."""

    _FILE_EXTENSIONS = ["json", "jsonl"]

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
    def _read_stream(self, f: "pyarrow.NativeFile", path: str):
        from io import BytesIO

        from pyarrow import ArrowInvalid, json

        # When reading large files, the default block size configured in PyArrow can be
        # too small, resulting in the following error: `pyarrow.lib.ArrowInvalid:
        # straddling object straddles two block boundaries (try to increase block
        # size?)`. More information on this issue can be found here:
        # https://github.com/apache/arrow/issues/25674 The read will be retried with
        # geometrically increasing block size until the size reaches
        # `DataContext.get_current().target_max_block_size`. The initial block size
        # will start at the PyArrow default block size or it can be manually set
        # through the `read_options` parameter as follows.
        #
        # >>> import pyarrow.json as pajson
        # >>> block_size = 10 << 20 # Set block size to 10MB
        # >>> ray.data.read_json(  # doctest: +SKIP
        # ...     "s3://anonymous@ray-example-data/log.json",
        # ...     read_options=pajson.ReadOptions(block_size=block_size)
        # ... )

        buffer = f.read_buffer()
        init_block_size = self.read_options.block_size
        max_block_size = DataContext.get_current().target_max_block_size
        while True:
            try:
                yield json.read_json(
                    BytesIO(buffer),
                    read_options=self.read_options,
                    **self.arrow_json_args,
                )
                self.read_options.block_size = init_block_size
                break
            except ArrowInvalid as e:
                if "straddling object straddles two block boundaries" in str(e):
                    if self.read_options.block_size < max_block_size:
                        # Increase the block size in case it was too small.
                        logger.get_logger(log_to_stdout=False).info(
                            f"JSONDatasource read failed with "
                            f"block_size={self.read_options.block_size}. Retrying with "
                            f"block_size={self.read_options.block_size * 2}."
                        )
                        self.read_options.block_size *= 2
                    else:
                        raise ArrowInvalid(
                            f"{e} - Auto-increasing block size to "
                            f"{self.read_options.block_size} bytes failed. "
                            f"More information on this issue can be found here: "
                            f"https://github.com/apache/arrow/issues/25674"
                        )
                else:
                    # unrelated error, simply reraise
                    raise e
