import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from ray.data.context import DataContext
from ray.data.datasource.file_based_datasource import FileBasedDatasource
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    import pyarrow

logger = logging.getLogger(__name__)


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

    def _open_input_source(
        self,
        filesystem: "pyarrow.fs.FileSystem",
        path: str,
        **open_args,
    ) -> "pyarrow.NativeFile":
        # JSON requires `open_input_file` for seekable file access
        return filesystem.open_input_file(path, **open_args)


    # TODO(ekl) The PyArrow JSON reader doesn't support streaming reads.
    def _read_stream(self, f: "pyarrow.NativeFile", path: str):
        from pyarrow import ArrowInvalid, json

        # Create local copy of read_options so block_size increases are not persisted
        # between _read_stream calls.
        local_read_options = json.ReadOptions(
            use_threads=self.read_options.use_threads,
            block_size=self.read_options.block_size,
        )
        init_file_pos = f.tell()
        max_block_size = DataContext.get_current().target_max_block_size
        while True:
            try:
                yield json.read_json(
                    f, read_options=local_read_options, **self.arrow_json_args
                )
                break
            except ArrowInvalid as e:
                if (
                    isinstance(e, ArrowInvalid)
                    and "straddling" not in str(e)
                    or local_read_options.block_size > max_block_size
                ):
                    raise e
                else:
                    # Increase the block size in case it was too small.
                    logger.info(
                        f"JSONDatasource read failed with "
                        f"block_size={local_read_options.block_size}. Retrying with "
                        f"block_size={local_read_options.block_size * 2}."
                    )
                    local_read_options.block_size *= 2
                    # Reset file position to re-attempt read.
                    f.seek(init_file_pos)
