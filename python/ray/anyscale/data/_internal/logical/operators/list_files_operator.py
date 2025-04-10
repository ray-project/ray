from typing import TYPE_CHECKING, Callable, List, Optional, Union

from ray.anyscale.data._internal.readers import FileReader
from ray.data import FileShuffleConfig
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data.datasource import PathPartitionFilter

if TYPE_CHECKING:
    from ray.anyscale.data._internal.planner.file_indexer import FileIndexer

PATH_COLUMN_NAME = "__path"
FILE_SIZE_COLUMN_NAME = "__file_size"


class ListFiles(LogicalOperator):
    """List files and get file sizes.

    If an input path is a directory, recursively list all files in the directory and
    their sizes. If an input path is a file, list the file and its size.

    Physical operators that implement this logical operator should output blocks with
    two columns: `PATH_COLUMN_NAME` and `FILE_SIZE_COLUMN_NAME`.
    """

    def __init__(
        self,
        *,
        paths: Union[str, List[str]],
        file_indexer: "FileIndexer",
        reader: FileReader,
        filesystem,
        file_extensions: List[str],
        partition_filter: PathPartitionFilter,
        shuffle_config_factory: Optional[Callable[[], Optional[FileShuffleConfig]]],
    ):
        assert filesystem is not None

        super().__init__(name="ListFiles", input_dependencies=[])

        if isinstance(paths, str):
            paths = [paths]

        self.paths = paths
        self.file_indexer = file_indexer
        self.reader = reader
        self.filesystem = filesystem
        self.file_extensions = file_extensions
        self.partition_filter = partition_filter
        self.shuffle_config_factory = shuffle_config_factory
