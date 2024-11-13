from typing import List, Union

from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data.datasource import PathPartitionFilter

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
        filesystem,
        ignore_missing_paths: bool,
        file_extensions: List[str],
        partition_filter: PathPartitionFilter,
    ):
        super().__init__(name="ListFiles", input_dependencies=[])

        if isinstance(paths, str):
            paths = [paths]

        self.paths = paths
        self.filesystem = filesystem
        self.ignore_missing_paths = ignore_missing_paths
        self.file_extensions = file_extensions
        self.partition_filter = partition_filter
