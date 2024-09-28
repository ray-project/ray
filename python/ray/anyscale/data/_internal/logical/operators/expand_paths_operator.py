from typing import List, Union

from ray.anyscale.data._internal.readers import FileReader
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data.datasource import PathPartitionFilter


class ExpandPaths(LogicalOperator):
    def __init__(
        self,
        *,
        paths: Union[str, List[str]],
        reader: FileReader,
        filesystem,
        ignore_missing_paths: bool,
        file_extensions: List[str],
        partition_filter: PathPartitionFilter,
    ):
        super().__init__(name="ExpandPaths", input_dependencies=[])

        if isinstance(paths, str):
            paths = [paths]

        self.paths = paths
        self.reader = reader
        self.filesystem = filesystem
        self.ignore_missing_paths = ignore_missing_paths
        self.file_extensions = file_extensions
        self.partition_filter = partition_filter
