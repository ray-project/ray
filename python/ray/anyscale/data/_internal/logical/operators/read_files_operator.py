from typing import Any, Dict, List, Union

from ray.anyscale.data._internal.readers import FileReader
from ray.data._internal.logical.interfaces import LogicalOperator


class ReadFiles(LogicalOperator):
    def __init__(
        self,
        input_dependency: LogicalOperator,
        *,
        paths: Union[str, List[str]],
        reader: FileReader,
        filesystem,
        ray_remote_args: Dict[str, Any],
        concurrency: int
    ):
        super().__init__(name="ReadFiles", input_dependencies=[input_dependency])

        if isinstance(paths, str):
            paths = [paths]

        self.paths = paths
        self.reader = reader
        self.filesystem = filesystem
        self.ray_remote_args = ray_remote_args
        self.concurrency = concurrency

    def is_read(self) -> bool:
        return True
