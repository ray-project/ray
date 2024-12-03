from typing import TYPE_CHECKING, Any, Dict, List, Optional

from ray.anyscale.data._internal.readers import FileReader
from ray.data._internal.logical.interfaces import LogicalOperator

if TYPE_CHECKING:
    import pyarrow.dataset as pd


class ReadFiles(LogicalOperator):
    def __init__(
        self,
        input_dependency: LogicalOperator,
        *,
        reader: FileReader,
        filesystem,
        filter_expr: Optional["pd.Expression"] = None,
        columns: Optional[List[str]],
        ray_remote_args: Dict[str, Any],
        concurrency: int
    ):
        super().__init__(name="ReadFiles", input_dependencies=[input_dependency])

        self.reader = reader
        self.filesystem = filesystem
        self.filter_expr = filter_expr
        self.columns = columns
        self.ray_remote_args = ray_remote_args
        self.concurrency = concurrency

    def is_read(self) -> bool:
        return True
