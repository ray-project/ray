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
        columns_rename: Optional[Dict[str, str]] = None,
        ray_remote_args: Dict[str, Any],
        concurrency: int,
    ):
        super().__init__(name="ReadFiles", input_dependencies=[input_dependency])

        self.reader = reader
        self.filesystem = filesystem
        self.filter_expr = filter_expr
        if columns is not None:
            if not isinstance(columns, list):
                raise TypeError("`columns` must be a list of strings.")
            if not columns:
                raise ValueError("`columns` cannot be an empty list.")
            if not all(isinstance(col, str) for col in columns):
                raise TypeError("All elements in `columns` must be strings.")
        if columns is not None and columns_rename is not None:
            assert set(columns_rename.keys()).issubset(columns), (
                f"All column rename keys must be a subset of the columns list. "
                f"Invalid keys: {set(columns_rename.keys()) - set(columns)}"
            )
        self.columns = columns
        self.columns_rename = columns_rename
        self.ray_remote_args = ray_remote_args
        self.concurrency = concurrency

    def is_read(self) -> bool:
        return True
