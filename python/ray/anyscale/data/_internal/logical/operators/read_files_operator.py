from typing import TYPE_CHECKING, Any, Dict, List, Optional

from ray.anyscale.data._internal.readers import FileReader
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.planner.plan_expression.expression_evaluator import (
    ExpressionEvaluator,
)

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
        concurrency: Optional[int],
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

    def pushdown_filter(self, filter_expr_strs: List[str]) -> None:
        filter_expr = self._create_filter_expr(filter_expr_strs)
        if self.filter_expr is not None:
            self.filter_expr &= filter_expr
        else:
            self.filter_expr = filter_expr

    def is_read(self) -> bool:
        return True

    def _create_filter_expr(self, filter_expr_strs: List[str]) -> "pd.Expression":
        # This is to handle a case where user specifies
        # read->rename(a->x)->filter("x>10")
        # When filter is pushed down to read, underlying schema wont know about column 'x' and fails.
        # So we need to reconstruct the filter expression with the original column names
        # Note: It is okay if there is a rename after filter pushdown as it doesnt break underlying read
        if not filter_expr_strs:
            return None
        field_changes = {}
        if self.columns_rename:
            for old_col, new_col in self.columns_rename.items():
                field_changes[new_col] = old_col
        filter_expr: "pd.Expression" = ExpressionEvaluator.get_filters(
            filter_expr_strs[0], field_changes=field_changes
        )
        for filter_expr_str in filter_expr_strs[1:]:
            filter_expr &= ExpressionEvaluator.get_filters(
                filter_expr_str, field_changes=field_changes
            )
        return filter_expr
