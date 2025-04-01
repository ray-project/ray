from typing import List, Optional, Union

from ray.data._internal.logical.interfaces import LogicalOperator


class Join(LogicalOperator):
    """Logical operator for join."""

    def __init__(
        self,
        left_input_op: LogicalOperator,
        right_input_op: LogicalOperator,
        left_on: Union[str, List[str]],
        right_on: Optional[Union[str, List[str]]] = None,
        how: str = "inner",
    ):
        """
        Args:
            left_input_op: The input operator at left hand side.
            right_input_op: The input operator at right hand side.
            left_on: The join key column name(s) for the left input.
            right_on: The join key column name(s) for the right input. If None,
                uses the same column name(s) as left_on.
            how: The type of join: 'inner', 'left_outer', 'right_outer', or 'full_outer'.
        """
        self.left_on = left_on
        self.right_on = right_on
        self.how = how
        super().__init__(
            f"Join[{how}]", 
            [left_input_op, right_input_op], 
            num_outputs=None
        )

    def estimated_num_outputs(self):
        # For joins, it's hard to predict the exact output size
        # For inner joins, it could be as small as 0 or as large as left * right
        # For outer joins, it depends on how many matches there are
        # A reasonable estimate might be the max of left and right
        left_num_outputs = self._input_dependencies[0].estimated_num_outputs()
        right_num_outputs = self._input_dependencies[1].estimated_num_outputs()
        
        if left_num_outputs is None or right_num_outputs is None:
            return None
        
        # For inner join, a rough estimate is the minimum of the two
        if self.how == "inner":
            return min(left_num_outputs, right_num_outputs)
        # For left_outer, all left rows are included
        elif self.how == "left_outer":
            return left_num_outputs
        # For right_outer, all right rows are included
        elif self.how == "right_outer":
            return right_num_outputs
        # For full_outer, a rough estimate is the sum of both sides minus duplicates
        elif self.how == "full_outer":
            # Just use the sum as an upper bound
            return left_num_outputs + right_num_outputs
        
        return None