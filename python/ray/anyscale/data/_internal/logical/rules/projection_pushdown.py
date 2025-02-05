from collections import deque
from typing import Iterable

from ray.anyscale.data._internal.logical.graph_utils import make_copy_of_dag, remove_op
from ray.anyscale.data._internal.logical.operators.read_files_operator import ReadFiles
from ray.data._internal.logical.interfaces import LogicalOperator, LogicalPlan, Rule
from ray.data._internal.logical.operators.map_operator import Project


class ProjectionPushdown(Rule):
    """Optimization rule that pushes down projections across the graph.

    This rule looks for `Project` operators that are immediately
    preceded by a `ReadFiles` operator and sets the
    projected columns on the `ReadFiles` operator.

    If there are redundant Project operators, it removes the `Project` operator from
    the graph.
    """

    def _handle_select_columns(self, prev_op: Project, cur_op: Project):
        # Step 1: Get prev columns as a set (to ensure no duplicates)
        prev_cols = set(prev_op.cols or [])

        # Step 2: Get current columns
        assert cur_op.cols and not cur_op.cols_rename
        cur_cols = set(cur_op.cols or [])

        # Step 3: Adjust prev_cols based on prev_op.cols_rename
        if prev_op.cols_rename:
            # Apply renames to previous columns
            prev_cols = {prev_op.cols_rename.get(col, col) for col in prev_cols}

        # Step 4: Ensure cur_cols is a subset of prev_cols
        if prev_cols and cur_cols and not cur_cols.issubset(prev_cols):
            raise ValueError(
                f"Selected columns '{cur_cols}' needs to be a subset of "
                f"'{prev_cols}'"
            )

        # Step 5: Adjust cur_cols based on prev_op.cols_rename to match previous
        # column names
        if prev_op.cols_rename:
            # Reverse the renaming process: if a column in cur_op.cols is renamed, map
            # it back.
            cur_cols = {
                next((k for k, v in prev_op.cols_rename.items() if v == col), col)
                for col in cur_cols
            }

        # Step 6: Prune prev_op.cols_rename to only include columns in cur_cols
        if prev_op.cols_rename:
            # Keep only those renames where the original column is in the selected
            # columns (cur_cols)
            prev_op._cols_rename = {
                k: v for k, v in prev_op.cols_rename.items() if k in cur_cols
            }

        # Step 7: Set final columns
        prev_op._cols = list(cur_cols if cur_cols else prev_cols)

    def _validate_rename_columns(self, prev_op: Project, cur_op: Project):
        prev_rename = prev_op.cols_rename
        cur_rename = cur_op.cols_rename

        # Validation Case 1: Both prev_op.cols and prev_op.cols_rename are valid
        if prev_op.cols and prev_op.cols_rename:
            # Get the final valid renamed column names
            renamed_cols = set(prev_rename.values())
            # The original columns from prev_op.cols
            untouched_cols = set(prev_op.cols)

            # Valid rename keys are a union of renamed columns and untouched columns
            valid_rename_keys = renamed_cols.union(untouched_cols)

            # Ensure that cur_rename keys are a subset of the valid rename keys
            invalid_keys = [key for key in cur_rename if key not in valid_rename_keys]
            if invalid_keys:
                raise ValueError(
                    f"Identified projections with invalid rename "
                    f"columns: {', '.join(invalid_keys)}"
                )

        # Validation Case 2: Only prev_op.cols is valid (no renames in prev_op)
        elif prev_op.cols:
            # Ensure cur_rename keys are a subset of prev_op.cols
            invalid_keys = [key for key in cur_rename if key not in prev_op.cols]
            if invalid_keys:
                raise ValueError(
                    f"Identified projections with invalid rename "
                    f"columns: {', '.join(invalid_keys)}"
                )

    def _handle_rename_columns(self, prev_op: Project, cur_op: Project):
        prev_rename = prev_op.cols_rename
        assert not cur_op.cols and cur_op.cols_rename
        cur_rename = cur_op.cols_rename

        self._validate_rename_columns(prev_op, cur_op)

        resolved_rename = {}

        # Step 1: Process prev renames and chain with cur renames
        prev_rename_copy = prev_rename.copy()
        for prev_old_col, prev_new_col in prev_rename_copy.items():
            # If the prev_new_col is in cur_rename, chain it
            if prev_new_col in cur_rename:
                final_col = cur_rename[prev_new_col]
                resolved_rename[prev_old_col] = final_col
                # Remove the resolved pairs from prev_rename and cur_rename
                del prev_rename[prev_old_col]
                del cur_rename[prev_new_col]
            else:
                # If no chaining is necessary, just copy the previous rename
                resolved_rename[prev_old_col] = prev_new_col
                del prev_rename[prev_old_col]  # Remove it from prev_rename

        # Step 2: Merge remaining cur renames into resolved_rename
        for cur_old_col, cur_new_col in cur_rename.items():
            if cur_old_col != cur_new_col:  # Only add if there's a real rename
                resolved_rename[cur_old_col] = cur_new_col

        # Step 3: Check for uniqueness
        inverse_mapping = {}
        for old_col, final_col in resolved_rename.items():
            if final_col in inverse_mapping:
                raise ValueError(
                    f"Identified projections with conflict in renaming: '{final_col}' "
                    f"is mapped from multiple sources: '{inverse_mapping[final_col]}' "
                    f"and '{old_col}'."
                )
            inverse_mapping[final_col] = old_col

        # Step 4: Apply the resolved renaming to the prev_op object
        prev_op._cols_rename = resolved_rename

    def apply(self, plan: LogicalPlan) -> LogicalPlan:
        dag_copy = make_copy_of_dag(plan.dag)
        plan = LogicalPlan(dag_copy, plan.context)
        plan = self._walk_output_dependencies(plan)
        plan = self._walk_input_dependencies(plan)
        return plan

    def _walk_output_dependencies(self, plan: LogicalPlan):
        """Walk plans output dependencies and merge all continguous projects."""
        projecting_op = None
        # Post-order traversal.
        nodes: Iterable[LogicalOperator] = deque()
        for node in plan.dag.post_order_iter():
            nodes.appendleft(node)

        while len(nodes) > 0:
            op = nodes.pop()
            if isinstance(op, Project):
                if not projecting_op:
                    projecting_op = op
                else:
                    # Handle column selections
                    if op.cols:
                        self._handle_select_columns(prev_op=projecting_op, cur_op=op)
                    # Handle column renames
                    if op.cols_rename:
                        self._handle_rename_columns(prev_op=projecting_op, cur_op=op)
                    plan = remove_op(op, plan)
            else:
                projecting_op = None

        return plan

    def _walk_input_dependencies(self, plan: LogicalPlan):
        """Walk plans input dependencies and pushdown down projects into ReadFiles."""
        projecting_op = None
        queue = deque([plan.dag])
        while queue:
            op = queue.popleft()
            if isinstance(op, Project):
                assert not projecting_op
                projecting_op = op
            elif isinstance(op, ReadFiles) and projecting_op:
                # Push down column selection/renames to ReadFiles
                readfiles = op
                readfiles.columns = projecting_op.cols
                readfiles.columns_rename = projecting_op.cols_rename
                plan = remove_op(projecting_op, plan)
                projecting_op = None
            else:
                projecting_op = None
            queue.extend(op.input_dependencies)

        return plan
