import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Union

from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalOperatorSupportsProjectionPushdown,
    LogicalPlan,
    Rule,
)
from ray.data._internal.logical.operators.map_operator import Project
from ray.data._internal.logical.operators.read_operator import Read
from ray.data.expressions import Expr

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _ProjectSpec:
    cols: Optional[List[str]]
    cols_remap: Optional[Dict[str, str]]
    exprs: Optional[Dict[str, Expr]]


class ProjectionPushdown(Rule):
    """Optimization rule that pushes down projections across the graph.

    This rule looks for `Project` operators that are immediately
    preceded by a `Read` operator and sets the
    projected columns on the `Read` operator.

    If there are redundant Project operators, it removes the `Project` operator from
    the graph.
    """

    def apply(self, plan: LogicalPlan) -> LogicalPlan:
        dag = plan.dag
        new_dag = dag._apply_transform(self._pushdown_project)

        return LogicalPlan(new_dag, plan.context) if dag is not new_dag else plan

    @classmethod
    def _pushdown_project(cls, op: LogicalOperator) -> LogicalOperator:
        if isinstance(op, Project):
            # Push-down projections into read op
            if cls._supports_projection_pushdown(op):
                project_op: Project = op
                target_op: LogicalOperatorSupportsProjectionPushdown = (
                    op.input_dependency
                )

                return cls._try_combine(target_op, project_op)

            # Otherwise, fuse projections into a single op
            elif isinstance(op.input_dependency, Project):
                outer_op: Project = op
                inner_op: Project = op.input_dependency

                return cls._fuse(inner_op, outer_op)

        return op

    @classmethod
    def _supports_projection_pushdown(cls, op: Project) -> bool:
        # NOTE: Currently only projecting into Parquet is supported
        input_op = op.input_dependency
        return (
            isinstance(input_op, LogicalOperatorSupportsProjectionPushdown)
            and input_op.supports_projection_pushdown()
        )

    @staticmethod
    def _fuse(inner_op: Project, outer_op: Project) -> Project:
        # Combine expressions from both operators
        combined_exprs = _combine_expressions(inner_op.exprs, outer_op.exprs)

        # Only combine projection specs if there are no expressions
        # When expressions are present, they take precedence
        if combined_exprs:
            # When expressions are present, preserve column operations from outer operation
            # The logical order is: expressions first, then column operations
            outer_cols = outer_op.cols
            outer_cols_rename = outer_op.cols_rename

            # If outer operation has no column operations, fall back to inner operation
            if outer_cols is None and outer_cols_rename is None:
                outer_cols = inner_op.cols
                outer_cols_rename = inner_op.cols_rename

            return Project(
                inner_op.input_dependency,
                cols=outer_cols,
                cols_rename=outer_cols_rename,
                exprs=combined_exprs,
                # Give precedence to outer operator's ray_remote_args
                ray_remote_args={
                    **inner_op._ray_remote_args,
                    **outer_op._ray_remote_args,
                },
            )
        else:
            # Fall back to original behavior for column-only projections
            inner_op_spec = _get_projection_spec(inner_op)
            outer_op_spec = _get_projection_spec(outer_op)

            new_spec = _combine_projection_specs(
                prev_spec=inner_op_spec, new_spec=outer_op_spec
            )

            return Project(
                inner_op.input_dependency,
                cols=new_spec.cols,
                cols_rename=new_spec.cols_remap,
                exprs=None,
                ray_remote_args={
                    **inner_op._ray_remote_args,
                    **outer_op._ray_remote_args,
                },
            )

    @staticmethod
    def _try_combine(
        target_op: LogicalOperatorSupportsProjectionPushdown,
        project_op: Project,
    ) -> LogicalOperator:
        # For now, don't push down expressions into `Read` operators
        # Only handle traditional column projections
        if project_op.exprs:
            # Cannot push expressions into `Read`, return unchanged
            return project_op

        target_op_spec = _get_projection_spec(target_op)
        project_op_spec = _get_projection_spec(project_op)

        new_spec = _combine_projection_specs(
            prev_spec=target_op_spec, new_spec=project_op_spec
        )

        logger.debug(
            f"Pushing projection down into read operation "
            f"projection columns = {new_spec.cols} (before: {target_op_spec.cols}), "
            f"remap = {new_spec.cols_remap} (before: {target_op_spec.cols_remap})"
        )

        return target_op.apply_projection(new_spec.cols)


def _combine_expressions(
    inner_exprs: Optional[Dict[str, Expr]], outer_exprs: Optional[Dict[str, Expr]]
) -> Optional[Dict[str, Expr]]:
    """Combine expressions from two Project operators.

    Args:
        inner_exprs: Expressions from the inner (upstream) Project operator
        outer_exprs: Expressions from the outer (downstream) Project operator

    Returns:
        Combined dictionary of expressions, or None if no expressions
    """
    if not inner_exprs and not outer_exprs:
        return None

    combined = {}

    # Add expressions from inner operator
    if inner_exprs:
        combined.update(inner_exprs)

    # Add expressions from outer operator
    if outer_exprs:
        combined.update(outer_exprs)

    return combined if combined else None


def _get_projection_spec(op: Union[Project, Read]) -> _ProjectSpec:
    assert op is not None

    if isinstance(op, Project):
        return _ProjectSpec(
            cols=op.cols,
            cols_remap=op.cols_rename,
            exprs=op.exprs,
        )
    elif isinstance(op, Read):
        assert op.supports_projection_pushdown()

        return _ProjectSpec(
            cols=op.get_current_projection(),
            cols_remap=None,
            exprs=None,
        )
    else:
        raise ValueError(
            f"Operation doesn't have projection spec (supported Project, "
            f"Read, got: {op.__class__})"
        )


def _combine_projection_specs(
    prev_spec: _ProjectSpec, new_spec: _ProjectSpec
) -> _ProjectSpec:
    combined_cols_remap = _combine_columns_remap(
        prev_spec.cols_remap,
        new_spec.cols_remap,
    )

    # Validate resulting remapping against existing projection (if any)
    _validate(combined_cols_remap, prev_spec.cols)

    new_projection_cols: Optional[List[str]]

    if prev_spec.cols is None and new_spec.cols is None:
        # If both projections are unset, resulting is unset
        new_projection_cols = None
    elif prev_spec.cols is not None and new_spec.cols is None:
        # If previous projection is set, but the new unset -- fallback to
        # existing projection
        new_projection_cols = prev_spec.cols
    else:
        # If new is set (and previous is either set or not)
        #   - Reconcile new projection
        #   - Project combined column remapping
        assert new_spec.cols is not None

        new_projection_cols = new_spec.cols

        # Remap new projected columns into the schema before remapping (from the
        # previous spec)
        if prev_spec.cols_remap and new_projection_cols:
            # Inverse remapping
            inv_cols_remap = {v: k for k, v in prev_spec.cols_remap.items()}
            new_projection_cols = [
                inv_cols_remap.get(col, col) for col in new_projection_cols
            ]

        prev_cols_set = set(prev_spec.cols or [])
        new_cols_set = set(new_projection_cols or [])

        # Validate new projection is a proper subset of the previous one
        if prev_cols_set and new_cols_set and not new_cols_set.issubset(prev_cols_set):
            raise ValueError(
                f"Selected columns '{new_cols_set}' needs to be a subset of "
                f"'{prev_cols_set}'"
            )

    # Project remaps to only map relevant columns
    if new_projection_cols is not None and combined_cols_remap is not None:
        projected_cols_remap = {
            k: v for k, v in combined_cols_remap.items() if k in new_projection_cols
        }
    else:
        projected_cols_remap = combined_cols_remap

    # Combine expressions from both specs
    combined_exprs = _combine_expressions(prev_spec.exprs, new_spec.exprs)

    return _ProjectSpec(
        cols=new_projection_cols, cols_remap=projected_cols_remap, exprs=combined_exprs
    )


def _combine_columns_remap(
    prev_remap: Optional[Dict[str, str]], new_remap: Optional[Dict[str, str]]
) -> Optional[Dict[str, str]]:

    if not new_remap and not prev_remap:
        return None

    new_remap = new_remap or {}
    base_remap = prev_remap or {}

    filtered_new_remap = dict(new_remap)
    # Apply new remapping to the base remap
    updated_base_remap = {
        # NOTE: We're removing corresponding chained mapping from the remap
        k: filtered_new_remap.pop(v, v)
        for k, v in base_remap.items()
    }

    resolved_remap = dict(updated_base_remap)
    resolved_remap.update(filtered_new_remap)

    return resolved_remap


def _validate(remap: Optional[Dict[str, str]], projection_cols: Optional[List[str]]):
    if not remap:
        return

    # Verify that the remapping is a proper bijection (ie no
    # columns are renamed into the same new name)
    prev_names_map = {}
    for prev_name, new_name in remap.items():
        if new_name in prev_names_map:
            raise ValueError(
                f"Identified projections with conflict in renaming: '{new_name}' "
                f"is mapped from multiple sources: '{prev_names_map[new_name]}' "
                f"and '{prev_name}'."
            )

        prev_names_map[new_name] = prev_name

    # Verify that remapping only references columns available in the projection
    if projection_cols is not None:
        invalid_cols = [key for key in remap.keys() if key not in projection_cols]

        if invalid_cols:
            raise ValueError(
                f"Identified projections with invalid rename "
                f"columns: {', '.join(invalid_cols)}"
            )
