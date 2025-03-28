import logging
from dataclasses import dataclass
from typing import List, Dict, Optional, Union

from ray.anyscale.data._internal.logical.graph_utils import make_copy_of_dag
from ray.anyscale.data._internal.logical.operators.read_files_operator import ReadFiles
from ray.data._internal.logical.interfaces import LogicalOperator, LogicalPlan, Rule
from ray.data._internal.logical.operators.map_operator import Project


logger = logging.getLogger(__file__)


@dataclass(frozen=True)
class _ProjectSpec:
    cols: Optional[List[str]]
    cols_remap: Optional[Dict[str, str]]


class ProjectionPushdown(Rule):
    """Optimization rule that pushes down projections across the graph.

    This rule looks for `Project` operators that are immediately
    preceded by a `ReadFiles` operator and sets the
    projected columns on the `ReadFiles` operator.

    If there are redundant Project operators, it removes the `Project` operator from
    the graph.
    """

    def apply(self, plan: LogicalPlan) -> LogicalPlan:
        dag_copy = make_copy_of_dag(plan.dag)
        plan = LogicalPlan(dag_copy, plan.context)
        plan = self._transform_plan(plan)
        return plan

    def _transform_plan(self, plan: LogicalPlan) -> LogicalPlan:
        dag = plan.dag
        new_dag = dag._apply_transform(self._pushdown_project)

        return LogicalPlan(new_dag, plan.context) if dag is not new_dag else plan

    @classmethod
    def _pushdown_project(cls, op: LogicalOperator) -> LogicalOperator:
        if isinstance(op, Project):
            # Push-down projections into read op
            if isinstance(op.input_dependency, ReadFiles):
                project_op: Project = op
                read_op: ReadFiles = op.input_dependency

                return cls._combine(read_op, project_op)

            # Otherwise, fuse projections into a single op
            elif isinstance(op.input_dependency, Project):
                outer_op: Project = op
                inner_op: Project = op.input_dependency

                return cls._fuse(inner_op, outer_op)

        return op

    @staticmethod
    def _fuse(inner_op: Project, outer_op: Project) -> Project:
        inner_op_spec = _get_projection_spec(inner_op)
        outer_op_spec = _get_projection_spec(outer_op)

        new_spec = _combine_projection_specs(
            prev_spec=inner_op_spec, new_spec=outer_op_spec
        )

        return Project(
            inner_op.input_dependency,
            cols=new_spec.cols,
            cols_rename=new_spec.cols_remap,
            ray_remote_args={
                **inner_op._ray_remote_args,
                **outer_op._ray_remote_args,
            },
        )

    @staticmethod
    def _combine(read_op: ReadFiles, project_op: Project) -> ReadFiles:
        read_op_spec = _get_projection_spec(read_op)
        project_op_spec = _get_projection_spec(project_op)

        new_spec = _combine_projection_specs(
            prev_spec=read_op_spec, new_spec=project_op_spec
        )

        logger.debug(
            f"Pushing projection down into read operation "
            f"(projection columns = {new_spec.cols}, remap = {new_spec.cols_remap})"
        )

        # TODO(DATA-843) avoid modifying in-place
        read_op.columns = new_spec.cols
        read_op.columns_rename = new_spec.cols_remap

        return read_op


def _get_projection_spec(op: Union[Project, ReadFiles]) -> _ProjectSpec:
    assert op is not None

    if isinstance(op, Project):
        return _ProjectSpec(
            cols=op.cols,
            cols_remap=op.cols_rename,
        )
    elif isinstance(op, ReadFiles):
        return _ProjectSpec(
            cols=op.columns,
            cols_remap=op.columns_rename,
        )
    else:
        raise ValueError(
            f"Operation doesn't have projection spec (supported Project, "
            f"ReadFiles, got: {op.__class__})"
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

    return _ProjectSpec(cols=new_projection_cols, cols_remap=projected_cols_remap)


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
