import functools
import warnings
from typing import Callable, Dict, List, Optional, Tuple, Type, TypeVar

from ray import ObjectRef
from ray.data._internal.execution.execution_callback import add_execution_callback
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.operators.aggregate_num_rows import (
    AggregateNumRows,
)
from ray.data._internal.execution.operators.input_data_buffer import (
    InputDataBuffer,
)
from ray.data._internal.execution.operators.join import JoinOperator
from ray.data._internal.execution.operators.limit_operator import LimitOperator
from ray.data._internal.execution.operators.output_splitter import OutputSplitter
from ray.data._internal.execution.operators.union_operator import UnionOperator
from ray.data._internal.execution.operators.zip_operator import ZipOperator
from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalPlan,
    PhysicalPlan,
)
from ray.data._internal.logical.operators import (
    AbstractAllToAll,
    AbstractFrom,
    AbstractUDFMap,
    Count,
    Download,
    Filter,
    InputData,
    Join,
    Limit,
    Project,
    Read,
    StreamingRepartition,
    StreamingSplit,
    Union,
    Write,
    Zip,
)
from ray.data._internal.planner.checkpoint import (
    plan_read_op_with_checkpoint_filter,
    plan_write_op_with_checkpoint_writer,
)
from ray.data._internal.planner.plan_all_to_all_op import plan_all_to_all_op
from ray.data._internal.planner.plan_download_op import plan_download_op
from ray.data._internal.planner.plan_read_op import plan_read_op
from ray.data._internal.planner.plan_udf_map_op import (
    plan_filter_op,
    plan_project_op,
    plan_streaming_repartition_op,
    plan_udf_map_op,
)
from ray.data._internal.planner.plan_write_op import plan_write_op
from ray.data.checkpoint.load_checkpoint_callback import LoadCheckpointCallback
from ray.data.context import DataContext

LogicalOperatorType = TypeVar("LogicalOperatorType", bound=LogicalOperator)
PlanLogicalOpFn = Callable[
    [LogicalOperatorType, List[PhysicalOperator], DataContext], PhysicalOperator
]


def plan_input_data_op(
    logical_op: InputData,
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
) -> PhysicalOperator:
    """Get the corresponding DAG of physical operators for InputData."""
    assert len(physical_children) == 0

    return InputDataBuffer(
        data_context,
        input_data=logical_op.input_data,
    )


def plan_from_op(
    op: AbstractFrom,
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
) -> PhysicalOperator:
    assert len(physical_children) == 0
    return InputDataBuffer(data_context, op.input_data)


def plan_zip_op(_, physical_children, data_context):
    assert len(physical_children) >= 2
    return ZipOperator(data_context, *physical_children)


def plan_union_op(_, physical_children, data_context):
    assert len(physical_children) >= 2
    return UnionOperator(data_context, *physical_children)


def plan_limit_op(logical_op, physical_children, data_context):
    assert len(physical_children) == 1
    return LimitOperator(logical_op.limit, physical_children[0], data_context)


def plan_count_op(logical_op, physical_children, data_context):
    assert len(physical_children) == 1
    return AggregateNumRows(
        [physical_children[0]], data_context, column_name=Count.COLUMN_NAME
    )


def plan_join_op(
    logical_op: Join,
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
) -> PhysicalOperator:
    assert len(physical_children) == 2
    return JoinOperator(
        data_context=data_context,
        left_input_op=physical_children[0],
        right_input_op=physical_children[1],
        join_type=logical_op.join_type,
        left_key_columns=logical_op.left_key_columns,
        right_key_columns=logical_op.right_key_columns,
        left_columns_suffix=logical_op.left_columns_suffix,
        right_columns_suffix=logical_op.right_columns_suffix,
        num_partitions=logical_op.num_outputs,
        partition_size_hint=logical_op.partition_size_hint,
        aggregator_ray_remote_args_override=logical_op.aggregator_ray_remote_args,
    )


def plan_streaming_split_op(
    logical_op: StreamingSplit,
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
):
    assert len(physical_children) == 1
    return OutputSplitter(
        physical_children[0],
        n=logical_op.num_splits,
        equal=logical_op.equal,
        data_context=data_context,
        locality_hints=logical_op.locality_hints,
    )


class Planner:
    """The planner to convert optimized logical to physical operators.

    Note that planner is only doing operators conversion. Physical optimization work is
    done by physical optimizer.
    """

    _DEFAULT_PLAN_FNS = {
        Read: plan_read_op,
        InputData: plan_input_data_op,
        Write: plan_write_op,
        AbstractFrom: plan_from_op,
        Filter: plan_filter_op,
        AbstractUDFMap: plan_udf_map_op,
        AbstractAllToAll: plan_all_to_all_op,
        Union: plan_union_op,
        Zip: plan_zip_op,
        Limit: plan_limit_op,
        Count: plan_count_op,
        Project: plan_project_op,
        StreamingRepartition: plan_streaming_repartition_op,
        Join: plan_join_op,
        StreamingSplit: plan_streaming_split_op,
        Download: plan_download_op,
    }
    # Operators that support checkpoint filtering. Subclasses can override.
    _CHECKPOINT_FILTER_OPS = (Read,)

    def __init__(self):
        self._supports_checkpointing = False
        self._plan_fns_for_checkpointing = {}

    def plan(self, logical_plan: LogicalPlan) -> PhysicalPlan:
        """Convert logical to physical operators recursively in post-order."""
        checkpoint_config = logical_plan.context.checkpoint_config
        if checkpoint_config is not None and self._check_supports_checkpointing(
            logical_plan
        ):
            self._supports_checkpointing = True

            checkpoint_callback = self._create_checkpoint_callback(checkpoint_config)
            add_execution_callback(checkpoint_callback, logical_plan.context)
            load_checkpoint = checkpoint_callback.load_checkpoint

            # Dynamically set the plan functions for checkpointing because they
            # need to a reference to the checkpoint ref.
            self._plan_fns_for_checkpointing = self._get_plan_fns_for_checkpointing(
                load_checkpoint
            )

        elif checkpoint_config is not None:
            assert not self._check_supports_checkpointing(logical_plan)
            warnings.warn(
                "You've enabled checkpointing, but the logical plan doesn't support "
                "checkpointing. Checkpointing will be disabled."
            )
        physical_dag, op_map = self._plan_recursively(
            logical_plan.dag, logical_plan.context
        )
        physical_plan = PhysicalPlan(physical_dag, op_map, logical_plan.context)
        return physical_plan

    def get_plan_fn(self, logical_op: LogicalOperator) -> PlanLogicalOpFn:
        if self._supports_checkpointing:
            assert self._plan_fns_for_checkpointing
            plan_fn = find_plan_fn(logical_op, self._plan_fns_for_checkpointing)
            if plan_fn is not None:
                return plan_fn

        plan_fn = find_plan_fn(logical_op, self._DEFAULT_PLAN_FNS)
        if plan_fn is not None:
            return plan_fn

        raise ValueError(
            f"Found unknown logical operator during planning: {logical_op}"
        )

    def _plan_recursively(
        self, logical_op: LogicalOperator, data_context: DataContext
    ) -> Tuple[PhysicalOperator, Dict[LogicalOperator, PhysicalOperator]]:
        """Plan a logical operator and its input dependencies recursively.

        Args:
            logical_op: The logical operator to plan.
            data_context: The data context.

        Returns:
            A tuple of the physical operator corresponding to the logical operator, and
            a mapping from physical to logical operators.
        """
        op_map: Dict[PhysicalOperator, LogicalOperator] = {}

        # Plan the input dependencies first.
        physical_children = []
        for child in logical_op.input_dependencies:
            physical_child, child_op_map = self._plan_recursively(child, data_context)
            physical_children.append(physical_child)
            op_map.update(child_op_map)

        plan_fn = self.get_plan_fn(logical_op)
        # We will call `set_logical_operators()` in the following for-loop,
        # no need to do it here.
        physical_op = plan_fn(logical_op, physical_children, data_context)

        # Traverse up the DAG, and set the mapping from physical to logical operators.
        # At this point, all physical operators without logical operators set
        # must have been created by the current logical operator.
        queue = [physical_op]
        while queue:
            curr_physical_op = queue.pop()
            # Once we find an operator with a logical operator set, we can stop.
            if curr_physical_op._logical_operators:
                break

            curr_physical_op.set_logical_operators(logical_op)
            # Add this operator to the op_map so optimizer can find it
            op_map[curr_physical_op] = logical_op
            queue.extend(curr_physical_op.input_dependencies)

        # Also add the final operator (in case the loop didn't catch it)
        op_map[physical_op] = logical_op
        return physical_op, op_map

    def _create_checkpoint_callback(self, checkpoint_config) -> LoadCheckpointCallback:
        """Factory method to create the LoadCheckpointCallback.

        Subclasses can override this to use a different callback implementation.
        """
        return LoadCheckpointCallback(checkpoint_config)

    def _get_plan_fns_for_checkpointing(
        self,
        load_checkpoint: Callable[[], ObjectRef],
    ) -> Dict[Type[LogicalOperator], PlanLogicalOpFn]:
        plan_fns = {
            Read: functools.partial(
                plan_read_op_with_checkpoint_filter,
                load_checkpoint=load_checkpoint,
            ),
            Write: plan_write_op_with_checkpoint_writer,
        }
        return plan_fns

    def _check_supports_checkpointing(self, logical_plan: LogicalPlan) -> bool:
        """Check if the logical plan supports checkpointing.

        Subclasses can override _CHECKPOINT_FILTER_OPS to support more operators.
        """
        if not isinstance(logical_plan.dag, (Write, StreamingSplit)):
            return False

        def _all_paths_contain_checkpoint_filter(op: LogicalOperator) -> bool:
            if isinstance(op, self._CHECKPOINT_FILTER_OPS):
                return True
            return all(
                _all_paths_contain_checkpoint_filter(input_dep)
                for input_dep in op.input_dependencies
            )

        return _all_paths_contain_checkpoint_filter(logical_plan.dag)


def find_plan_fn(
    logical_op: LogicalOperator, plan_fns: Dict[Type[LogicalOperator], PlanLogicalOpFn]
) -> Optional[PlanLogicalOpFn]:
    """Find the plan function for a logical operator.

    This function goes through the plan functions in order and returns the first one
    that is an instance of the logical operator type.

    Args:
        logical_op: The logical operator to find the plan function for.
        plan_fns: The dictionary of plan functions.

    Returns:
        The plan function for the logical operator, or None if no plan function is
        found.
    """
    # TODO: This implementation doesn't account for type hierarchies conflicts or
    # multiple inheritance.
    for op_type, plan_fn in plan_fns.items():
        if isinstance(logical_op, op_type):
            return plan_fn
    return None
