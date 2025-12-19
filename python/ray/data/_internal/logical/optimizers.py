from typing import Callable, List

from .ruleset import Ruleset
from ray.data._internal.logical.interfaces import (
    LogicalPlan,
    Optimizer,
    PhysicalPlan,
    Plan,
    Rule,
)
from ray.data._internal.logical.rules.combine_repartitions import CombineRepartitions
from ray.data._internal.logical.rules.configure_map_task_memory import (
    ConfigureMapTaskMemoryUsingOutputSize,
)
from ray.data._internal.logical.rules.inherit_batch_format import InheritBatchFormatRule
from ray.data._internal.logical.rules.inherit_target_max_block_size import (
    InheritTargetMaxBlockSizeRule,
)
from ray.data._internal.logical.rules.limit_pushdown import LimitPushdownRule
from ray.data._internal.logical.rules.operator_fusion import FuseOperators
from ray.data._internal.logical.rules.predicate_pushdown import PredicatePushdown
from ray.data._internal.logical.rules.projection_pushdown import ProjectionPushdown
from ray.data._internal.logical.rules.set_read_parallelism import SetReadParallelismRule
from ray.util.annotations import DeveloperAPI

_LOGICAL_RULESET = Ruleset(
    [
        InheritBatchFormatRule,
        LimitPushdownRule,
        ProjectionPushdown,
        PredicatePushdown,
        CombineRepartitions,
    ]
)


_PHYSICAL_RULESET = Ruleset(
    [
        InheritTargetMaxBlockSizeRule,
        SetReadParallelismRule,
        FuseOperators,
        ConfigureMapTaskMemoryUsingOutputSize,
    ]
)


@DeveloperAPI
def get_logical_ruleset() -> Ruleset:
    return _LOGICAL_RULESET


@DeveloperAPI
def get_physical_ruleset() -> Ruleset:
    return _PHYSICAL_RULESET


class LogicalOptimizer(Optimizer):
    """The optimizer for logical operators."""

    @property
    def rules(self) -> List[Rule]:
        return [rule_cls() for rule_cls in get_logical_ruleset()]


class PhysicalOptimizer(Optimizer):
    """The optimizer for physical operators."""

    @property
    def rules(self) -> List[Rule]:
        return [rule_cls() for rule_cls in get_physical_ruleset()]


def get_plan_conversion_fns() -> List[Callable[[Plan], Plan]]:
    """Get the list of transformation functions to convert a logical plan
    to an optimized physical plan.

    This returns the 3 transformation steps:
    1. Logical optimization
    2. Planning (logical -> physical operators)
    3. Physical optimization

    Returns:
        A list of transformation functions, each taking a Plan and returning a Plan.
    """
    from ray.data._internal.planner import create_planner

    return [
        LogicalOptimizer().optimize,  # Logical optimization
        create_planner().plan,  # Planning
        PhysicalOptimizer().optimize,  # Physical optimization
    ]


def get_execution_plan(logical_plan: LogicalPlan) -> PhysicalPlan:
    """Get the physical execution plan for the provided logical plan.

    This process has 3 steps:
    (1) logical optimization: optimize logical operators.
    (2) planning: convert logical to physical operators.
    (3) physical optimization: optimize physical operators.
    """

    # 1. Get planning functions
    optimize_logical, plan, optimize_physical = get_plan_conversion_fns()

    # 2. Logical -> Logical (Optimized)
    optimized_logical_plan = optimize_logical(logical_plan)

    # 3. Rewire Logical -> Logical (Optimized)
    logical_plan._dag = optimized_logical_plan.dag

    # 4. Logical (Optimized) -> Physical
    physical_plan = plan(optimized_logical_plan)

    # 5. Physical (Optimized) -> Physical
    return optimize_physical(physical_plan)
