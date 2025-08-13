from typing import List

from .ruleset import Ruleset
from ray.data._internal.logical.interfaces import (
    LogicalPlan,
    Optimizer,
    PhysicalPlan,
    Rule,
)
from ray.data._internal.logical.rules.configure_map_task_memory import (
    ConfigureMapTaskMemoryUsingOutputSize,
)
from ray.data._internal.logical.rules.inherit_batch_format import InheritBatchFormatRule
from ray.data._internal.logical.rules.inherit_target_max_block_size import (
    InheritTargetMaxBlockSizeRule,
)
from ray.data._internal.logical.rules.limit_pushdown import LimitPushdownRule
from ray.data._internal.logical.rules.operator_fusion import FuseOperators
from ray.data._internal.logical.rules.set_read_parallelism import SetReadParallelismRule
from ray.data._internal.logical.rules.zero_copy_map_fusion import (
    EliminateBuildOutputBlocks,
)
from ray.util.annotations import DeveloperAPI

_LOGICAL_RULESET = Ruleset(
    [
        InheritBatchFormatRule,
        LimitPushdownRule,
    ]
)


_PHYSICAL_RULESET = Ruleset(
    [
        InheritTargetMaxBlockSizeRule,
        SetReadParallelismRule,
        FuseOperators,
        EliminateBuildOutputBlocks,
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


def get_execution_plan(logical_plan: LogicalPlan) -> PhysicalPlan:
    """Get the physical execution plan for the provided logical plan.

    This process has 3 steps:
    (1) logical optimization: optimize logical operators.
    (2) planning: convert logical to physical operators.
    (3) physical optimization: optimize physical operators.
    """
    from ray.data._internal.planner import create_planner

    optimized_logical_plan = LogicalOptimizer().optimize(logical_plan)
    logical_plan._dag = optimized_logical_plan.dag
    physical_plan = create_planner().plan(optimized_logical_plan)
    return PhysicalOptimizer().optimize(physical_plan)
