from typing import List, Type

from ray.data._internal.logical.interfaces import (
    LogicalPlan,
    Optimizer,
    PhysicalPlan,
    Rule,
)
from ray.data._internal.logical.rules.inherit_target_max_block_size import (
    InheritTargetMaxBlockSizeRule,
)
from ray.data._internal.logical.rules.operator_fusion import OperatorFusionRule
from ray.data._internal.logical.rules.randomize_blocks import ReorderRandomizeBlocksRule
from ray.data._internal.logical.rules.set_read_parallelism import SetReadParallelismRule
from ray.data._internal.logical.rules.zero_copy_map_fusion import (
    EliminateBuildOutputBlocks,
)
from ray.data._internal.planner.planner import Planner
from ray.util.annotations import DeveloperAPI

_LOGICAL_RULES = [
    ReorderRandomizeBlocksRule,
]

_PHYSICAL_RULES = [
    InheritTargetMaxBlockSizeRule,
    SetReadParallelismRule,
    OperatorFusionRule,
    EliminateBuildOutputBlocks,
]


@DeveloperAPI
def register_logical_rule(cls: Type[Rule]):
    _LOGICAL_RULES.append(cls)


@DeveloperAPI
def register_physical_rule(cls: Type[Rule]):
    _PHYSICAL_RULES.append(cls)


class LogicalOptimizer(Optimizer):
    """The optimizer for logical operators."""

    @property
    def rules(self) -> List[Rule]:
        return [rule_cls() for rule_cls in _LOGICAL_RULES]


class PhysicalOptimizer(Optimizer):
    """The optimizer for physical operators."""

    @property
    def rules(self) -> List[Rule]:
        return [rule_cls() for rule_cls in _PHYSICAL_RULES]


def get_execution_plan(logical_plan: LogicalPlan) -> PhysicalPlan:
    """Get the physical execution plan for the provided logical plan.

    This process has 3 steps:
    (1) logical optimization: optimize logical operators.
    (2) planning: convert logical to physical operators.
    (3) physical optimization: optimize physical operators.
    """
    optimized_logical_plan = LogicalOptimizer().optimize(logical_plan)
    logical_plan._dag = optimized_logical_plan.dag
    physical_plan = Planner().plan(optimized_logical_plan)
    return PhysicalOptimizer().optimize(physical_plan)
