from ray.data._internal.logical.rules.operator_fusion import OperatorFusionRule
from ray.data._internal.logical.rules.randomize_blocks import ReorderRandomizeBlocksRule
from ray.data._internal.logical.rules.zero_copy_map_fusion import (
    EliminateBuildOutputBlocks,
)

DEFAULT_LOGICAL_RULES = [
    ReorderRandomizeBlocksRule,
]

DEFAULT_PHYSICAL_RULES = [
    OperatorFusionRule,
    # Subclasses of ZeroCopyMapFusionRule (e.g., EliminateBuildOutputBlocks) should
    # be run after OperatorFusionRule.
    EliminateBuildOutputBlocks,
]

USER_PROVIDED_LOGICAL_RULES = []

USER_PROVIDED_PHYSICAL_RULES = []


def get_logical_optimizer_rules():
    return DEFAULT_LOGICAL_RULES + USER_PROVIDED_LOGICAL_RULES


def get_physical_optimizer_rules():
    return DEFAULT_PHYSICAL_RULES + USER_PROVIDED_PHYSICAL_RULES
