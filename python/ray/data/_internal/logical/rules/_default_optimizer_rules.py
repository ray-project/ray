from ray.data._internal.logical.rules.operator_fusion import OperatorFusionRule
from ray.data._internal.logical.rules.randomize_blocks import ReorderRandomizeBlocksRule
from ray.data._internal.logical.rules.zero_copy_map_fusion import (
    ReadOpZeroCopyMapFusion,
)


def get_logical_optimizer_rules():
    rules = [ReorderRandomizeBlocksRule]
    return rules


def get_physical_optimizer_rules():
    rules = [OperatorFusionRule, ReadOpZeroCopyMapFusion]
    return rules
