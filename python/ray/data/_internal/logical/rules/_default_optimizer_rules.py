from ray.data._internal.logical.rules.operator_fusion import OperatorFusionRule
from ray.data._internal.logical.rules.randomize_blocks import ReorderRandomizeBlocksRule


def get_logical_optimizer_rules():
    rules = [ReorderRandomizeBlocksRule]
    return rules


def get_physical_optimizer_rules():
    rules = [OperatorFusionRule]
    return rules
