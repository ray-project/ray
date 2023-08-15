from ray._private.ray_constants import env_bool
from ray.data._internal.logical.rules.operator_fusion import OperatorFusionRule
from ray.data._internal.logical.rules.randomize_blocks import ReorderRandomizeBlocksRule

ANYSCALE_LOCAL_LIMIT_MAP_OPERATOR_ENABLED = env_bool(
    "ANYSCALE_LOCAL_LIMIT_MAP_OPERATOR_ENABLED", True
)


def get_logical_optimizer_rules():
    rules = [ReorderRandomizeBlocksRule]
    return rules


def get_physical_optimizer_rules():
    rules = [OperatorFusionRule]
    # TODO(scottjlee): add back LimitPushdownRule once we
    # enforce number of input/output rows remains the same
    # for Map/MapBatches ops.
    if ANYSCALE_LOCAL_LIMIT_MAP_OPERATOR_ENABLED:
        from ray.anyscale.data.local_limit import ApplyLocalLimitRule

        # Apply ApplyLocalLimitRule before OperatorFusionRule.
        rules = [ApplyLocalLimitRule] + rules
    return rules
