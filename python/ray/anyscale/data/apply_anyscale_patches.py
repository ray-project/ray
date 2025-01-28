from dataclasses import fields

import ray
from ray._private.ray_constants import env_bool
from ray.anyscale.data._internal.execution.rules.insert_checkpointing import (
    InsertCheckpointingLayerRule,
)
from ray.anyscale.data._internal.logging import configure_anyscale_logging
from ray.anyscale.data._internal.logical.rules import (
    ApplyLocalLimitRule,
    PredicatePushdown,
    ProjectionPushdown,
    PushdownCountFiles,
    RedundantMapTransformBatchPruning,
    RedundantMapTransformRowPruning,
)
from ray.anyscale.data.api.context_mixin import DataContextMixin
from ray.anyscale.data.api.dataset_mixin import DatasetMixin
from ray.anyscale.data.planner import _register_anyscale_plan_logical_op_fns
from ray.data._internal.logical.optimizers import (
    _PHYSICAL_RULES,
    register_logical_rule,
    register_physical_rule,
)
from ray.data._internal.logical.rules.operator_fusion import OperatorFusionRule

ANYSCALE_LOCAL_LIMIT_MAP_OPERATOR_ENABLED = env_bool(
    "ANYSCALE_LOCAL_LIMIT_MAP_OPERATOR_ENABLED", False
)


def _patch_class_with_mixin(original_cls, mixin_cls):
    for name, method in mixin_cls.__dict__.items():
        if not name.startswith("__"):
            setattr(original_cls, name, method)


def _patch_class_with_dataclass_mixin(original_cls, dataclass_mixin_cls):
    # Create an instance of the dataclass in order to get default values.
    mixin_instance = dataclass_mixin_cls()
    for field in fields(dataclass_mixin_cls):
        setattr(original_cls, field.name, getattr(mixin_instance, field.name))


def apply_anyscale_patches():
    """Apply Anyscale-specific patches for Ray Data."""
    # Patch ray.data.Dataset
    _patch_class_with_mixin(ray.data.Dataset, DatasetMixin)
    _patch_class_with_dataclass_mixin(ray.data.DataContext, DataContextMixin)

    _register_anyscale_plan_logical_op_fns()

    register_logical_rule(PredicatePushdown)
    register_logical_rule(PushdownCountFiles)
    register_logical_rule(ProjectionPushdown)

    if ANYSCALE_LOCAL_LIMIT_MAP_OPERATOR_ENABLED:
        register_physical_rule(ApplyLocalLimitRule)

    # Insert checkpointing rule before operator fusion.
    op_fusion_idx = _PHYSICAL_RULES.index(OperatorFusionRule)
    register_physical_rule(InsertCheckpointingLayerRule, op_fusion_idx - 1)
    register_physical_rule(RedundantMapTransformRowPruning)
    register_physical_rule(RedundantMapTransformBatchPruning)

    configure_anyscale_logging()
