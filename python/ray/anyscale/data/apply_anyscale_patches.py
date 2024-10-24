import ray
from ray._private.ray_constants import env_bool
from ray.anyscale.data._internal.logging import configure_anyscale_logging
from ray.anyscale.data._internal.logical.rules import (
    ApplyLocalLimitRule,
    PushdownCountFiles,
)
from ray.anyscale.data.api.dataset_mixin import DatasetMixin
from ray.anyscale.data.planner import _register_anyscale_plan_logical_op_fns
from ray.data._internal.logical.optimizers import (
    register_logical_rule,
    register_physical_rule,
)

ANYSCALE_LOCAL_LIMIT_MAP_OPERATOR_ENABLED = env_bool(
    "ANYSCALE_LOCAL_LIMIT_MAP_OPERATOR_ENABLED", False
)


def _patch_class_with_mixin(original_cls, mixin_cls):
    for name, method in mixin_cls.__dict__.items():
        if not name.startswith("__"):
            setattr(original_cls, name, method)


def apply_anyscale_patches():
    """Apply Anyscale-specific patches for Ray Data."""
    # Patch ray.data.Dataset
    _patch_class_with_mixin(ray.data.Dataset, DatasetMixin)

    _register_anyscale_plan_logical_op_fns()

    register_logical_rule(PushdownCountFiles)

    if ANYSCALE_LOCAL_LIMIT_MAP_OPERATOR_ENABLED:
        register_physical_rule(ApplyLocalLimitRule)

    configure_anyscale_logging()
