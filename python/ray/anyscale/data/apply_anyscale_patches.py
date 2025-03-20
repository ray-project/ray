from dataclasses import fields

import ray
from ray._private.ray_constants import env_bool
from ray.anyscale.data._internal.execution.callbacks.insert_issue_detectors import (
    IssueDetectionExecutionCallback,
)
from ray.anyscale.data._internal.execution.rules.insert_checkpointing import (
    InsertCheckpointingLayerRule,
)
from ray.anyscale.data._internal.logging import configure_anyscale_logging
from ray.anyscale.data._internal.logical.rules import (
    ApplyLocalLimitRule,
    FuseRepartitionOutputBlocks,
    PredicatePushdown,
    ProjectionPushdown,
    PushdownCountFiles,
    RedundantMapTransformPruning,
)
from ray.anyscale.data.api.context_mixin import DataContextMixin
from ray.anyscale.data.api.dataset_mixin import DatasetMixin
from ray.anyscale.data.planner import _register_anyscale_plan_logical_op_fns
from ray.data._internal.execution.execution_callback import add_execution_callback
from ray.data._internal.logical.optimizers import (
    get_logical_ruleset,
    get_physical_ruleset,
)

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


def _patch_default_execution_callbacks():
    add_execution_callback(
        IssueDetectionExecutionCallback(), ray.data.DataContext.get_current()
    )


def apply_anyscale_patches():
    """Apply Anyscale-specific patches for Ray Data."""
    # Patch ray.data.Dataset
    _patch_class_with_mixin(ray.data.Dataset, DatasetMixin)
    _patch_class_with_dataclass_mixin(ray.data.DataContext, DataContextMixin)
    _patch_default_execution_callbacks()

    _register_anyscale_plan_logical_op_fns()

    logical_ruleset = get_logical_ruleset()
    logical_ruleset.add(PredicatePushdown)
    logical_ruleset.add(PushdownCountFiles)
    logical_ruleset.add(ProjectionPushdown)

    physical_ruleset = get_physical_ruleset()
    if ANYSCALE_LOCAL_LIMIT_MAP_OPERATOR_ENABLED:
        physical_ruleset.add(ApplyLocalLimitRule)
    physical_ruleset.add(InsertCheckpointingLayerRule)
    physical_ruleset.add(RedundantMapTransformPruning)
    physical_ruleset.add(FuseRepartitionOutputBlocks)

    configure_anyscale_logging()
