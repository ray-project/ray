import ray
from ray.anyscale.data.api.dataset_mixin import DatasetMixin
from ray.anyscale.data.planner import _register_anyscale_plan_logical_op_fns


def _patch_class_with_mixin(original_cls, mixin_cls):
    for name, method in mixin_cls.__dict__.items():
        if not name.startswith("__"):
            setattr(original_cls, name, method)


def apply_anyscale_patches():
    """Apply Anyscale-specific patches for Ray Data."""
    # Patch ray.data.Dataset
    _patch_class_with_mixin(ray.data.Dataset, DatasetMixin)

    _register_anyscale_plan_logical_op_fns()
