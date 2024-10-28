import pytest

import ray
from ray.anyscale.data.api.context_mixin import DataContextMixin
from ray.anyscale.data.api.dataset_mixin import DatasetMixin
from ray.anyscale.data.apply_anyscale_patches import (
    _patch_class_with_dataclass_mixin,
    _patch_class_with_mixin,
)
from ray.tests.conftest import *  # noqa


def test__patch_class_with_mixin(ray_start_regular_shared):
    _patch_class_with_mixin(ray.data.Dataset, DatasetMixin)

    # Check that Dataset has custom rayturbo methods and attributes.
    assert hasattr(ray.data.Dataset, "write_snowflake")
    assert hasattr(ray.data.Dataset, "streaming_aggregate")


def test__patch_class_with_dataclass_mixin(ray_start_regular_shared):
    _patch_class_with_dataclass_mixin(ray.data.DataContext, DataContextMixin)

    # Check that DataContext has custom rayturbo methods and attributes.
    assert hasattr(ray.data.DataContext, "checkpoint_config")
    assert hasattr(ray.data.DataContext, "_skip_checkpoint_temp")


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
