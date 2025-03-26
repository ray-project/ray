import pytest

import ray
from ray._private.arrow_utils import get_pyarrow_version
from ray.anyscale.data.aggregate_vectorized import (
    MIN_PYARROW_VERSION_VECTORIZED_AGGREGATIONS,
)
from ray.anyscale.data.api.context_mixin import DataContextMixin
from ray.anyscale.data.api.dataset_mixin import DatasetMixin
from ray.anyscale.data.apply_anyscale_patches import (
    _patch_class_with_dataclass_mixin,
    _patch_class_with_mixin,
    _patch_aggregations,
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


def test_patch_aggregations(ray_start_regular_shared):
    _patch_aggregations()

    from ray.data import aggregate
    from ray.anyscale.data import aggregate_vectorized

    should_be_vectorized = (
        get_pyarrow_version() >= MIN_PYARROW_VERSION_VECTORIZED_AGGREGATIONS
    )

    assert should_be_vectorized == (
        aggregate.Count is aggregate_vectorized.CountVectorized
    )
    assert should_be_vectorized == (aggregate.Sum is aggregate_vectorized.SumVectorized)
    assert should_be_vectorized == (aggregate.Min is aggregate_vectorized.MinVectorized)
    assert should_be_vectorized == (aggregate.Max is aggregate_vectorized.MaxVectorized)
    assert should_be_vectorized == (
        aggregate.AbsMax is aggregate_vectorized.AbsMaxVectorized
    )
    assert should_be_vectorized == (
        aggregate.Quantile is aggregate_vectorized.QuantileVectorized
    )
    assert should_be_vectorized == (
        aggregate.Unique is aggregate_vectorized.UniqueVectorized
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
