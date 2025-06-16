import itertools
import random
import time
from typing import Optional

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
from packaging.version import parse as parse_version

import ray
from ray._private.arrow_utils import get_pyarrow_version
from ray.data._internal.arrow_ops.transform_pyarrow import (
    MIN_PYARROW_VERSION_TYPE_PROMOTION,
    combine_chunks,
)
from ray.data._internal.execution.interfaces.ref_bundle import (
    _ref_bundles_iterator_to_block_refs_list,
)
from ray.data._internal.planner.exchange.sort_task_spec import SortKey
from ray.data._internal.util import is_nan
from ray.data.aggregate import (
    AbsMax,
    AggregateFn,
    Count,
    Max,
    Mean,
    Min,
    Quantile,
    Std,
    Sum,
    Unique,
)
from ray.data.block import BlockAccessor
from ray.data.context import DataContext, ShuffleStrategy
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.util import named_values
from ray.tests.conftest import *  # noqa

RANDOM_SEED = 123


def test_grouped_dataset_repr(
    ray_start_regular_shared_2_cpus, disable_fallback_to_object_extension
):
    ds = ray.data.from_items([{"key": "spam"}, {"key": "ham"}, {"key": "spam"}])
    assert repr(ds.groupby("key")) == f"GroupedData(dataset={ds!r}, key='key')"


def test_groupby_arrow(
    ray_start_regular_shared_2_cpus,
    configure_shuffle_method,
    disable_fallback_to_object_extension,
):
    # Test empty dataset.
    agg_ds = ray.data.range(10).filter(lambda r: r["id"] > 10).groupby("value").count()
    assert agg_ds.count() == 0


def test_groupby_none(
    ray_start_regular_shared_2_cpus,
    configure_shuffle_method,
    disable_fallback_to_object_extension,
):
    ds = ray.data.range(10)
    assert ds.groupby(None).min().take_all() == [{"min(id)": 0}]
    assert ds.groupby(None).max().take_all() == [{"max(id)": 9}]


def test_groupby_errors(
    ray_start_regular_shared_2_cpus, disable_fallback_to_object_extension
):
    ds = ray.data.range(100)
    ds.groupby(None).count().show()  # OK
    with pytest.raises(ValueError):
        ds.groupby(lambda x: x % 2).count().show()
    with pytest.raises(ValueError):
        ds.groupby("foo").count().show()


def test_map_groups_with_gpus(
    shutdown_only, configure_shuffle_method, disable_fallback_to_object_extension
):
    ray.shutdown()
    ray.init(num_gpus=1)

    rows = (
        ray.data.range(1).groupby("id").map_groups(lambda x: x, num_gpus=1).take_all()
    )

    assert rows == [{"id": 0}]


def test_map_groups_with_actors(
    ray_start_regular_shared_2_cpus,
    configure_shuffle_method,
    disable_fallback_to_object_extension,
):
    class Identity:
        def __call__(self, batch):
            return batch

    rows = (
        ray.data.range(1).groupby("id").map_groups(Identity, concurrency=1).take_all()
    )

    assert rows == [{"id": 0}]


def test_map_groups_with_actors_and_args(
    ray_start_regular_shared_2_cpus,
    configure_shuffle_method,
    disable_fallback_to_object_extension,
):
    class Fn:
        def __init__(self, x: int, y: Optional[int] = None):
            self.x = x
            self.y = y

        def __call__(self, batch, q: int, r: Optional[int] = None):
            return {"x": [self.x], "y": [self.y], "q": [q], "r": [r]}

    rows = (
        ray.data.range(1)
        .groupby("id")
        .map_groups(
            Fn,
            concurrency=1,
            fn_constructor_args=[0],
            fn_constructor_kwargs={"y": 1},
            fn_args=[2],
            fn_kwargs={"r": 3},
        )
        .take_all()
    )

    assert rows == [{"x": 0, "y": 1, "q": 2, "r": 3}]


def test_groupby_large_udf_returns(
    ray_start_regular_shared_2_cpus,
    configure_shuffle_method,
    disable_fallback_to_object_extension,
):
    # Test for https://github.com/ray-project/ray/issues/44861.

    # Each UDF return is 128 MiB. If Ray Data doesn't incrementally yield outputs, the
    # combined output size is 128 MiB * 1024 = 128 GiB and Arrow errors.
    def create_large_data(group):
        return {"item": np.zeros((1, 128 * 1024 * 1024), dtype=np.uint8)}

    ds = (
        ray.data.range(1024, override_num_blocks=1)
        .groupby(key="id")
        .map_groups(create_large_data)
    )
    ds.take(1)


@pytest.mark.parametrize("keys", ["A", ["A", "B"]])
def test_agg_inputs(
    ray_start_regular_shared_2_cpus,
    keys,
    configure_shuffle_method,
    disable_fallback_to_object_extension,
):
    xs = list(range(100))
    ds = ray.data.from_items([{"A": (x % 3), "B": x, "C": (x % 2)} for x in xs])

    def check_init(k):
        if len(keys) == 2:
            assert isinstance(k, tuple), k
            assert len(k) == 2
        elif len(keys) == 1:
            assert isinstance(k, int)
        return 1

    def check_finalize(v):
        assert v == 1

    def check_accumulate_merge(a, r):
        assert a == 1
        if isinstance(r, int):
            return 1
        elif len(r) == 3:
            assert all(x in r for x in ["A", "B", "C"])
        else:
            assert False, r
        return 1

    output = ds.groupby(keys).aggregate(
        AggregateFn(
            init=check_init,
            accumulate_row=check_accumulate_merge,
            merge=check_accumulate_merge,
            finalize=check_finalize,
            name="foo",
        )
    )
    output.take_all()


def test_agg_errors(
    ray_start_regular_shared_2_cpus,
    configure_shuffle_method,
    disable_fallback_to_object_extension,
):
    from ray.data.aggregate import Max

    ds = ray.data.range(100)
    ds.aggregate(Max("id"))  # OK
    with pytest.raises(ValueError):
        ds.aggregate(Max())
    with pytest.raises(ValueError):
        ds.aggregate(Max(lambda x: x))
    with pytest.raises(ValueError):
        ds.aggregate(Max("bad_field"))

if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
