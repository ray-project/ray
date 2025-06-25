import pytest

import ray
from ray.data.aggregate import (
    AggregateFn,
    Max,
)
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa

RANDOM_SEED = 123


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
