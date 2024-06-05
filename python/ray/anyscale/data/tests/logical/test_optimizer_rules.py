import pytest

import ray
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.test_execution_optimizer import _check_valid_plan_and_result
from ray.tests.conftest import *  # noqa


def test_apply_local_limit(ray_start_regular_shared, enable_optimizer):
    def f1(x):
        return x

    ds = ray.data.range(100, parallelism=2).map(f1).limit(1)
    _check_valid_plan_and_result(
        ds,
        "Read[ReadRange] -> MapRows[Map(f1)] -> Limit[limit=1]",
        [{"id": 0}],
        ["ReadRange->Map(f1->Limit[1])", "limit=1"],
    )
    assert ds._block_num_rows() == [1]

    # Test larger parallelism still only yields one block.
    ds = ray.data.range(10000, parallelism=50).map(f1).limit(50)
    _check_valid_plan_and_result(
        ds,
        "Read[ReadRange] -> MapRows[Map(f1)] -> Limit[limit=50]",
        [{"id": i} for i in range(50)],
        ["ReadRange->Map(f1->Limit[50])", "limit=50"],
    )
    assert ds._block_num_rows() == [50]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
