import pytest

import ray
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.test_execution_optimizer_limit_pushdown import (
    _check_valid_plan_and_result,
)
from ray.tests.conftest import *  # noqa


def test_add_streaming_repartition_when_zip(ray_start_regular_shared):
    """Test AddStreamingRepartitionWhenZip rule adds/updates StreamingRepartition before Zip."""
    # Test 1: Basic zip with no StreamingRepartition
    ds1 = ray.data.range(10)
    ds2 = ray.data.range(10)
    ds = ds1.zip(ds2)
    _check_valid_plan_and_result(
        ds,
        "Read[ReadRange] -> StreamingRepartition[StreamingRepartition[num_rows_per_block=128]], Read[ReadRange] -> StreamingRepartition[StreamingRepartition[num_rows_per_block=128]] -> Zip[Zip]",
        [{"id": i, "id_1": i} for i in range(10)],
    )

    # Test 2: Zip with existing StreamingRepartition (target=128)
    ds1 = ray.data.range(10).repartition(target_num_rows_per_block=128)
    ds2 = ray.data.range(10).repartition(target_num_rows_per_block=128)
    ds = ds1.zip(ds2)
    _check_valid_plan_and_result(
        ds,
        "Read[ReadRange] -> StreamingRepartition[StreamingRepartition[num_rows_per_block=128]], Read[ReadRange] -> StreamingRepartition[StreamingRepartition[num_rows_per_block=128]] -> Zip[Zip]",
        [{"id": i, "id_1": i} for i in range(10)],
    )

    # Test 3: Zip with existing StreamingRepartition with different target
    ds1 = ray.data.range(10).repartition(target_num_rows_per_block=64)
    ds2 = ray.data.range(10).repartition(target_num_rows_per_block=256)
    ds = ds1.zip(ds2)
    _check_valid_plan_and_result(
        ds,
        "Read[ReadRange] -> StreamingRepartition[StreamingRepartition[num_rows_per_block=128]], Read[ReadRange] -> StreamingRepartition[StreamingRepartition[num_rows_per_block=128]] -> Zip[Zip]",
        [{"id": i, "id_1": i} for i in range(10)],
    )

    # Test 4: Mixed case - some inputs have StreamingRepartition, some don't
    ds1 = ray.data.range(10).repartition(target_num_rows_per_block=64)
    ds2 = ray.data.range(10)  # No StreamingRepartition
    ds3 = ray.data.range(10).repartition(target_num_rows_per_block=128)
    ds = ds1.zip(ds2, ds3)
    _check_valid_plan_and_result(
        ds,
        "Read[ReadRange] -> StreamingRepartition[StreamingRepartition[num_rows_per_block=128]], Read[ReadRange] -> StreamingRepartition[StreamingRepartition[num_rows_per_block=128]], Read[ReadRange] -> StreamingRepartition[StreamingRepartition[num_rows_per_block=128]] -> Zip[Zip]",
        [{"id": i, "id_1": i, "id_2": i} for i in range(10)],
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
