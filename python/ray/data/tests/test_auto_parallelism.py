from dataclasses import astuple, dataclass

import pytest

import ray
from ray.data._internal.util import _autodetect_parallelism
from ray.data.context import DataContext
from ray.tests.conftest import *  # noqa
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy


@dataclass
class TestCase:
    avail_cpus: int
    target_max_block_size: int
    data_size: int
    expected_parallelism: int


MiB = 1024 * 1024
GiB = 1024 * MiB


TEST_CASES = [
    TestCase(
        avail_cpus=4,
        target_max_block_size=DataContext.get_current().target_max_block_size,
        data_size=1024,
        expected_parallelism=1,  # Capped by DataContext.target_min_block_size (16Mb)
    ),
    TestCase(
        avail_cpus=4,
        target_max_block_size=DataContext.get_current().target_max_block_size,
        data_size=10 * MiB,
        expected_parallelism=1,  # Capped by DataContext.target_min_block_size (16Mb)
    ),
    TestCase(
        avail_cpus=4,
        target_max_block_size=DataContext.get_current().target_max_block_size,
        data_size=1 * GiB,
        expected_parallelism=64,  # Capped by DataContext.target_min_block_size (16Mb)
    ),
    TestCase(
        avail_cpus=4,
        target_max_block_size=DataContext.get_current().target_max_block_size,
        data_size=10 * GiB,
        expected_parallelism=200,  # Determined by DataContext.read_op_min_num_blocks
    ),
    TestCase(
        avail_cpus=150,
        target_max_block_size=DataContext.get_current().target_max_block_size,
        data_size=10 * GiB,
        expected_parallelism=300,  # Determined by available CPUs (x2)
    ),
    TestCase(
        avail_cpus=400,
        target_max_block_size=DataContext.get_current().target_max_block_size,
        data_size=10 * GiB,
        expected_parallelism=640,  # Capped by DataContext.target_min_block_size (16Mb)
    ),
    TestCase(
        avail_cpus=4,
        target_max_block_size=DataContext.get_current().target_max_block_size,
        data_size=1000 * GiB,
        expected_parallelism=8000,  # Floored by DataContext.target_max_block_size
    ),
    TestCase(
        avail_cpus=4,
        target_max_block_size=512 * MiB,
        data_size=1000 * GiB,
        expected_parallelism=2000,  # Floored by passed in target_max_block_size (128Mb)
    ),
]


@pytest.mark.parametrize(
    "avail_cpus,target_max_block_size,data_size,expected",
    [astuple(test) for test in TEST_CASES],
)
def test_autodetect_parallelism(
    shutdown_only, avail_cpus, target_max_block_size, data_size, expected
):
    class MockReader:
        def estimate_inmemory_data_size(self):
            return data_size

    result, reason, _ = _autodetect_parallelism(
        parallelism=-1,
        target_max_block_size=target_max_block_size,
        ctx=DataContext.get_current(),
        datasource_or_legacy_reader=MockReader(),
        avail_cpus=avail_cpus,
    )

    print(f">>> Parallelism chosen based on: {reason}")

    assert result == expected, reason


def test_auto_parallelism_basic(shutdown_only, restore_data_context):
    ray.init(num_cpus=8)

    context = DataContext.get_current()
    context.read_op_min_num_blocks = 1
    context.target_min_block_size = 1 * MiB

    # Datasource bound.
    ds = ray.data.range_tensor(5, shape=(100,), override_num_blocks=-1)
    # Expected data size is < 4Kb, therefore all of the data could fit into
    # 1 block (of at least 1MiB)
    assert ds._plan.initial_num_blocks() == 1, ds

    ds = ray.data.range_tensor(10000, shape=(100,), override_num_blocks=-1)
    # Expected data size is < 8Mb, therefore all of the data could fit into
    # 8 blocks (of at least 1MiB each)
    assert ds._plan.initial_num_blocks() == 8, ds

    # Block size bound.
    ds = ray.data.range_tensor(100000000, shape=(100,), override_num_blocks=-1)
    assert ds._plan.initial_num_blocks() >= 590, ds
    assert ds._plan.initial_num_blocks() <= 600, ds


def test_auto_parallelism_placement_group(shutdown_only):
    ray.init(num_cpus=16, num_gpus=8)

    @ray.remote
    def run():
        context = DataContext.get_current()
        context.min_parallelism = 1
        context.target_min_block_size = 1 * MiB
        ds = ray.data.range_tensor(10000, shape=(100,), override_num_blocks=-1)
        return ds._plan.initial_num_blocks()

    # 1/16 * 4 * 16 = 4
    pg = ray.util.placement_group([{"CPU": 1}])
    num_blocks = ray.get(
        run.options(
            scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg)
        ).remote()
    )
    assert num_blocks == 4, num_blocks

    # 2/16 * 4 * 16 = 8
    pg = ray.util.placement_group([{"CPU": 2}])
    num_blocks = ray.get(
        run.options(
            scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg)
        ).remote()
    )
    assert num_blocks == 8, num_blocks

    # 1/8 * 4 * 16 = 8
    pg = ray.util.placement_group([{"CPU": 1, "GPU": 1}])
    num_blocks = ray.get(
        run.options(
            scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg)
        ).remote()
    )
    assert num_blocks == 8, num_blocks


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
