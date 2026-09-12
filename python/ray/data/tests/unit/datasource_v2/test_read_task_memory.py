"""Tests for the per-read-task Ray ``memory`` reservation.

The reservation only makes sense when a partition's decoded size is actually
known, and it must decline in the cases where reserving memory would either
override the caller or make tasks unschedulable -- so most of these are about
when *not* to set it.
"""

import pytest

from ray.data._internal.datasource_v2.partitioners.online_bin_packer import (
    OnlineBinPacker,
)
from ray.data._internal.datasource_v2.partitioners.round_robin_partitioner import (
    RoundRobinPartitioner,
)
from ray.data._internal.datasource_v2.read_task_memory import (
    READ_TASK_BASE_MEMORY,
    READ_TASK_MEMORY_PER_DECODED_BYTE,
    apply_read_task_memory,
    read_task_memory,
)
from ray.data._internal.util import MiB
from ray.data.context import DataContext
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy


@pytest.fixture
def ctx():
    # A copy, so mutating scheduling strategy can't leak into other tests.
    return DataContext.get_current().copy()


def test_reservation_is_base_plus_multiple_of_decoded_size():
    assert read_task_memory(0) == READ_TASK_BASE_MEMORY
    assert (
        read_task_memory(128 * MiB)
        == READ_TASK_BASE_MEMORY + READ_TASK_MEMORY_PER_DECODED_BYTE * 128 * MiB
    )
    # Monotonic in the bound, or a bigger bin could reserve less than a smaller one.
    assert read_task_memory(64 * MiB) < read_task_memory(128 * MiB)


def test_bin_packer_exposes_its_cap_as_the_decoded_bound():
    packer = OnlineBinPacker(max_bin_bytes=128 * MiB)
    assert packer.max_partition_decoded_bytes == 128 * MiB


def test_memory_is_set_from_the_packers_cap(ctx):
    packer = OnlineBinPacker(max_bin_bytes=128 * MiB)

    args = apply_read_task_memory({"num_cpus": 1}, packer, ctx)

    assert args["memory"] == read_task_memory(128 * MiB)
    # Untouched keys survive.
    assert args["num_cpus"] == 1


def test_input_args_are_not_mutated(ctx):
    original = {"num_cpus": 1}

    apply_read_task_memory(original, OnlineBinPacker(max_bin_bytes=128 * MiB), ctx)

    assert original == {"num_cpus": 1}


def test_explicit_caller_memory_wins(ctx):
    packer = OnlineBinPacker(max_bin_bytes=128 * MiB)

    args = apply_read_task_memory({"memory": 7}, packer, ctx)

    assert args["memory"] == 7


def test_declines_when_the_partitioner_cannot_bound_decoded_size(ctx):
    """``RoundRobinPartitioner`` sizes buckets from an encoding-ratio estimate.

    Reserving against that would promote a guess to a scheduling constraint, so
    the bound is ``None`` and no ``memory`` is set.
    """
    partitioner = RoundRobinPartitioner(
        in_memory_size_estimator=None,
        min_bucket_size=0,
        max_bucket_size=128 * MiB,
        num_buckets=1,
    )
    assert partitioner.max_partition_decoded_bytes is None

    args = apply_read_task_memory({"num_cpus": 1}, partitioner, ctx)

    assert "memory" not in args


def test_declines_when_a_zero_cap_gives_nothing_to_size_against(ctx):
    args = apply_read_task_memory(
        {"num_cpus": 1}, OnlineBinPacker(max_bin_bytes=0), ctx
    )

    assert "memory" not in args


@pytest.mark.parametrize("source", ["ray_remote_args", "context"])
def test_declines_under_a_placement_group_strategy(ctx, source):
    """A placement group that reserves no memory would never schedule the task."""
    strategy = PlacementGroupSchedulingStrategy(placement_group=object())
    ray_remote_args = {"num_cpus": 1}
    if source == "ray_remote_args":
        ray_remote_args["scheduling_strategy"] = strategy
    else:
        ctx.scheduling_strategy = strategy

    args = apply_read_task_memory(
        ray_remote_args, OnlineBinPacker(max_bin_bytes=128 * MiB), ctx
    )

    assert "memory" not in args


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
