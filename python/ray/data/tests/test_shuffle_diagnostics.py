import logging

import pytest

import ray
from ray.data.context import DataContext, ShuffleStrategy
from ray.data.dataset import Dataset

SHUFFLE_ALL_TO_ALL_OPS = [
    Dataset.random_shuffle,
    lambda ds: ds.sort(key="id"),
    lambda ds: ds.groupby("id").map_groups(lambda group: group),
]


@pytest.mark.parametrize(
    "shuffle_op",
    SHUFFLE_ALL_TO_ALL_OPS,
)
def test_debug_limit_shuffle_execution_to_num_blocks(
    ray_start_regular, restore_data_context, configure_shuffle_method, shuffle_op
):
    if configure_shuffle_method == ShuffleStrategy.HASH_SHUFFLE:
        pytest.skip("Not supported by hash-shuffle")

    shuffle_fn = shuffle_op

    parallelism = 100
    ds = ray.data.range(1000, override_num_blocks=parallelism)
    shuffled_ds = shuffle_fn(ds).materialize()
    shuffled_ds = shuffled_ds.materialize()
    assert shuffled_ds._plan.initial_num_blocks() == parallelism

    ds.context.set_config("debug_limit_shuffle_execution_to_num_blocks", 1)
    shuffled_ds = shuffle_fn(ds).materialize()
    shuffled_ds = shuffled_ds.materialize()
    assert shuffled_ds._plan.initial_num_blocks() == 1


def test_memory_usage(
    ray_start_regular, restore_data_context, configure_shuffle_method
):
    parallelism = 2
    ds = ray.data.range(int(1e8), override_num_blocks=parallelism)
    ds = ds.random_shuffle().materialize()

    stats = ds._get_stats_summary()
    # TODO(swang): Sort on this dataset seems to produce significant skew, so
    # one task uses much more memory than the other.
    for op_stats in stats.operators_stats:
        assert op_stats.memory["max"] < 2000


@pytest.mark.parametrize("under_threshold", [False, True])
def test_sort_object_ref_warnings(
    ray_start_regular,
    restore_data_context,
    configure_shuffle_method,
    under_threshold,
    propagate_logs,
    caplog,
):
    # Test that we warn iff expected driver memory usage from
    # storing ObjectRefs is higher than the configured
    # threshold.
    warning_str = "Execution is estimated to use"
    warning_str_with_bytes = (
        "Execution is estimated to use at least "
        f"{90 if configure_shuffle_method == ShuffleStrategy.SORT_SHUFFLE_PUSH_BASED else 300}KB"
    )

    if not under_threshold:
        DataContext.get_current().warn_on_driver_memory_usage_bytes = 10_000

    ds = ray.data.range(int(1e8), override_num_blocks=10)
    with caplog.at_level(logging.WARNING, logger="ray.data.dataset"):
        ds = ds.random_shuffle().materialize()

    if under_threshold:
        assert warning_str not in caplog.text
        assert warning_str_with_bytes not in caplog.text
    else:
        assert warning_str in caplog.text
        assert warning_str_with_bytes in caplog.text


@pytest.mark.parametrize("under_threshold", [False, True])
def test_sort_inlined_objects_warnings(
    ray_start_regular,
    restore_data_context,
    configure_shuffle_method,
    under_threshold,
    propagate_logs,
    caplog,
):
    # Test that we warn iff expected driver memory usage from
    # storing tiny Ray objects on driver heap is higher than
    # the configured threshold.
    if configure_shuffle_method == ShuffleStrategy.SORT_SHUFFLE_PUSH_BASED:
        warning_strs = [
            "More than 3MB of driver memory used",
            "More than 7MB of driver memory used",
        ]
    else:
        warning_strs = [
            "More than 8MB of driver memory used",
        ]

    if not under_threshold:
        DataContext.get_current().warn_on_driver_memory_usage_bytes = 3_000_000

    ds = ray.data.range(int(1e6), override_num_blocks=10)
    with caplog.at_level(logging.WARNING, logger="ray.data.dataset"):
        ds = ds.random_shuffle().materialize()

    if under_threshold:
        assert all(warning_str not in caplog.text for warning_str in warning_strs)
    else:
        assert all(warning_str in caplog.text for warning_str in warning_strs)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
