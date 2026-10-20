import time

import pytest

import ray
from ray.data.tests.conftest import *  # noqa


@ray.remote
def sleep():
    time.sleep(999)


@pytest.mark.parametrize(
    "max_errored_blocks, num_errored_blocks",
    [
        (0, 0),
        (0, 1),
        (2, 1),
        (2, 2),
        (2, 3),
        (-1, 5),
    ],
)
def test_max_errored_blocks(
    restore_data_context,
    max_errored_blocks,
    num_errored_blocks,
):
    """Test DataContext.max_errored_blocks."""
    num_tasks = 5

    ctx = ray.data.DataContext.get_current()
    ctx.max_errored_blocks = max_errored_blocks

    def map_func(row):
        id = row["id"]
        if id < num_errored_blocks:
            # Fail the first num_errored_tasks tasks.
            raise RuntimeError(f"Task failed: {id}")
        return row

    ds = ray.data.range(num_tasks, override_num_blocks=num_tasks).map(map_func)
    should_fail = 0 <= max_errored_blocks < num_errored_blocks
    if should_fail:
        with pytest.raises(Exception, match="Task failed"):
            res = ds.take_all()
    else:
        res = sorted([row["id"] for row in ds.take_all()])
        assert res == list(range(num_errored_blocks, num_tasks))
        stats = ds._get_stats_summary()
        assert stats.extra_metrics["num_tasks_failed"] == num_errored_blocks


def test_map_retry_after_partial_output_returns_every_row_once(restore_data_context):
    """A task that fails once after producing rows and is retried
    (``retried_map_errors``) returns every row exactly once, also when the job
    skips errors."""
    ctx = ray.data.DataContext.get_current()
    ctx.retried_map_errors = ["transient"]
    ctx.max_map_retries = 1
    ctx.max_errored_blocks = 1
    failed = []

    def flaky(batch):
        # Fails once, at the third batch, after two batches are buffered.
        if batch["id"][0] == 4 and not failed:
            failed.append(True)
            raise ValueError("transient")
        return batch

    ds = ray.data.range(10, override_num_blocks=1).map_batches(flaky, batch_size=2)

    assert sorted(row["id"] for row in ds.take_all()) == list(range(10))


def test_ray_task_retry_after_partial_output_returns_every_row_once(
    restore_data_context, tmp_path
):
    """A task that fails once after producing rows and that Ray reruns
    (``retry_exceptions``) returns every row exactly once, also when the job
    skips errors."""
    ctx = ray.data.DataContext.get_current()
    ctx.max_errored_blocks = 1
    # Ray reruns the task in a fresh worker, so "failed once" lives in a file.
    failed = tmp_path / "failed"

    def flaky(batch):
        # Fails once, at the third batch, after two batches are buffered.
        if batch["id"][0] == 4 and not failed.exists():
            failed.touch()
            raise ValueError("transient")
        return batch

    ds = ray.data.range(10, override_num_blocks=1).map_batches(
        flaky, batch_size=2, retry_exceptions=[ValueError]
    )

    assert sorted(row["id"] for row in ds.take_all()) == list(range(10))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
