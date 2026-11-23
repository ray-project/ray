import time

import pytest

import ray
from ray._common.test_utils import run_string_as_driver
from ray.data.tests.conftest import *  # noqa

pytestmark = pytest.mark.usefixtures("ray_start_2_cpus_shared")


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


@pytest.mark.parametrize(
    "prefetch_on_thread", [False, True], ids=["inline", "threaded"]
)
def test_skipped_metadata_failure_does_not_exhaust_error_budget(
    ray_start_2_cpus_shared, monkeypatch, prefetch_on_thread
):
    """Count a metadata failure once and keep output from subsequent inputs."""
    # The fetch mode is read at import time, so use a fresh driver connected to
    # the shared cluster instead of patching the executor or its Ray calls.
    monkeypatch.setenv(
        "RAY_DATA_METADATA_PREFETCH_ON_THREAD", str(int(prefetch_on_thread))
    )
    driver = f"""
import pandas as pd
import ray

ray.init(address={ray_start_2_cpus_shared["address"]!r})
context = ray.data.DataContext.get_current()
context.max_errored_blocks = 1
context.batch_to_block_arrow_format = False

def encode(batch):
    if batch["id"][0] != 0:
        return pd.DataFrame({{"id": batch["id"]}})

    class LocalValue:
        pass

    # Ray's cloudpickle can serialize the DataFrame, but standard pickle
    # cannot encode the local class in its categorical schema. The task
    # fails after yielding the block and before yielding its metadata.
    return pd.DataFrame({{"id": pd.Categorical([LocalValue()])}})

dataset = ray.data.range(2, override_num_blocks=2).map_batches(
    encode, batch_size=1, max_retries=0
)
assert dataset.take_all() == [{{"id": 1}}]
ray.shutdown()
"""
    run_string_as_driver(driver, timeout=60)


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


NUM_ROWS = 10  # rows of the one-block datasets below, one read task
FAILING_ROW = 7  # the row whose UDF call raises


def fail_at_failing_row(batch):
    """A batch UDF that raises on the batch holding ``FAILING_ROW``."""
    if FAILING_ROW in batch["id"]:
        raise ValueError(f"bad row {FAILING_ROW}")
    return batch


def fail_row_at_failing_row(row):
    """A row UDF that raises on ``FAILING_ROW``."""
    if row["id"] == FAILING_ROW:
        raise ValueError(f"bad row {FAILING_ROW}")
    return row


def test_a_skipped_error_keeps_the_rows_a_task_produced_before_it(restore_data_context):
    """A one-task ``map_batches`` that fails at the batch holding row 7 keeps the
    rows of the batches before it when the job skips the error."""
    ctx = ray.data.DataContext.get_current()
    # Any limit other than 0 skips the error. -1 (no limit) also catches a check
    # that treats only a positive limit as skipping.
    ctx.max_errored_blocks = -1

    ds = ray.data.range(NUM_ROWS, override_num_blocks=1).map_batches(
        fail_at_failing_row, batch_size=2
    )
    rows = sorted(row["id"] for row in ds.take_all())

    # Batches of two: [6, 7] fails, so the batches before it, rows 0 to 5, stay.
    assert rows == list(range(6))


def test_a_skipped_error_keeps_the_rows_a_fused_batcher_holds(restore_data_context):
    """In one fused task, ``map`` fails at row 7 while the downstream
    ``map_batches`` batcher holds rows 4 to 6 of an unfinished batch of four.
    The batcher hands them on as a short batch, so every row before the error
    is kept."""
    ctx = ray.data.DataContext.get_current()
    ctx.max_errored_blocks = 1

    ds = (
        ray.data.range(NUM_ROWS, override_num_blocks=1)
        .map(fail_row_at_failing_row)
        .map_batches(lambda batch: batch, batch_size=4)
    )
    rows = sorted(row["id"] for row in ds.take_all())

    assert rows == list(range(FAILING_ROW))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
