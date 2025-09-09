import sys
import threading

import pandas as pd
import pytest

import ray
from ray._common.test_utils import wait_for_condition
from ray._private.internal_api import memory_summary
from ray.tests.conftest import *  # noqa


def check_no_spill(ctx, dataset):
    # Iterate over the dataset for 10 epochs to stress test that
    # no spilling will happen.
    max_epoch = 10
    for _ in range(max_epoch):
        for _ in dataset.iter_batches(batch_size=None):
            pass
    meminfo = memory_summary(ctx.address_info["address"], stats_only=True)
    assert "Spilled" not in meminfo, meminfo

    def _all_executor_threads_exited():
        for thread in threading.enumerate():
            if thread.name.startswith("StreamingExecutor-"):
                return False
        return True

    # Wait for all executor threads to exit here.
    # If we don't do this, the executor will continue running after the current
    # task case is finished, and auto-init Ray when using some Ray APIs.
    # This will make the next test case fail to init Ray.
    wait_for_condition(_all_executor_threads_exited, timeout=10, retry_interval_ms=1000)


def check_iter_torch_batches_no_spill(ctx, dataset):
    # Iterate over the dataset for 10 epochs to stress test that
    # no spilling will happen.
    max_epoch = 10
    for _ in range(max_epoch):
        for _ in dataset.iter_torch_batches(batch_size=None):
            pass
    meminfo = memory_summary(ctx.address_info["address"], stats_only=True)
    assert "Spilled" not in meminfo, meminfo


def check_to_tf_no_spill(ctx, dataset):
    # Iterate over the dataset for 10 epochs to stress test that
    # no spilling will happen.
    max_epoch = 10
    for _ in range(max_epoch):
        for _ in dataset.to_tf(
            feature_columns="data", label_columns="label", batch_size=None
        ):
            pass
    meminfo = memory_summary(ctx.address_info["address"], stats_only=True)
    assert "Spilled" not in meminfo, meminfo


def check_iter_tf_batches_no_spill(ctx, dataset):
    # Iterate over the dataset for 10 epochs to stress test that
    # no spilling will happen.
    max_epoch = 10
    for _ in range(max_epoch):
        for _ in dataset.iter_tf_batches():
            pass
    meminfo = memory_summary(ctx.address_info["address"], stats_only=True)
    assert "Spilled" not in meminfo, meminfo


def test_iter_batches_no_spilling_upon_no_transformation(shutdown_only):
    # The object store is about 300MB.
    ctx = ray.init(num_cpus=1, object_store_memory=300e6)
    # The size of dataset is 500*(80*80*4)*8B, about 100MB.
    ds = ray.data.range_tensor(500, shape=(80, 80, 4), override_num_blocks=100)
    check_no_spill(ctx, ds)


def test_torch_iteration(shutdown_only):
    # The object store is about 400MB.
    ctx = ray.init(num_cpus=1, object_store_memory=400e6)
    # The size of dataset is 500*(80*80*4)*8B, about 100MB.
    ds = ray.data.range_tensor(500, shape=(80, 80, 4), override_num_blocks=100)

    # iter_torch_batches
    check_iter_torch_batches_no_spill(ctx, ds)


@pytest.mark.skipif(
    sys.version_info >= (3, 12), reason="No tensorflow for Python 3.12+"
)
def test_tf_iteration(shutdown_only):
    # The object store is about 800MB.
    ctx = ray.init(num_cpus=1, object_store_memory=800e6)
    # The size of dataset is 500*(80*80*4)*8B, about 100MB.
    ds = ray.data.range_tensor(
        500, shape=(80, 80, 4), override_num_blocks=100
    ).add_column("label", lambda df: pd.Series([1] * len(df)))

    # to_tf
    check_to_tf_no_spill(ctx, ds.map(lambda x: x))
    # iter_tf_batches
    check_iter_tf_batches_no_spill(ctx, ds.map(lambda x: x))


def test_iter_batches_no_spilling_upon_prior_transformation(shutdown_only):
    # The object store is about 500MB.
    ctx = ray.init(num_cpus=1, object_store_memory=500e6)
    # The size of dataset is 500*(80*80*4)*8B, about 100MB.
    ds = ray.data.range_tensor(500, shape=(80, 80, 4), override_num_blocks=100)

    check_no_spill(ctx, ds.map_batches(lambda x: x))


def test_iter_batches_no_spilling_upon_post_transformation(shutdown_only):
    # The object store is about 500MB.
    ctx = ray.init(num_cpus=1, object_store_memory=500e6)
    # The size of dataset is 500*(80*80*4)*8B, about 100MB.
    ds = ray.data.range_tensor(500, shape=(80, 80, 4), override_num_blocks=100)

    check_no_spill(ctx, ds.map_batches(lambda x: x, batch_size=5))


def test_iter_batches_no_spilling_upon_transformations(shutdown_only):
    # The object store is about 700MB.
    ctx = ray.init(num_cpus=1, object_store_memory=700e6)
    # The size of dataset is 500*(80*80*4)*8B, about 100MB.
    ds = ray.data.range_tensor(500, shape=(80, 80, 4), override_num_blocks=100)

    check_no_spill(
        ctx,
        ds.map_batches(lambda x: x).map_batches(lambda x: x),
    )


def test_global_bytes_spilled(shutdown_only):
    # The object store is about 90MB.
    ctx = ray.init(object_store_memory=90e6)
    # The size of dataset is 500*(80*80*4)*8B, about 100MB.
    ds = (
        ray.data.range_tensor(500, shape=(80, 80, 4), override_num_blocks=100)
        .materialize()
        .map_batches(lambda x: x)
        .materialize()
    )

    with pytest.raises(AssertionError):
        check_no_spill(ctx, ds)

    assert ds._get_stats_summary().global_bytes_spilled > 0
    assert ds._get_stats_summary().global_bytes_restored > 0

    assert "Spilled to disk:" in ds.stats()


def test_no_global_bytes_spilled(shutdown_only):
    # The object store is about 200MB.
    ctx = ray.init(object_store_memory=200e6)
    # The size of dataset is 500*(80*80*4)*8B, about 100MB.
    ds = ray.data.range_tensor(
        500, shape=(80, 80, 4), override_num_blocks=100
    ).materialize()

    check_no_spill(ctx, ds)
    assert ds._get_stats_summary().global_bytes_spilled == 0
    assert ds._get_stats_summary().global_bytes_restored == 0

    assert "Cluster memory:" not in ds.stats()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
