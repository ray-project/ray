import pytest

import ray
from ray._private.internal_api import memory_summary
from ray.tests.conftest import *  # noqa


def check_no_spill(ctx, pipe):
    # Run up to 10 epochs of the pipeline to stress test that
    # no spilling will happen.
    max_epoch = 10
    for p in pipe.iter_epochs(max_epoch):
        for _ in p.iter_batches(batch_size=None):
            pass
    meminfo = memory_summary(ctx.address_info["address"], stats_only=True)
    assert "Spilled" not in meminfo, meminfo


def check_to_torch_no_spill(ctx, pipe):
    # Run up to 10 epochs of the pipeline to stress test that
    # no spilling will happen.
    max_epoch = 10
    for p in pipe.iter_epochs(max_epoch):
        for _ in p.to_torch(batch_size=None):
            pass
    meminfo = memory_summary(ctx.address_info["address"], stats_only=True)
    assert "Spilled" not in meminfo, meminfo


def check_iter_torch_batches_no_spill(ctx, pipe):
    # Run up to 10 epochs of the pipeline to stress test that
    # no spilling will happen.
    max_epoch = 10
    for p in pipe.iter_epochs(max_epoch):
        for _ in p.iter_torch_batches(batch_size=None):
            pass
    meminfo = memory_summary(ctx.address_info["address"], stats_only=True)
    assert "Spilled" not in meminfo, meminfo


def check_to_tf_no_spill(ctx, pipe):
    # Run up to 10 epochs of the pipeline to stress test that
    # no spilling will happen.
    max_epoch = 10
    for p in pipe.iter_epochs(max_epoch):
        for _ in p.to_tf(
            feature_columns="data", label_columns="label", batch_size=None
        ):
            pass
    meminfo = memory_summary(ctx.address_info["address"], stats_only=True)
    assert "Spilled" not in meminfo, meminfo


def check_iter_tf_batches_no_spill(ctx, pipe):
    # Run up to 10 epochs of the pipeline to stress test that
    # no spilling will happen.
    max_epoch = 10
    for p in pipe.iter_epochs(max_epoch):
        for _ in p.iter_tf_batches():
            pass
    meminfo = memory_summary(ctx.address_info["address"], stats_only=True)
    assert "Spilled" not in meminfo, meminfo


def test_iter_batches_no_spilling_upon_no_transformation(shutdown_only):
    # The object store is about 300MB.
    ctx = ray.init(num_cpus=1, object_store_memory=300e6)
    # The size of dataset is 500*(80*80*4)*8B, about 100MB.
    ds = ray.data.range_tensor(500, shape=(80, 80, 4), parallelism=100)

    check_no_spill(ctx, ds.repeat())
    check_no_spill(ctx, ds.window(blocks_per_window=20))


def test_torch_iteration(shutdown_only):
    # The object store is about 400MB.
    ctx = ray.init(num_cpus=1, object_store_memory=400e6)
    # The size of dataset is 500*(80*80*4)*8B, about 100MB.
    ds = ray.data.range_tensor(500, shape=(80, 80, 4), parallelism=100)

    # to_torch
    check_to_torch_no_spill(ctx, ds.repeat())
    check_to_torch_no_spill(ctx, ds.window(blocks_per_window=20))
    # iter_torch_batches
    check_iter_torch_batches_no_spill(ctx, ds.repeat())
    check_iter_torch_batches_no_spill(ctx, ds.window(blocks_per_window=20))


def test_tf_iteration(shutdown_only):
    # The object store is about 800MB.
    ctx = ray.init(num_cpus=1, object_store_memory=800e6)
    # The size of dataset is 500*(80*80*4)*8B, about 100MB.
    ds = ray.data.range_tensor(500, shape=(80, 80, 4), parallelism=100).add_column(
        "label", lambda x: 1
    )

    # to_tf
    check_to_tf_no_spill(ctx, ds.repeat().map(lambda x: x))
    check_to_tf_no_spill(ctx, ds.window(blocks_per_window=20).map(lambda x: x))
    # iter_tf_batches
    check_iter_tf_batches_no_spill(ctx, ds.repeat().map(lambda x: x))
    check_iter_tf_batches_no_spill(
        ctx, ds.window(blocks_per_window=20).map(lambda x: x)
    )


def test_iter_batches_no_spilling_upon_rewindow(shutdown_only):
    # The object store is about 300MB.
    ctx = ray.init(num_cpus=1, object_store_memory=300e6)
    # The size of dataset is 500*(80*80*4)*8B, about 100MB.
    ds = ray.data.range_tensor(500, shape=(80, 80, 4), parallelism=100)

    check_no_spill(
        ctx, ds.window(blocks_per_window=20).repeat().rewindow(blocks_per_window=10)
    )


def test_iter_batches_no_spilling_upon_prior_transformation(shutdown_only):
    # The object store is about 500MB.
    ctx = ray.init(num_cpus=1, object_store_memory=500e6)
    # The size of dataset is 500*(80*80*4)*8B, about 100MB.
    ds = ray.data.range_tensor(500, shape=(80, 80, 4), parallelism=100)

    # Repeat, with transformation prior to the pipeline.
    check_no_spill(ctx, ds.map_batches(lambda x: x).repeat())
    # Window, with transformation prior to the pipeline.
    check_no_spill(ctx, ds.map_batches(lambda x: x).window(blocks_per_window=20))


def test_iter_batches_no_spilling_upon_post_transformation(shutdown_only):
    # The object store is about 500MB.
    ctx = ray.init(num_cpus=1, object_store_memory=500e6)
    # The size of dataset is 500*(80*80*4)*8B, about 100MB.
    ds = ray.data.range_tensor(500, shape=(80, 80, 4), parallelism=100)

    # Repeat, with transformation post the pipeline creation.
    check_no_spill(ctx, ds.repeat().map_batches(lambda x: x, batch_size=5))
    # Window, with transformation post the pipeline creation.
    check_no_spill(ctx, ds.window(blocks_per_window=20).map_batches(lambda x: x))


def test_iter_batches_no_spilling_upon_transformations(shutdown_only):
    # The object store is about 700MB.
    ctx = ray.init(num_cpus=1, object_store_memory=700e6)
    # The size of dataset is 500*(80*80*4)*8B, about 100MB.
    ds = ray.data.range_tensor(500, shape=(80, 80, 4), parallelism=100)

    # Repeat, with transformation before and post the pipeline.
    check_no_spill(
        ctx,
        ds.map_batches(lambda x: x, batch_size=5)
        .repeat()
        .map_batches(lambda x: x, batch_size=5),
    )
    # Window, with transformation before and post the pipeline.
    check_no_spill(
        ctx,
        ds.map_batches(lambda x: x)
        .window(blocks_per_window=20)
        .map_batches(lambda x: x),
    )


def test_iter_batches_no_spilling_upon_shuffle(shutdown_only):
    # The object store is about 500MB.
    ctx = ray.init(num_cpus=1, object_store_memory=500e6)
    # The size of dataset is 500*(80*80*4)*8B, about 100MB.
    ds = ray.data.range_tensor(500, shape=(80, 80, 4), parallelism=100)

    check_no_spill(ctx, ds.repeat().random_shuffle_each_window())
    check_no_spill(ctx, ds.window(blocks_per_window=20).random_shuffle_each_window())


def test_pipeline_splitting_has_no_spilling(shutdown_only):
    # The object store is about 800MiB.
    ctx = ray.init(num_cpus=1, object_store_memory=1200e6)
    # The size of dataset is 50000*(80*80*4)*8B, about 10GiB, 50MiB/block.
    ds = ray.data.range_tensor(5000, shape=(80, 80, 4), parallelism=20)

    # 2 blocks/window.
    p = ds.window(bytes_per_window=100 * 1024 * 1024).repeat(2)
    p1, p2 = p.split(2)

    @ray.remote
    def consume(p):
        for batch in p.iter_batches(batch_size=None):
            pass
        print(p.stats())

    tasks = [consume.remote(p1), consume.remote(p2)]
    ray.get(tasks)
    meminfo = memory_summary(ctx.address_info["address"], stats_only=True)
    assert "Spilled" not in meminfo, meminfo


def test_pipeline_splitting_has_no_spilling_with_equal_splitting(shutdown_only):
    # The object store is about 1200MiB.
    ctx = ray.init(num_cpus=1, object_store_memory=1200e6)
    # The size of dataset is 50000*(80*80*4)*8B, about 10GiB, 50MiB/block.
    ds = ray.data.range_tensor(50000, shape=(80, 80, 4), parallelism=200)

    # 150Mib/window, which is 3 blocks/window, which means equal splitting
    # will need to split one block.
    p = ds.window(bytes_per_window=150 * 1024 * 1024).repeat()
    p1, p2 = p.split(2, equal=True)

    @ray.remote
    def consume(p):
        for batch in p.iter_batches():
            pass

    tasks = [consume.remote(p1), consume.remote(p2)]
    try:
        # Run it for 20 seconds.
        ray.get(tasks, timeout=20)
    except Exception:
        for t in tasks:
            ray.cancel(t, force=True)
    meminfo = memory_summary(ctx.address_info["address"], stats_only=True)
    assert "Spilled" not in meminfo, meminfo


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
