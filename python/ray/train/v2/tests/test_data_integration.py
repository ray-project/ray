import asyncio
from unittest.mock import MagicMock

import pytest

import ray.data
import ray.train
from ray.data import DataContext, ExecutionResources
from ray.data._internal.iterator.stream_split_iterator import StreamSplitDataIterator
from ray.data.tests.conftest import restore_data_context  # noqa: F401
from ray.train.v2._internal.callbacks.datasets import (
    DatasetManager,
    DatasetShardMetadata,
    DatasetsSetupCallback,
)
from ray.train.v2._internal.execution.context import TrainRunContext
from ray.train.v2._internal.execution.worker_group.worker_group import (
    WorkerGroupContext,
)
from ray.train.v2.api.data_parallel_trainer import DataParallelTrainer
from ray.train.v2.tests.util import DummyObjectRefWrapper, DummyWorkerGroup

# TODO(justinvyu): Bring over more tests from ray/air/tests/test_new_dataset_config.py


def test_e2e_single_dataset(ray_start_4_cpus, restore_data_context):  # noqa: F811
    """
    Test passing a Ray Dataset to the trainer and check the automatic dataset sharding.
    """
    NUM_ROWS = 1000
    NUM_TRAIN_WORKERS = 2

    # Test propagating DataContext to the Train workers.
    data_context = DataContext.get_current()
    data_context.set_config("foo", "bar")

    train_ds = ray.data.range(NUM_ROWS)

    def train_fn():
        data_context = DataContext.get_current()
        assert data_context.get_config("foo") == "bar"

        try:
            ray.train.get_dataset_shard("val")
            assert False, "Should raise an error if the dataset is not found"
        except KeyError:
            pass

        train_ds = ray.train.get_dataset_shard("train")
        num_rows = 0
        for batch in train_ds.iter_batches():
            num_rows += len(batch["id"])
        assert num_rows == NUM_ROWS // NUM_TRAIN_WORKERS

    trainer = DataParallelTrainer(
        train_fn,
        datasets={"train": train_ds},
        scaling_config=ray.train.ScalingConfig(num_workers=NUM_TRAIN_WORKERS),
    )
    trainer.fit()
    result = trainer.fit()
    assert not result.error


def test_dataset_setup_callback(ray_start_4_cpus):
    """Check that the `DatasetsSetupCallback` correctly configures the
    dataset shards and execution options."""
    NUM_WORKERS = 2

    train_ds = ray.data.range(1000)
    valid_ds = ray.data.range(1000)

    data_config = ray.train.DataConfig(datasets_to_split=["train"])
    scaling_config = ray.train.ScalingConfig(
        num_workers=NUM_WORKERS, use_gpu=True, resources_per_worker={"CPU": 1, "GPU": 1}
    )

    worker_group_context = WorkerGroupContext(
        run_attempt_id="attempt_1",
        train_fn_ref=DummyObjectRefWrapper(lambda: None),
        num_workers=scaling_config.num_workers,
        resources_per_worker=scaling_config.resources_per_worker,
    )
    worker_group = DummyWorkerGroup(
        train_run_context=MagicMock(spec=TrainRunContext),
        worker_group_context=worker_group_context,
    )
    worker_group._start()

    callback = DatasetsSetupCallback(
        datasets={"train": train_ds, "valid": valid_ds},
        data_config=data_config,
        scaling_config=scaling_config,
    )
    dataset_manager_for_each_worker = callback.before_init_train_context(
        worker_group.get_workers()
    )["dataset_manager"]
    assert len(dataset_manager_for_each_worker) == NUM_WORKERS

    # We should send the same dataset manager to all workers.
    dataset_manager = dataset_manager_for_each_worker[0]
    assert all(
        manager == dataset_manager for manager in dataset_manager_for_each_worker
    )

    def get_rank_0_shard(dataset_name: str):
        for i in range(1, NUM_WORKERS):
            dataset_manager.get_dataset_shard.remote(
                DatasetShardMetadata(dataset_name=dataset_name, world_rank=i)
            )

        return ray.get(
            dataset_manager.get_dataset_shard.remote(
                DatasetShardMetadata(dataset_name=dataset_name, world_rank=0)
            )
        )

    processed_train_ds = get_rank_0_shard("train")
    processed_valid_ds = get_rank_0_shard("valid")

    assert isinstance(processed_train_ds, StreamSplitDataIterator)
    assert not isinstance(processed_valid_ds, StreamSplitDataIterator)

    # The callback should have excluded the resources reserved for training.
    assert (
        processed_train_ds._base_dataset.context.execution_options.exclude_resources
        == ExecutionResources(cpu=NUM_WORKERS, gpu=NUM_WORKERS)
    )
    assert (
        processed_valid_ds._base_dataset.context.execution_options.exclude_resources
        == ExecutionResources(cpu=NUM_WORKERS, gpu=NUM_WORKERS)
    )


async def get_dataset_shard_for_worker(
    dataset_manager: DatasetManager,
    dataset_name: str,
    num_workers: int,
    worker_rank: int,
):
    return await asyncio.create_task(
        dataset_manager.get_dataset_shard(
            DatasetShardMetadata(dataset_name=dataset_name, world_rank=worker_rank)
        )
    )


async def get_dataset_shard_for_all_workers(
    dataset_manager: DatasetManager,
    dataset_name: str,
    num_workers: int,
):
    return await asyncio.gather(
        *[
            get_dataset_shard_for_worker(dataset_manager, dataset_name, num_workers, i)
            for i in range(num_workers)
        ]
    )


@pytest.mark.asyncio
async def test_get_multiple_datasets_serially(ray_start_4_cpus):
    """Tests DatasetManager.get_dataset_shard for multiple datasets,
    called serially by each worker. This is the typical case.

    Workers 0, 1:
    ray.train.get_dataset_shard("sharded_1")
    ray.train.get_dataset_shard("sharded_2")
    ray.train.get_dataset_shard("unsharded")
    """

    NUM_ROWS = 100
    NUM_TRAIN_WORKERS = 2

    sharded_ds_1 = ray.data.range(NUM_ROWS)
    sharded_ds_2 = ray.data.range(NUM_ROWS)
    unsharded_ds = ray.data.range(NUM_ROWS)

    dataset_manager = DatasetManager(
        datasets={
            "sharded_1": sharded_ds_1,
            "sharded_2": sharded_ds_2,
            "unsharded": unsharded_ds,
        },
        data_config=ray.train.DataConfig(datasets_to_split=["sharded_1", "sharded_2"]),
        data_context=DataContext.get_current(),
        world_size=NUM_TRAIN_WORKERS,
        worker_node_ids=None,
    )

    shards = await get_dataset_shard_for_all_workers(
        dataset_manager, "sharded_1", NUM_TRAIN_WORKERS
    )
    assert all(isinstance(shard, StreamSplitDataIterator) for shard in shards)
    assert [shard._base_dataset.name for shard in shards] == [
        "sharded_1"
    ] * NUM_TRAIN_WORKERS

    shards = await get_dataset_shard_for_all_workers(
        dataset_manager, "sharded_2", NUM_TRAIN_WORKERS
    )
    assert all(isinstance(shard, StreamSplitDataIterator) for shard in shards)
    assert [shard._base_dataset.name for shard in shards] == [
        "sharded_2"
    ] * NUM_TRAIN_WORKERS

    shards = await get_dataset_shard_for_all_workers(
        dataset_manager, "unsharded", NUM_TRAIN_WORKERS
    )
    assert not any(isinstance(shard, StreamSplitDataIterator) for shard in shards)
    assert [shard._base_dataset.name for shard in shards] == [
        "unsharded"
    ] * NUM_TRAIN_WORKERS


@pytest.mark.asyncio
async def test_get_multiple_datasets_interleaved(ray_start_4_cpus):
    """Tests DatasetManager.get_dataset_shard for multiple datasets,
    called in an interleaved order by workers.

    Worker 0:
    ray.train.get_dataset_shard("train")
    ray.train.get_dataset_shard("valid")

    Worker 1:
    ray.train.get_dataset_shard("valid")
    ray.train.get_dataset_shard("train")
    """

    NUM_ROWS = 100
    NUM_TRAIN_WORKERS = 2

    train_ds = ray.data.range(NUM_ROWS)
    valid_ds = ray.data.range(NUM_ROWS)

    dataset_manager = DatasetManager(
        datasets={"train": train_ds, "valid": valid_ds},
        data_config=ray.train.DataConfig(datasets_to_split="all"),
        data_context=DataContext.get_current(),
        world_size=NUM_TRAIN_WORKERS,
        worker_node_ids=None,
    )

    tasks = [
        get_dataset_shard_for_worker(dataset_manager, "train", NUM_TRAIN_WORKERS, 0),
        get_dataset_shard_for_worker(dataset_manager, "valid", NUM_TRAIN_WORKERS, 1),
        get_dataset_shard_for_worker(dataset_manager, "train", NUM_TRAIN_WORKERS, 1),
        get_dataset_shard_for_worker(dataset_manager, "valid", NUM_TRAIN_WORKERS, 0),
    ]
    iterators = await asyncio.gather(*tasks)
    assert all(isinstance(iterator, StreamSplitDataIterator) for iterator in iterators)
    assert [iterator._base_dataset.name for iterator in iterators] == [
        "train",
        "valid",
        "train",
        "valid",
    ]


@pytest.mark.asyncio
async def test_get_multiple_datasets_rank_specific(ray_start_4_cpus):
    """Tests rank-specific DatasetManager.get_dataset_shard calls.

    # Epoch 1
    ray.train.get_dataset_shard("train")

    # Validation, which only happens on worker 0.
    if world_rank == 0:
        ray.train.get_dataset_shard("valid")

    # Epoch 2
    ray.train.get_dataset_shard("train")
    """

    NUM_ROWS = 100
    NUM_TRAIN_WORKERS = 2

    train_ds = ray.data.range(NUM_ROWS)
    valid_ds = ray.data.range(NUM_ROWS)

    dataset_manager = DatasetManager(
        datasets={"train": train_ds, "valid": valid_ds},
        data_config=ray.train.DataConfig(datasets_to_split=["train"]),
        data_context=DataContext.get_current(),
        world_size=NUM_TRAIN_WORKERS,
        worker_node_ids=None,
    )

    # ray.train.get_dataset_shard("train")
    iterators = await get_dataset_shard_for_all_workers(
        dataset_manager, "train", NUM_TRAIN_WORKERS
    )
    assert all(isinstance(iterator, StreamSplitDataIterator) for iterator in iterators)
    assert [iterator._base_dataset.name for iterator in iterators] == [
        "train"
    ] * NUM_TRAIN_WORKERS

    # if world_rank == 0:
    #     ray.train.get_dataset_shard("valid")
    iterator = await get_dataset_shard_for_worker(
        dataset_manager, "valid", NUM_TRAIN_WORKERS, 0
    )
    assert not isinstance(iterator, StreamSplitDataIterator)
    assert iterator._base_dataset.name == "valid"

    # ray.train.get_dataset_shard("train")
    iterators = await get_dataset_shard_for_all_workers(
        dataset_manager, "train", NUM_TRAIN_WORKERS
    )
    assert all(isinstance(iterator, StreamSplitDataIterator) for iterator in iterators)
    assert [iterator._base_dataset.name for iterator in iterators] == [
        "train"
    ] * NUM_TRAIN_WORKERS


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
