import asyncio
from unittest.mock import MagicMock

import pytest

import ray
import ray.data
from ray.data import DataContext
from ray.data._internal.iterator.stream_split_iterator import StreamSplitDataIterator
from ray.train.v2._internal.data_integration.dataset_manager import (
    DatasetManager,
    DatasetShardMetadata,
)


async def get_dataset_shard_for_worker(
    dataset_manager: DatasetManager,
    dataset_name: str,
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
            get_dataset_shard_for_worker(dataset_manager, dataset_name, i)
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
    assert all(shard._get_dataset_tag().startswith("sharded_1_") for shard in shards)

    shards = await get_dataset_shard_for_all_workers(
        dataset_manager, "sharded_2", NUM_TRAIN_WORKERS
    )
    assert all(isinstance(shard, StreamSplitDataIterator) for shard in shards)
    assert all(shard._get_dataset_tag().startswith("sharded_2_") for shard in shards)

    shards = await get_dataset_shard_for_all_workers(
        dataset_manager, "unsharded", NUM_TRAIN_WORKERS
    )
    assert not any(isinstance(shard, StreamSplitDataIterator) for shard in shards)
    assert all(shard._get_dataset_tag().startswith("unsharded_") for shard in shards)


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
        get_dataset_shard_for_worker(dataset_manager, "train", 0),
        get_dataset_shard_for_worker(dataset_manager, "valid", 1),
        get_dataset_shard_for_worker(dataset_manager, "train", 1),
        get_dataset_shard_for_worker(dataset_manager, "valid", 0),
    ]
    iterators = await asyncio.gather(*tasks)
    assert all(isinstance(iterator, StreamSplitDataIterator) for iterator in iterators)
    expected_names = ["train", "valid", "train", "valid"]
    assert all(
        it._get_dataset_tag().startswith(f"{name}_")
        for it, name in zip(iterators, expected_names)
    )


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
    assert all(it._get_dataset_tag().startswith("train_") for it in iterators)

    # if world_rank == 0:
    #     ray.train.get_dataset_shard("valid")
    iterator = await get_dataset_shard_for_worker(dataset_manager, "valid", 0)
    assert not isinstance(iterator, StreamSplitDataIterator)
    assert iterator._get_dataset_tag().startswith("valid_")

    # ray.train.get_dataset_shard("train")
    iterators = await get_dataset_shard_for_all_workers(
        dataset_manager, "train", NUM_TRAIN_WORKERS
    )
    assert all(isinstance(iterator, StreamSplitDataIterator) for iterator in iterators)
    assert all(it._get_dataset_tag().startswith("train_") for it in iterators)


@pytest.mark.asyncio
async def test_dataset_manager_shutdown_multiple_datasets(ray_start_4_cpus):
    """The DatasetManager collects SplitCoordinator actors for sharded datasets
    and triggers executor shutdown on them.
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

    await get_dataset_shard_for_all_workers(
        dataset_manager, "sharded_1", NUM_TRAIN_WORKERS
    )
    assert len(dataset_manager._coordinator_actors) == 1
    assert isinstance(dataset_manager._coordinator_actors[0], ray.actor.ActorHandle)

    await get_dataset_shard_for_all_workers(
        dataset_manager, "sharded_2", NUM_TRAIN_WORKERS
    )
    assert len(dataset_manager._coordinator_actors) == 2
    assert isinstance(dataset_manager._coordinator_actors[1], ray.actor.ActorHandle)

    # Unsharded datasets are not tracked for shutdown.
    await get_dataset_shard_for_all_workers(
        dataset_manager, "unsharded", NUM_TRAIN_WORKERS
    )
    assert len(dataset_manager._coordinator_actors) == 2

    mocks = [MagicMock() for _ in range(2)]
    remote_mocks = [mock.shutdown_executor.remote for mock in mocks]
    dataset_manager._coordinator_actors = mocks

    dataset_manager.shutdown_data_executors()

    for remote_mock in remote_mocks:
        remote_mock.assert_called_once()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
