from unittest.mock import MagicMock

import pytest

import ray.data
import ray.train
from ray.data import DataContext, ExecutionOptions, ExecutionResources
from ray.data._internal.iterator.stream_split_iterator import StreamSplitDataIterator
from ray.data.tests.conftest import restore_data_context  # noqa: F401
from ray.train.v2._internal.callbacks.datasets import DatasetsSetupCallback
from ray.train.v2._internal.data_integration.interfaces import DatasetShardMetadata
from ray.train.v2._internal.execution.worker_group.worker_group import (
    WorkerGroupContext,
)
from ray.train.v2.api.data_parallel_trainer import DataParallelTrainer
from ray.train.v2.tests.util import (
    DummyObjectRefWrapper,
    DummyWorkerGroup,
    create_dummy_run_context,
)


@pytest.mark.parametrize("num_workers", [1, 2])
def test_dataset_sharding_across_workers(ray_start_4_cpus, num_workers):
    """Tests that the dataset shards properly across a variety of num_workers."""
    NUM_ROWS = 1000

    train_ds = ray.data.range(NUM_ROWS)

    def train_fn():
        with pytest.raises(KeyError):
            ray.train.get_dataset_shard("val")

        train_ds = ray.train.get_dataset_shard("train")
        num_rows = 0
        for batch in train_ds.iter_batches():
            num_rows += len(batch["id"])
        assert num_rows == NUM_ROWS // num_workers

    trainer = DataParallelTrainer(
        train_fn,
        datasets={"train": train_ds},
        scaling_config=ray.train.ScalingConfig(num_workers=num_workers),
    )
    trainer.fit()


@pytest.mark.parametrize("datasets_to_split", ["all", ["train"], []])
def test_multiple_datasets(ray_start_4_cpus, datasets_to_split):
    """Tests that the dataset is sharded across a variety of num_workers."""
    NUM_ROWS = 1000
    NUM_WORKERS = 2

    train_ds = ray.data.range(NUM_ROWS)
    val_ds = ray.data.range(NUM_ROWS)

    def train_fn():
        for dataset_name in ["train", "val"]:
            ds = ray.train.get_dataset_shard(dataset_name)
            num_rows = 0
            for batch in ds.iter_batches():
                num_rows += len(batch["id"])

            if datasets_to_split == "all" or dataset_name in datasets_to_split:
                assert num_rows == NUM_ROWS // NUM_WORKERS
            else:
                assert num_rows == NUM_ROWS

    trainer = DataParallelTrainer(
        train_fn,
        datasets={"train": train_ds, "val": val_ds},
        dataset_config=ray.train.DataConfig(datasets_to_split=datasets_to_split),
        scaling_config=ray.train.ScalingConfig(num_workers=NUM_WORKERS),
    )
    trainer.fit()


def test_data_config_validation():
    with pytest.raises(TypeError, match="`datasets_to_split` should be.*"):
        ray.train.DataConfig(datasets_to_split="hello")
    with pytest.raises(TypeError, match="`datasets_to_split` should be.*"):
        ray.train.DataConfig(datasets_to_split={})


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
    train_run_context = create_dummy_run_context(
        datasets={"train": train_ds, "valid": valid_ds},
        dataset_config=data_config,
        scaling_config=scaling_config,
    )
    worker_group = DummyWorkerGroup(
        train_run_context=train_run_context,
        worker_group_context=worker_group_context,
    )
    worker_group._start()

    callback = DatasetsSetupCallback(train_run_context)
    dataset_manager_for_each_worker = callback.before_init_train_context(
        worker_group.get_workers()
    )["dataset_shard_provider"]
    assert len(dataset_manager_for_each_worker) == NUM_WORKERS

    dataset_manager = dataset_manager_for_each_worker[0]
    processed_train_ds = dataset_manager.get_dataset_shard(
        DatasetShardMetadata(dataset_name="train")
    )
    processed_valid_ds = dataset_manager.get_dataset_shard(
        DatasetShardMetadata(dataset_name="valid")
    )

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


def test_data_context_propagation(ray_start_4_cpus, restore_data_context):  # noqa: F811
    """Tests that the DataContext from the driver is propagated to the Train workers."""
    data_context = DataContext.get_current()
    data_context.set_config("foo", "bar")
    train_ds = ray.data.range(2)

    def train_fn():
        assert DataContext.get_current().get_config("foo") == "bar"

    trainer = DataParallelTrainer(
        train_fn,
        datasets={"train": train_ds},
        scaling_config=ray.train.ScalingConfig(num_workers=2),
    )
    trainer.fit()


def test_configure_execution_options_carryover_context():
    """Tests that execution options in DataContext
    carry over to DataConfig automatically."""

    ctx = ray.data.DataContext.get_current()
    ctx.execution_options.preserve_order = True
    ctx.execution_options.verbose_progress = True

    data_config = ray.train.DataConfig()

    ingest_options = data_config.default_ingest_options()
    assert ingest_options.preserve_order is True
    assert ingest_options.verbose_progress is True


@pytest.mark.parametrize("enable_shard_locality", [True, False])
def test_configure_locality(enable_shard_locality):
    data_config = ray.train.DataConfig(enable_shard_locality=enable_shard_locality)

    mock_ds = MagicMock()
    mock_ds.streaming_split = MagicMock()
    mock_ds.copy = MagicMock(return_value=mock_ds)
    world_size = 2
    worker_handles = [MagicMock() for _ in range(world_size)]
    worker_node_ids = ["node" + str(i) for i in range(world_size)]
    data_config.configure(
        datasets={"train": mock_ds},
        world_size=world_size,
        worker_handles=worker_handles,
        worker_node_ids=worker_node_ids,
    )
    mock_ds.streaming_split.assert_called_once()
    mock_ds.streaming_split.assert_called_with(
        world_size,
        equal=True,
        locality_hints=worker_node_ids if enable_shard_locality else None,
    )


@pytest.mark.parametrize("cache_random_preprocessing", [True, False])
def test_per_epoch_preprocessing(ray_start_4_cpus, cache_random_preprocessing):
    """Random preprocessing should change per-epoch."""
    NUM_ROWS = 32
    NUM_WORKERS = 2

    ds = ray.data.range(NUM_ROWS, override_num_blocks=NUM_ROWS).random_shuffle()
    if cache_random_preprocessing:
        # Materialize the dataset to cache the random preprocessing.
        # In this case, every epoch should use the same random preprocessing.
        ds = ds.materialize()

    def train_fn():
        ds = ray.train.get_dataset_shard("train")
        epoch_0 = [row["id"] for row in ds.iter_rows()]
        epoch_1 = [row["id"] for row in ds.iter_rows()]

        assert len(epoch_0) == len(epoch_1) == NUM_ROWS // NUM_WORKERS
        if cache_random_preprocessing:
            assert epoch_0 == epoch_1
        else:
            assert epoch_0 != epoch_1, (epoch_0, epoch_1)

    trainer = DataParallelTrainer(
        train_fn,
        datasets={"train": ds},
        scaling_config=ray.train.ScalingConfig(num_workers=NUM_WORKERS),
    )
    trainer.fit()


@pytest.mark.parametrize("exclude_resources", [None, ExecutionResources(cpu=2, gpu=1)])
def test_data_config_exclude_resources(ray_start_4_cpus, exclude_resources):
    execution_options = ExecutionOptions(exclude_resources=exclude_resources)
    data_config = ray.train.DataConfig(execution_options=execution_options)

    NUM_WORKERS = 2

    def check_exclude_resources(config):
        ds = ray.train.get_dataset_shard("train")
        exclude_resources = config.get("exclude_resources") or ExecutionResources.zero()

        # Ray Data always excludes resources reserved by Ray Train workers.
        expected_exclude_resources = exclude_resources.add(
            ExecutionResources(cpu=NUM_WORKERS)
        )
        assert (
            ds.get_context().execution_options.exclude_resources
            == expected_exclude_resources
        )

    ds = ray.data.range(1)
    trainer = DataParallelTrainer(
        check_exclude_resources,
        train_loop_config={"exclude_resources": exclude_resources},
        datasets={"train": ds},
        dataset_config=data_config,
        scaling_config=ray.train.ScalingConfig(num_workers=NUM_WORKERS),
    )
    trainer.fit()


@pytest.mark.parametrize(
    "resource_limits", [None, ExecutionResources.for_limits(cpu=2, gpu=1)]
)
def test_data_config_resource_limits(ray_start_4_cpus, resource_limits):
    execution_options = ExecutionOptions(resource_limits=resource_limits)
    data_config = ray.train.DataConfig(execution_options=execution_options)

    NUM_WORKERS = 2

    def check_resource_limits(config):
        ds = ray.train.get_dataset_shard("train")
        resource_limits = (
            config.get("resource_limits") or ExecutionResources.for_limits()
        )
        assert ds.get_context().execution_options.resource_limits == resource_limits

        if not ds.get_context().execution_options.is_resource_limits_default():
            # Don't exclude train worker resources if the user already
            # set the resource_limits.
            assert (
                ds.get_context().execution_options.exclude_resources
                == ExecutionResources.zero()
            )

    ds = ray.data.range(1)
    trainer = DataParallelTrainer(
        check_resource_limits,
        train_loop_config={"resource_limits": resource_limits},
        datasets={"train": ds},
        dataset_config=data_config,
        scaling_config=ray.train.ScalingConfig(num_workers=NUM_WORKERS),
    )
    trainer.fit()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
