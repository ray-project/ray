import pytest
from unittest.mock import MagicMock

import ray.data
import ray.train
from ray.data import DataContext, ExecutionResources
from ray.data._internal.iterator.stream_split_iterator import StreamSplitDataIterator
from ray.data.tests.conftest import restore_data_context  # noqa: F401
from ray.train.v2._internal.callbacks import DatasetsSetupCallback
from ray.train.v2._internal.execution.context import TrainRunContext
from ray.train.v2._internal.execution.worker_group.worker_group import (
    WorkerGroupContext,
)
from ray.train.v2.api.data_parallel_trainer import DataParallelTrainer
from ray.train.v2.tests.test_controller import DummyWorkerGroup

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
        train_fn=lambda: None,
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
    dataset_shards = callback.before_init_train_context(worker_group.get_workers())[
        "dataset_shards"
    ]
    assert len(dataset_shards) == NUM_WORKERS

    processed_train_ds = dataset_shards[0]["train"]
    processed_valid_ds = dataset_shards[0]["valid"]

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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
