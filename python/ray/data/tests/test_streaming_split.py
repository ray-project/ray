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


def test_streaming_split_iter_locality_hints():
    """Check that the `DatasetsSetupCallback` correctly configures the
    dataset shards and execution options."""
    NUM_TRAINING_WORKERS = 2

    train_ds = ray.data.range(100)
    valid_ds = ray.data.range(100)

    data_config = ray.train.DataConfig(datasets_to_split="all")
    data_config._execution_options.locality_with_output = True
    data_config._execution_options.actor_locality_enabled = False

    scaling_config = ray.train.ScalingConfig(
        num_workers=NUM_TRAINING_WORKERS, use_gpu=True, resources_per_worker={"CPU": 1, "GPU": 1}
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
    assert len(dataset_shards) == NUM_TRAINING_WORKERS

    processed_train_ds = dataset_shards[0]["train"]
    processed_valid_ds = dataset_shards[0]["valid"]

    # Locality hints are set based on locality_with_output
    assert isinstance(processed_train_ds, StreamSplitDataIterator)
    assert len(processed_train_ds._locality_hints) is not None
    assert isinstance(processed_valid_ds, StreamSplitDataIterator)
    assert len(processed_valid_ds._locality_hints) is not None


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
