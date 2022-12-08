import os
import time
from unittest.mock import patch
import pytest

import ray
from ray import tune
from ray.air import session
from ray.air.checkpoint import Checkpoint
from ray.data.preprocessor import Preprocessor
from ray.train._internal.backend_executor import BackendExecutor
from ray.train._internal.worker_group import WorkerGroup
from ray.train.backend import Backend, BackendConfig
from ray.train.constants import PREPROCESSOR_KEY
from ray.train.data_parallel_trainer import DataParallelTrainer
from ray.air.config import ScalingConfig, CheckpointConfig, RunConfig
from ray.tune.tune_config import TuneConfig
from ray.tune.tuner import Tuner
from ray.tune.callback import Callback


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def ray_start_4_cpus_4_gpus_4_extra():
    address_info = ray.init(num_cpus=4, num_gpus=4, resources={"extra": 4})
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


# Currently in DataParallelTrainers we only report metrics from rank 0.
# For testing purposes here, we need to be able to report from all
# workers.


class DataParallelTrainerPatchedMultipleReturns(DataParallelTrainer):
    def _report(self, training_iterator) -> None:
        for results in training_iterator:
            tune.report(results=results)


def gen_execute_single_async_special(special_f):
    def execute_single_async_special(self, i, f, *args, **kwargs):
        assert len(self.workers) == 2
        if i == 0 and hasattr(self, "should_fail") and self.should_fail:
            kwargs["train_func"] = special_f
        return self.workers[i].actor._RayTrainWorker__execute.remote(f, *args, **kwargs)

    return execute_single_async_special


def gen_new_backend_executor(special_f):
    """Returns a BackendExecutor that runs special_f on worker 0 once."""

    class TestBackendExecutor(BackendExecutor):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self._has_failed = False

        def start_training(self, *args, **kwargs):
            special_execute = gen_execute_single_async_special(special_f)
            if not self._has_failed:
                self.worker_group.should_fail = True
                self._has_failed = True
            else:
                self.worker_group.should_fail = False
            with patch.object(WorkerGroup, "execute_single_async", special_execute):
                super().start_training(*args, **kwargs)

    return TestBackendExecutor


class CaptureReportCallback(Callback):
    def __init__(self):
        self.result_list = []

    def on_trial_result(self, iteration, trials, trial, result, **info):
        self.result_list.append(result)


scale_config = ScalingConfig(num_workers=2)


def test_fit_train(ray_start_4_cpus):
    def train_func():
        session.report({"loss": 1})

    trainer = DataParallelTrainer(
        train_loop_per_worker=train_func, scaling_config=scale_config
    )
    assert trainer.fit().metrics["loss"] == 1


def test_scaling_config(ray_start_4_cpus):
    def train_func():
        assert ray.available_resources()["CPU"] == 1
        session.report({"loss": 1})

    assert ray.available_resources()["CPU"] == 4
    trainer = DataParallelTrainer(
        train_loop_per_worker=train_func, scaling_config=ScalingConfig(num_workers=2)
    )
    trainer.fit()


def test_fit_train_config(ray_start_4_cpus):
    def train_func(config):
        session.report({"loss": config["x"]})

    trainer = DataParallelTrainer(
        train_loop_per_worker=train_func,
        scaling_config=scale_config,
        train_loop_config={"x": 100},
    )
    assert trainer.fit().metrics["loss"] == 100


def test_datasets(ray_start_4_cpus):
    num_train_data = 10
    num_val_data = 6

    train_dataset = ray.data.range(num_train_data)
    val_dataset = ray.data.range(num_val_data)

    def get_dataset():
        # Train dataset should be sharded.
        train_dataset = session.get_dataset_shard("train")
        assert train_dataset.count() == num_train_data / scale_config.num_workers
        # All other datasets should not be sharded.
        val_dataset = session.get_dataset_shard("val")
        assert val_dataset.count() == num_val_data

    trainer = DataParallelTrainer(
        train_loop_per_worker=get_dataset,
        scaling_config=scale_config,
        datasets={"train": train_dataset, "val": val_dataset},
    )
    trainer.fit()


def test_checkpoint(ray_start_4_cpus):
    def train_func():
        for i in range(3):
            session.report({"epoch": i}, checkpoint=Checkpoint.from_dict({"model": i}))

    trainer = DataParallelTrainer(
        train_loop_per_worker=train_func, scaling_config=scale_config
    )
    result = trainer.fit()
    assert result.checkpoint.to_dict()["model"] == 2


def test_preprocessor_in_checkpoint(ray_start_4_cpus):
    class DummyPreprocessor(Preprocessor):
        def __init__(self):
            super().__init__()
            self.is_same = True

    def train_func():
        for i in range(3):
            session.report({"epoch": i}, checkpoint=Checkpoint.from_dict({"model": i}))

    trainer = DataParallelTrainer(
        train_loop_per_worker=train_func,
        scaling_config=scale_config,
        preprocessor=DummyPreprocessor(),
    )
    result = trainer.fit()
    assert result.checkpoint.to_dict()["model"] == 2
    assert result.checkpoint.to_dict()[PREPROCESSOR_KEY].is_same


def test_resume_from_checkpoint(ray_start_4_cpus, tmpdir):
    def train_func():
        checkpoint = session.get_checkpoint()
        if checkpoint:
            epoch = checkpoint.to_dict()["epoch"]
        else:
            epoch = 0
        for i in range(epoch, epoch + 2):
            session.report({"epoch": i}, checkpoint=Checkpoint.from_dict({"epoch": i}))

    trainer = DataParallelTrainer(
        train_loop_per_worker=train_func, scaling_config=scale_config
    )
    result = trainer.fit()
    assert result.checkpoint.to_dict()["epoch"] == 1

    # Move checkpoint to a different directory.
    checkpoint_dict = result.checkpoint.to_dict()
    checkpoint = Checkpoint.from_dict(checkpoint_dict)
    checkpoint_path = checkpoint.to_directory(tmpdir)
    resume_from = Checkpoint.from_directory(checkpoint_path)

    trainer = DataParallelTrainer(
        train_loop_per_worker=train_func,
        scaling_config=scale_config,
        resume_from_checkpoint=resume_from,
    )
    result = trainer.fit()
    assert result.checkpoint.to_dict()["epoch"] == 2


def test_invalid_train_loop(ray_start_4_cpus):
    def train_loop(config, extra_arg):
        pass

    with pytest.raises(ValueError):
        DataParallelTrainer(train_loop_per_worker=train_loop)


def test_bad_return_in_train_loop(ray_start_4_cpus):
    """Test to check if returns from train loop are discarded."""

    # Simulates what happens with eg. torch models
    class FailOnUnpickle:
        def __reduce__(self):
            raise RuntimeError("Failing")

    def train_loop(config):
        session.report({"loss": 1})
        return FailOnUnpickle()

    trainer = DataParallelTrainer(
        train_loop_per_worker=train_loop, scaling_config=scale_config
    )

    # No exception should happen here
    trainer.fit()


def test_tune(ray_start_4_cpus):
    def train_func(config):
        session.report({"loss": config["x"]})

    trainer = DataParallelTrainer(
        train_loop_per_worker=train_func,
        train_loop_config={"x": 100},
        scaling_config=scale_config,
    )

    tuner = Tuner(
        trainer,
        param_space={"train_loop_config": {"x": tune.choice([200, 300])}},
        tune_config=TuneConfig(num_samples=2),
    )
    result_grid = tuner.fit()
    assert result_grid[0].metrics["loss"] in [200, 300]

    # Make sure original Trainer is not affected.
    assert trainer._train_loop_config["x"] == 100


def test_scaling_config_validation(ray_start_4_cpus):
    def train_func(config):
        session.report({"loss": config["x"]})

    # Should be able to create a DataParallelTrainer w/o scaling_config,
    # but it should fail on fit
    trainer = DataParallelTrainer(
        train_loop_per_worker=train_func,
        train_loop_config={"x": 100},
    )
    with pytest.raises(ValueError):
        trainer.fit()

    # Scaling config must be passed in through Tuner param space if not
    # included in the initial trainer
    tuner = Tuner(trainer)
    with pytest.raises(ValueError):
        tuner.fit()

    tuner = Tuner(trainer, param_space={"scaling_config": ScalingConfig(num_workers=1)})
    results = tuner.fit()
    assert not results.errors


def test_fast_slow(ray_start_4_cpus):
    def train_func():
        for i in range(2):
            session.report(dict(index=i), checkpoint=Checkpoint.from_dict({"epoch": i}))

    def train_slow():
        for i in range(2):
            session.report(dict(index=i), checkpoint=Checkpoint.from_dict({"epoch": i}))
            time.sleep(5)

    new_backend_executor_cls = gen_new_backend_executor(train_slow)
    callback = CaptureReportCallback()

    class DataParallelTrainerPatched(DataParallelTrainer):
        _backend_executor_cls = new_backend_executor_cls

    trainer = DataParallelTrainerPatched(
        train_func,
        scaling_config=scale_config,
        run_config=RunConfig(callbacks=[callback]),
    )
    results = trainer.fit()

    assert results.checkpoint.to_dict()["epoch"] == 1

    result_list = callback.result_list
    assert len(result_list) == 2


def test_mismatch_report(ray_start_4_cpus):
    def train_func():
        for _ in range(2):
            session.report(dict(loss=1))

    def train_mismatch():
        session.report(dict(loss=1))

    new_backend_executor_cls = gen_new_backend_executor(train_mismatch)

    class DataParallelTrainerPatched(DataParallelTrainer):
        _backend_executor_cls = new_backend_executor_cls

    trainer = DataParallelTrainerPatched(
        train_func,
        scaling_config=scale_config,
    )
    with pytest.raises(RuntimeError):
        trainer.fit()


def test_world_rank(ray_start_4_cpus):
    def train_func():
        session.report(dict(world_rank=session.get_world_rank()))

    # Currently in DataParallelTrainers we only report metrics from rank 0.
    # For testing purposes here, we need to be able to report from all
    # workers.
    class DataParallelTrainerPatched(DataParallelTrainer):
        def _report(self, training_iterator) -> None:
            for results in training_iterator:
                tune.report(results=results)

    trainer = DataParallelTrainerPatched(
        train_func,
        scaling_config=scale_config,
    )
    results = trainer.fit()

    assert [result["world_rank"] for result in results.metrics["results"]] == [
        0,
        1,
    ]


@pytest.mark.parametrize("mode", ["min", "max"])
def test_checkpoints_to_keep(ray_start_4_cpus, mode):
    def train_func():
        session.report(
            dict(loss=float("nan")), checkpoint=Checkpoint.from_dict({"idx": 0})
        )  # nan, deleted
        session.report(
            dict(loss=3), checkpoint=Checkpoint.from_dict({"idx": 1})
        )  # best for min, worst for max (del)
        session.report(
            dict(loss=7), checkpoint=Checkpoint.from_dict({"idx": 2})
        )  # worst for min (del), best for max
        session.report(dict(loss=5), checkpoint=Checkpoint.from_dict({"idx": 3}))

    checkpoint_config = CheckpointConfig(
        num_to_keep=2, checkpoint_score_attribute="loss", checkpoint_score_order=mode
    )

    trainer = DataParallelTrainer(
        train_func,
        scaling_config=scale_config,
        run_config=RunConfig(checkpoint_config=checkpoint_config),
    )
    result = trainer.fit()
    assert len(result.best_checkpoints) == 2

    # Last checkpoint
    assert result.checkpoint.to_dict()["idx"] == 3

    if mode == "min":
        indices = [3, 1]
        losses = [5, 3]
    else:
        indices = [3, 2]
        losses = [5, 7]

    assert result.best_checkpoints[0][0].to_dict()["idx"] == indices[0]
    assert result.best_checkpoints[1][0].to_dict()["idx"] == indices[1]
    assert result.best_checkpoints[0][1]["loss"] == losses[0]
    assert result.best_checkpoints[1][1]["loss"] == losses[1]


def test_gpu_requests(ray_start_4_cpus_4_gpus_4_extra):
    class CudaTestBackend(Backend):
        share_cuda_visible_devices = True

    class CudaTestConfig(BackendConfig):
        @property
        def backend_cls(self):
            return CudaTestBackend

    def get_resources():
        cuda_visible_devices = os.environ["CUDA_VISIBLE_DEVICES"]
        session.report(dict(devices=cuda_visible_devices))

    # 0 GPUs will be requested and should not raise an error.
    trainer = DataParallelTrainerPatchedMultipleReturns(
        get_resources,
        backend_config=CudaTestConfig(),
        scaling_config=ScalingConfig(num_workers=2, use_gpu=False),
    )
    results = trainer.fit()
    results = [result["devices"] for result in results.metrics["results"]]
    assert results == ["", ""]

    # 1 GPU will be requested and should not raise an error.
    trainer = DataParallelTrainerPatchedMultipleReturns(
        get_resources,
        backend_config=CudaTestConfig(),
        scaling_config=ScalingConfig(num_workers=2, use_gpu=True),
    )
    results = trainer.fit()
    results = [result["devices"] for result in results.metrics["results"]]
    # Sort the cuda visible devices to have exact match with expected result.
    results = [",".join(sorted(r.split(","))) for r in results]
    assert results == ["0,1", "0,1"]

    # Partial GPUs should not raise an error.
    trainer = DataParallelTrainerPatchedMultipleReturns(
        get_resources,
        backend_config=CudaTestConfig(),
        scaling_config=ScalingConfig(
            num_workers=2, use_gpu=True, resources_per_worker={"GPU": 0.1}
        ),
    )
    results = trainer.fit()
    results = [result["devices"] for result in results.metrics["results"]]
    assert results == ["0", "0"]

    # Multiple GPUs should not raise an error.
    trainer = DataParallelTrainerPatchedMultipleReturns(
        get_resources,
        backend_config=CudaTestConfig(),
        scaling_config=ScalingConfig(
            num_workers=2, use_gpu=True, resources_per_worker={"GPU": 2}
        ),
    )
    results = trainer.fit()
    results = [result["devices"] for result in results.metrics["results"]]
    # Sort the cuda visible devices to have exact match with expected result.
    results = [",".join(sorted(r.split(","))) for r in results]
    assert results == ["0,1,2,3", "0,1,2,3"]


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
