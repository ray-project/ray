import os

import pytest

import ray
from ray import tune
from ray.air import Checkpoint, session
from ray.air.config import FailureConfig, RunConfig, ScalingConfig
from ray.train._internal.worker_group import WorkerGroup
from ray.train.backend import Backend, BackendConfig
from ray.train.data_parallel_trainer import DataParallelTrainer
from ray.train.examples.tensorflow_mnist_example import (
    train_func as tensorflow_mnist_train_func,
)
from ray.train.examples.torch_fashion_mnist_example import (
    train_func as fashion_mnist_train_func,
)
from ray.train.tensorflow.tensorflow_trainer import TensorflowTrainer
from ray.train.torch.torch_trainer import TorchTrainer
from ray.tune.tune_config import TuneConfig
from ray.tune.tuner import Tuner


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def ray_start_8_cpus():
    address_info = ray.init(num_cpus=8)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


class TestConfig(BackendConfig):
    @property
    def backend_cls(self):
        return TestBackend


class TestBackend(Backend):
    def on_start(self, worker_group: WorkerGroup, backend_config: TestConfig):
        pass

    def on_shutdown(self, worker_group: WorkerGroup, backend_config: TestConfig):
        pass


def torch_fashion_mnist(num_workers, use_gpu, num_samples):
    trainer = TorchTrainer(
        fashion_mnist_train_func,
        scaling_config=ScalingConfig(num_workers=num_workers, use_gpu=use_gpu),
    )
    tuner = Tuner(
        trainer,
        param_space={
            "train_loop_config": {
                "lr": tune.loguniform(1e-4, 1e-1),
                "batch_size": tune.choice([32, 64, 128]),
                "epochs": 2,
            }
        },
        tune_config=TuneConfig(
            num_samples=num_samples,
        ),
    )
    analysis = tuner.fit()._experiment_analysis

    # Check that loss decreases in each trial.
    for path, df in analysis.trial_dataframes.items():
        assert df.loc[1, "loss"] < df.loc[0, "loss"]


def test_tune_torch_fashion_mnist(ray_start_8_cpus):
    torch_fashion_mnist(num_workers=2, use_gpu=False, num_samples=2)


def tune_tensorflow_mnist(num_workers, use_gpu, num_samples):
    trainer = TensorflowTrainer(
        tensorflow_mnist_train_func,
        scaling_config=ScalingConfig(num_workers=num_workers, use_gpu=use_gpu),
    )
    tuner = Tuner(
        trainer,
        param_space={
            "train_loop_config": {
                "lr": tune.loguniform(1e-4, 1e-1),
                "batch_size": tune.choice([32, 64, 128]),
                "epochs": 2,
            }
        },
        tune_config=TuneConfig(
            num_samples=num_samples,
        ),
    )
    analysis = tuner.fit()._experiment_analysis

    # Check that loss decreases in each trial.
    for path, df in analysis.trial_dataframes.items():
        assert df.loc[1, "loss"] < df.loc[0, "loss"]


def test_tune_tensorflow_mnist(ray_start_8_cpus):
    tune_tensorflow_mnist(num_workers=2, use_gpu=False, num_samples=2)


def test_tune_error(ray_start_4_cpus):
    def train_func(config):
        raise RuntimeError("Error in training function!")

    trainer = DataParallelTrainer(
        train_func,
        backend_config=TestConfig(),
        scaling_config=ScalingConfig(num_workers=1),
    )
    tuner = Tuner(
        trainer,
    )

    result_grid = tuner.fit()
    with pytest.raises(RuntimeError):
        raise result_grid[0].error


def test_tune_checkpoint(ray_start_4_cpus):
    def train_func():
        for i in range(9):
            session.report(dict(test=i))
        session.report(
            dict(test=i + 1), checkpoint=Checkpoint.from_dict(dict(hello="world"))
        )

    trainer = DataParallelTrainer(
        train_func,
        backend_config=TestConfig(),
        scaling_config=ScalingConfig(num_workers=1),
    )
    tuner = Tuner(
        trainer,
        param_space={"train_loop_config": {"max_iter": 5}},
    )

    [trial] = tuner.fit()._experiment_analysis.trials
    checkpoint_path = trial.checkpoint.dir_or_data
    assert os.path.exists(checkpoint_path)
    checkpoint = Checkpoint.from_directory(checkpoint_path).to_dict()
    assert checkpoint["hello"] == "world"


def test_reuse_checkpoint(ray_start_4_cpus):
    def train_func(config):
        itr = 0
        ckpt = session.get_checkpoint()
        if ckpt is not None:
            ckpt = ckpt.to_dict()
            itr = ckpt["iter"] + 1

        for i in range(itr, config["max_iter"]):
            session.report(
                dict(test=i, training_iteration=i),
                checkpoint=Checkpoint.from_dict(dict(iter=i)),
            )

    trainer = DataParallelTrainer(
        train_func,
        backend_config=TestConfig(),
        scaling_config=ScalingConfig(num_workers=1),
    )
    tuner = Tuner(
        trainer,
        param_space={"train_loop_config": {"max_iter": 5}},
    )
    [trial] = tuner.fit()._experiment_analysis.trials
    checkpoint_path = trial.checkpoint.dir_or_data
    checkpoint = Checkpoint.from_directory(checkpoint_path).to_dict()
    assert checkpoint["iter"] == 4

    tuner = Tuner(
        trainer,
        param_space={"train_loop_config": {"max_iter": 10}},
    ).restore(trial.local_dir)
    analysis = tuner.fit()._experiment_analysis
    trial_dfs = list(analysis.trial_dataframes.values())
    assert len(trial_dfs[0]["training_iteration"]) == 5


def test_retry(ray_start_4_cpus):
    def train_func():
        ckpt = session.get_checkpoint()
        restored = bool(ckpt)  # Does a previous checkpoint exist?
        itr = 0
        if ckpt:
            ckpt = ckpt.to_dict()
            itr = ckpt["iter"] + 1

        for i in range(itr, 4):
            if i == 2 and not restored:
                raise Exception("try to fail me")
            session.report(
                dict(test=i, training_iteration=i),
                checkpoint=Checkpoint.from_dict(dict(iter=i)),
            )

    trainer = DataParallelTrainer(
        train_func,
        backend_config=TestConfig(),
        scaling_config=ScalingConfig(num_workers=1),
    )
    tuner = Tuner(
        trainer, run_config=RunConfig(failure_config=FailureConfig(max_failures=3))
    )

    analysis = tuner.fit()._experiment_analysis
    checkpoint_path = analysis.trials[0].checkpoint.dir_or_data
    checkpoint = Checkpoint.from_directory(checkpoint_path).to_dict()
    assert checkpoint["iter"] == 3

    trial_dfs = list(analysis.trial_dataframes.values())
    assert len(trial_dfs[0]["training_iteration"]) == 4


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
