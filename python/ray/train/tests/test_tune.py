import os

import pytest
import ray
import ray.train as train
from ray import tune, cloudpickle
from ray.tune import TuneError
from ray.train import Trainer
from ray.train.backend import Backend, BackendConfig
from ray.train.constants import TUNE_CHECKPOINT_FILE_NAME
from ray.train.examples.tensorflow_mnist_example import (
    train_func as tensorflow_mnist_train_func,
)
from ray.train.examples.train_fashion_mnist_example import (
    train_func as fashion_mnist_train_func,
)
from ray.train.worker_group import WorkerGroup


@pytest.fixture
def ray_start_2_cpus():
    address_info = ray.init(num_cpus=2)
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
    epochs = 2

    trainer = Trainer("torch", num_workers=num_workers, use_gpu=use_gpu)
    MnistTrainable = trainer.to_tune_trainable(fashion_mnist_train_func)

    analysis = tune.run(
        MnistTrainable,
        num_samples=num_samples,
        config={
            "lr": tune.loguniform(1e-4, 1e-1),
            "batch_size": tune.choice([32, 64, 128]),
            "epochs": epochs,
        },
    )

    # Check that loss decreases in each trial.
    for path, df in analysis.trial_dataframes.items():
        assert df.loc[1, "loss"] < df.loc[0, "loss"]


def test_tune_torch_fashion_mnist(ray_start_8_cpus):
    torch_fashion_mnist(num_workers=2, use_gpu=False, num_samples=2)


def tune_tensorflow_mnist(num_workers, use_gpu, num_samples):
    epochs = 2
    trainer = Trainer("tensorflow", num_workers=num_workers, use_gpu=use_gpu)
    MnistTrainable = trainer.to_tune_trainable(tensorflow_mnist_train_func)

    analysis = tune.run(
        MnistTrainable,
        num_samples=num_samples,
        config={
            "lr": tune.loguniform(1e-4, 1e-1),
            "batch_size": tune.choice([32, 64, 128]),
            "epochs": epochs,
        },
    )

    # Check that loss decreases in each trial.
    for path, df in analysis.trial_dataframes.items():
        assert df.loc[1, "loss"] < df.loc[0, "loss"]


def test_tune_tensorflow_mnist(ray_start_8_cpus):
    tune_tensorflow_mnist(num_workers=2, use_gpu=False, num_samples=2)


def test_tune_error(ray_start_2_cpus):
    def train_func(config):
        raise RuntimeError("Error in training function!")

    trainer = Trainer(TestConfig(), num_workers=1)
    TestTrainable = trainer.to_tune_trainable(train_func)

    with pytest.raises(TuneError):
        tune.run(TestTrainable)


def test_tune_checkpoint(ray_start_2_cpus):
    def train_func():
        for i in range(10):
            train.report(test=i)
        train.save_checkpoint(hello="world")

    trainer = Trainer(TestConfig(), num_workers=1)
    TestTrainable = trainer.to_tune_trainable(train_func)

    [trial] = tune.run(TestTrainable).trials
    checkpoint_file = os.path.join(trial.checkpoint.value, TUNE_CHECKPOINT_FILE_NAME)
    assert os.path.exists(checkpoint_file)
    with open(checkpoint_file, "rb") as f:
        checkpoint = cloudpickle.load(f)
        assert checkpoint["hello"] == "world"


def test_reuse_checkpoint(ray_start_2_cpus):
    def train_func(config):
        itr = 0
        ckpt = train.load_checkpoint()
        if ckpt is not None:
            itr = ckpt["iter"] + 1

        for i in range(itr, config["max_iter"]):
            train.save_checkpoint(iter=i)
            train.report(test=i, training_iteration=i)

    trainer = Trainer(TestConfig(), num_workers=1)
    TestTrainable = trainer.to_tune_trainable(train_func)

    [trial] = tune.run(TestTrainable, config={"max_iter": 5}).trials
    last_ckpt = trial.checkpoint.value
    checkpoint_file = os.path.join(last_ckpt, TUNE_CHECKPOINT_FILE_NAME)
    assert os.path.exists(checkpoint_file)
    with open(checkpoint_file, "rb") as f:
        checkpoint = cloudpickle.load(f)
        assert checkpoint["iter"] == 4
    analysis = tune.run(TestTrainable, config={"max_iter": 10}, restore=last_ckpt)
    trial_dfs = list(analysis.trial_dataframes.values())
    assert len(trial_dfs[0]["training_iteration"]) == 5


def test_retry(ray_start_2_cpus):
    def train_func():
        ckpt = train.load_checkpoint()
        restored = bool(ckpt)  # Does a previous checkpoint exist?
        itr = 0
        if ckpt:
            itr = ckpt["iter"] + 1

        for i in range(itr, 4):
            if i == 2 and not restored:
                raise Exception("try to fail me")
            train.save_checkpoint(iter=i)
            train.report(test=i, training_iteration=i)

    trainer = Trainer(TestConfig(), num_workers=1)
    TestTrainable = trainer.to_tune_trainable(train_func)

    analysis = tune.run(TestTrainable, max_failures=3)
    last_ckpt = analysis.trials[0].checkpoint.value
    checkpoint_file = os.path.join(last_ckpt, TUNE_CHECKPOINT_FILE_NAME)
    assert os.path.exists(checkpoint_file)
    with open(checkpoint_file, "rb") as f:
        checkpoint = cloudpickle.load(f)
        assert checkpoint["iter"] == 3
    trial_dfs = list(analysis.trial_dataframes.values())
    assert len(trial_dfs[0]["training_iteration"]) == 4


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
