import logging

import pytest

import ray
from ray import train, tune
from ray.air.constants import TRAINING_ITERATION
from ray.train import FailureConfig, RunConfig, ScalingConfig
from ray.train._internal.worker_group import WorkerGroup
from ray.train.backend import Backend, BackendConfig
from ray.train.data_parallel_trainer import DataParallelTrainer
from ray.train.examples.tf.tensorflow_mnist_example import (
    train_func as tensorflow_mnist_train_func,
)
from ray.train.examples.pytorch.torch_fashion_mnist_example import (
    train_func_per_worker as fashion_mnist_train_func,
)
from ray.train.tensorflow.tensorflow_trainer import TensorflowTrainer
from ray.train.torch.torch_trainer import TorchTrainer
from ray.tune.tune_config import TuneConfig
from ray.tune.tuner import Tuner

from ray.train.tests.util import create_dict_checkpoint, load_dict_checkpoint


@pytest.fixture(scope="module")
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
                "batch_size_per_worker": tune.choice([32, 64, 128]),
                "epochs": 2,
            }
        },
        tune_config=TuneConfig(
            num_samples=num_samples,
        ),
    )
    analysis = tuner.fit()._experiment_analysis

    # Check that loss decreases in each trial.
    for df in analysis.trial_dataframes.values():
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
    for df in analysis.trial_dataframes.values():
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
            train.report(dict(test=i))
        with create_dict_checkpoint(dict(hello="world")) as checkpoint:
            train.report(dict(test=i + 1), checkpoint=checkpoint)

    trainer = DataParallelTrainer(
        train_func,
        backend_config=TestConfig(),
        scaling_config=ScalingConfig(num_workers=1),
    )
    tuner = Tuner(
        trainer,
        param_space={"train_loop_config": {"max_iter": 5}},
    )

    result_grid = tuner.fit()
    assert len(result_grid) == 1
    result = result_grid[0]
    assert result.checkpoint
    assert load_dict_checkpoint(result.checkpoint)["hello"] == "world"


def test_reuse_checkpoint(ray_start_4_cpus):
    def train_func(config):
        itr = 0
        ckpt = train.get_checkpoint()
        if ckpt is not None:
            ckpt = load_dict_checkpoint(ckpt)
            itr = ckpt["iter"] + 1

        for i in range(itr, config["max_iter"]):
            with create_dict_checkpoint(dict(iter=i)) as checkpoint:
                train.report(dict(test=i, training_iteration=i), checkpoint=checkpoint)

    trainer = DataParallelTrainer(
        train_func,
        backend_config=TestConfig(),
        scaling_config=ScalingConfig(num_workers=1),
    )
    tuner = Tuner(
        trainer,
        param_space={"train_loop_config": {"max_iter": 5}},
    )
    result_grid = tuner.fit()
    assert len(result_grid) == 1
    result = result_grid[0]
    assert result.checkpoint
    assert load_dict_checkpoint(result.checkpoint)["iter"] == 4

    tuner = Tuner.restore(result_grid.experiment_path, trainable=trainer)
    result_grid = tuner.fit()
    assert len(result_grid) == 1
    assert len(result_grid[0].metrics_dataframe) == 5


def test_retry_with_max_failures(ray_start_4_cpus):
    """Tests trainer retry with max_failures > 0 when integrating with Tune."""

    def train_func():
        ckpt = train.get_checkpoint()
        restored = bool(ckpt)  # Does a previous checkpoint exist?
        itr = 0
        if ckpt:
            ckpt = load_dict_checkpoint(ckpt)
            itr = ckpt["iter"] + 1

        for i in range(itr, 4):
            if i == 2 and not restored:
                raise Exception("try to fail me")
            with create_dict_checkpoint(dict(iter=i)) as checkpoint:
                train.report(dict(test=i, training_iteration=i), checkpoint=checkpoint)

    trainer = DataParallelTrainer(
        train_func,
        backend_config=TestConfig(),
        scaling_config=ScalingConfig(num_workers=1),
    )
    tuner = Tuner(
        trainer, run_config=RunConfig(failure_config=FailureConfig(max_failures=3))
    )

    result_grid = tuner.fit()
    checkpoint = load_dict_checkpoint(result_grid[0].checkpoint)
    assert checkpoint["iter"] == 3
    df = result_grid[0].metrics_dataframe
    assert len(df[TRAINING_ITERATION]) == 4


def test_restore_with_new_trainer(ray_start_4_cpus, tmpdir, propagate_logs, caplog):
    def train_func(config):
        raise RuntimeError("failing!")

    trainer = DataParallelTrainer(
        train_func,
        backend_config=TestConfig(),
        scaling_config=ScalingConfig(num_workers=1),
        run_config=RunConfig(local_dir=str(tmpdir), name="restore_new_trainer"),
        datasets={"train": ray.data.from_items([{"a": i} for i in range(10)])},
    )
    results = Tuner(trainer).fit()
    assert results.errors

    def train_func(config):
        dataset = train.get_dataset_shard("train")
        assert train.get_context().get_world_size() == 2
        rows = 0
        for _ in dataset.iter_rows():
            rows += 1
        assert rows == 10

    trainer = DataParallelTrainer(
        # Training function can be modified
        train_func,
        backend_config=TestConfig(),
        # ScalingConfig can be modified
        scaling_config=ScalingConfig(num_workers=2),
        # New RunConfig will be ignored
        run_config=RunConfig(name="ignored"),
        # Datasets and preprocessors can be re-specified
        datasets={"train": ray.data.from_items([{"a": i} for i in range(20)])},
    )
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="ray.tune.impl.tuner_internal"):
        tuner = Tuner.restore(
            str(tmpdir / "restore_new_trainer"),
            trainable=trainer,
            resume_errored=True,
        )
        assert "they will be ignored in the resumed run" in caplog.text

    results = tuner.fit()
    assert not results.errors


@pytest.mark.parametrize("in_trainer", [True, False])
@pytest.mark.parametrize("in_tuner", [True, False])
def test_run_config_in_trainer_and_tuner(
    propagate_logs, tmp_path, monkeypatch, caplog, in_trainer, in_tuner
):
    monkeypatch.setenv("RAY_AIR_LOCAL_CACHE_DIR", str(tmp_path))
    trainer_run_config = RunConfig(name="trainer") if in_trainer else None
    tuner_run_config = RunConfig(name="tuner") if in_tuner else None
    trainer = DataParallelTrainer(
        lambda config: None,
        backend_config=TestConfig(),
        scaling_config=ScalingConfig(num_workers=1),
        run_config=trainer_run_config,
    )
    with caplog.at_level(logging.INFO, logger="ray.tune.impl.tuner_internal"):
        tuner = Tuner(trainer, run_config=tuner_run_config)

    both_msg = (
        "`RunConfig` was passed to both the `Tuner` and the `DataParallelTrainer`"
    )
    if in_trainer and in_tuner:
        assert (tmp_path / "tuner").exists()
        assert not (tmp_path / "trainer").exists()
        assert both_msg in caplog.text
    elif in_trainer and not in_tuner:
        assert not (tmp_path / "tuner").exists()
        assert (tmp_path / "trainer").exists()
        assert both_msg not in caplog.text
    elif not in_trainer and in_tuner:
        assert (tmp_path / "tuner").exists()
        assert not (tmp_path / "trainer").exists()
        assert both_msg not in caplog.text
    else:
        assert tuner._local_tuner.get_run_config() == RunConfig()
        assert both_msg not in caplog.text


def test_run_config_in_param_space():
    trainer = DataParallelTrainer(
        lambda config: None,
        backend_config=TestConfig(),
        scaling_config=ScalingConfig(num_workers=1),
    )
    with pytest.raises(ValueError):
        Tuner(trainer, param_space={"run_config": RunConfig(name="ignored")})


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
