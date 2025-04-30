from functools import partial
from pathlib import Path
from typing import Dict, List

import pyarrow.fs
import pytest

import ray
from ray import train
from ray.air._internal.uri_utils import URI
from ray.train import CheckpointConfig, RunConfig, ScalingConfig
from ray.train.base_trainer import BaseTrainer
from ray.train.data_parallel_trainer import DataParallelTrainer
from ray.train.lightgbm import LightGBMTrainer
from ray.train.tests.util import create_dict_checkpoint, load_dict_checkpoint
from ray.train.trainer import TrainingFailedError
from ray.train.xgboost import XGBoostTrainer
from ray.tune import Callback
from ray.tune.experiment import Trial


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    if ray.is_initialized():
        ray.shutdown()


@pytest.fixture
def ray_start_6_cpus():
    address_info = ray.init(num_cpus=6)
    yield address_info
    # The code after the yield will run as teardown code.
    if ray.is_initialized():
        ray.shutdown()


class _TestSpecificError(RuntimeError):
    pass


def _failing_train_fn(config):
    checkpoint = train.get_checkpoint()
    it = 1
    if checkpoint:
        it = load_dict_checkpoint(checkpoint)["it"] + 1
        print(f"\nLoading from checkpoint, which is at iteration {it}...\n")
    with create_dict_checkpoint({"it": it}) as checkpoint:
        train.report({"it": it}, checkpoint=checkpoint)
    if it == 1:
        raise _TestSpecificError


class FailureInjectionCallback(Callback):
    """Inject failure at the configured iteration number."""

    def __init__(self, fail_marker_path: Path, num_iters: int = 2):
        self.num_iters = num_iters
        self.fail_marker_path = fail_marker_path

    def on_trial_result(
        self, iteration: int, trials: List[Trial], trial: Trial, result: Dict, **info
    ):
        if not self.fail_marker_path.exists():
            return

        if trial.last_result.get("training_iteration", -1) >= self.num_iters:
            print(f"Failing after {self.num_iters} iters...")
            self.fail_marker_path.unlink()
            raise _TestSpecificError


def test_data_parallel_trainer_restore(ray_start_4_cpus, tmpdir):
    """Restoring a DataParallelTrainer with object refs captured in the train fn
    or config works by re-specifying them.
    Success criteria:
    - Restored to the correct iteration. (1 iteration before crash, 1 after restore).
    - Results are being logged to the same directory as before.
    """
    dataset_size = 10
    num_workers = 2

    def create_train_fn_and_config():
        obj_ref = ray.put({"test": 1})

        def train_fn(config):
            assert ray.get(obj_ref)["test"] == 1
            assert ray.get(config["obj_ref"])["test"] == 1
            ds = train.get_dataset_shard("train")
            assert (
                sum([len(batch["feature"]) for batch in ds.iter_batches()])
                == dataset_size // num_workers
            )
            _failing_train_fn(config)

        train_loop_config = {"obj_ref": obj_ref}
        return train_fn, train_loop_config

    datasets = {"train": ray.data.from_items([{"feature": i} for i in range(10)])}
    train_fn, train_loop_config = create_train_fn_and_config()
    trainer = DataParallelTrainer(
        train_loop_per_worker=train_fn,
        train_loop_config=train_loop_config,
        datasets=datasets,
        scaling_config=ScalingConfig(num_workers=num_workers),
        run_config=RunConfig(
            name="data_parallel_restore_test",
            storage_path=str(tmpdir),
            checkpoint_config=CheckpointConfig(num_to_keep=1),
        ),
    )
    with pytest.raises(TrainingFailedError) as exc_info:
        result = trainer.fit()
    assert isinstance(exc_info.value.__cause__, _TestSpecificError)

    # Include an explicit cluster shutdown.
    # Otherwise, the previously registered object references will still exist,
    # and the test may trivially pass.
    ray.shutdown()
    ray.init(num_cpus=4)

    train_fn, train_loop_config = create_train_fn_and_config()
    datasets = {"train": ray.data.from_items([{"feature": i} for i in range(10)])}
    trainer = DataParallelTrainer.restore(
        str(tmpdir / "data_parallel_restore_test"),
        train_loop_per_worker=train_fn,
        train_loop_config=train_loop_config,
        datasets=datasets,
    )
    result = trainer.fit()
    assert not result.error
    assert result.metrics["training_iteration"] == 2
    assert result.metrics["iterations_since_restore"] == 1
    assert tmpdir / "data_parallel_restore_test" in Path(result.path).parents


@pytest.mark.parametrize("trainer_cls", [XGBoostTrainer, LightGBMTrainer])
def test_gbdt_trainer_restore(ray_start_6_cpus, tmp_path, trainer_cls, monkeypatch):
    """Tests restoring gradient boosted decision tree trainers.
    Success criteria:
    - Picks up at the right iteration. 2 before crash. 3 after. 5 total trees.
    - Results are being logged to the same directory as before.
    """
    monkeypatch.setenv("TUNE_GLOBAL_CHECKPOINT_S", "0")
    exp_name = f"{trainer_cls.__name__}_restore_test"
    datasets = {
        "train": ray.data.from_items([{"x": x, "y": x + 1} for x in range(100)])
    }

    fail_marker_path = tmp_path / "fail_marker"
    fail_marker_path.touch()

    trainer = trainer_cls(
        label_column="y",
        params={
            "objective": (
                "reg:squarederror" if trainer_cls == XGBoostTrainer else "regression"
            )
        },
        datasets=datasets,
        scaling_config=ScalingConfig(
            num_workers=2, trainer_resources={"CPU": 0}, resources_per_worker={"CPU": 1}
        ),
        run_config=RunConfig(
            storage_path=str(tmp_path),
            name=exp_name,
            checkpoint_config=CheckpointConfig(
                num_to_keep=1, checkpoint_frequency=1, checkpoint_at_end=False
            ),
            callbacks=[FailureInjectionCallback(fail_marker_path, num_iters=2)],
        ),
        num_boost_round=5,
    )
    with pytest.raises(TrainingFailedError):
        result = trainer.fit()

    trainer = trainer_cls.restore(str(tmp_path / exp_name), datasets=datasets)
    result = trainer.fit()
    assert not result.error
    assert result.metrics["training_iteration"] == 5
    assert result.metrics["iterations_since_restore"] == 3
    assert tmp_path / exp_name in Path(result.path).parents


@pytest.mark.parametrize("name", [None, "restore_from_uri"])
def test_restore_from_uri_s3(
    ray_start_4_cpus, tmp_path, monkeypatch, mock_s3_bucket_uri, name
):
    """Restoration from S3 should work."""
    trainer = DataParallelTrainer(
        train_loop_per_worker=lambda config: train.report({"score": 1}),
        scaling_config=ScalingConfig(num_workers=2),
        run_config=RunConfig(name=name, storage_path=mock_s3_bucket_uri),
    )
    result = trainer.fit()

    if name is None:
        name = Path(result.path).parent.name

    # Restore from S3
    assert DataParallelTrainer.can_restore(str(URI(mock_s3_bucket_uri) / name))
    DataParallelTrainer.restore(str(URI(mock_s3_bucket_uri) / name))


def test_restore_with_datasets(ray_start_4_cpus, tmpdir):
    """Datasets are required to re-specify if they were originally provided."""
    datasets = {
        "train": ray.data.from_items([{"x": x, "y": x + 1} for x in range(8)]),
        "valid": ray.data.from_items([{"x": x, "y": x + 1} for x in range(8)]),
    }

    trainer = DataParallelTrainer(
        train_loop_per_worker=lambda config: train.report({"score": 1}),
        datasets=datasets,
        scaling_config=ScalingConfig(num_workers=2),
        run_config=RunConfig(name="datasets_respecify_test"),
    )
    trainer._save(pyarrow.fs.LocalFileSystem(), str(tmpdir))

    # Restore should complain, if all the datasets don't get passed in again
    with pytest.raises(ValueError):
        DataParallelTrainer.restore(str(tmpdir))

    with pytest.raises(ValueError):
        DataParallelTrainer.restore(str(tmpdir), datasets={"train": datasets["train"]})

    with pytest.raises(ValueError):
        DataParallelTrainer.restore(
            str(tmpdir),
            datasets={"train": datasets["train"], "invalid_key": datasets["valid"]},
        )

    trainer = DataParallelTrainer.restore(str(tmpdir), datasets=datasets)


def test_restore_from_invalid_dir(tmpdir):
    """Should raise an error if the restore directory doesn't exist or is invalid."""
    with pytest.raises(ValueError):
        BaseTrainer.restore(str(tmpdir))

    with pytest.raises(ValueError):
        BaseTrainer.restore("mock:///not/found")


def test_trainer_can_restore_utility(tmp_path):
    """Make sure that `can_restore` detects an existing experiment at a
    local/remote path and only returns True if it's at the Train experiment dir root.
    """
    name = "exp_name"
    path = tmp_path / name

    assert not DataParallelTrainer.can_restore(path)

    trainer = DataParallelTrainer(
        train_loop_per_worker=lambda config: train.report({"score": 1}),
        scaling_config=ScalingConfig(num_workers=1),
    )
    (tmp_path / name).mkdir(exist_ok=True)
    trainer._save(pyarrow.fs.LocalFileSystem(), str(tmp_path / name))

    assert DataParallelTrainer.can_restore(path)


@pytest.mark.parametrize("eventual_success", [True, False])
def test_retry_with_max_failures(ray_start_4_cpus, eventual_success):
    """Test auto-resume of a Train run when setting max_failures > 0."""

    num_failures = 2 if eventual_success else 3
    max_retries = 2
    final_iter = 10

    def train_func():
        ckpt = train.get_checkpoint()
        itr = 1
        restore_count = 0
        if ckpt:
            ckpt = load_dict_checkpoint(ckpt)
            itr = ckpt["iter"] + 1
            restore_count = ckpt["restore_count"] + 1

        for i in range(itr, final_iter + 1):
            with create_dict_checkpoint(
                dict(iter=i, restore_count=restore_count)
            ) as checkpoint:
                train.report(dict(test=i, training_iteration=i), checkpoint=checkpoint)
            if restore_count < num_failures:
                raise RuntimeError("try to fail me")

    trainer = DataParallelTrainer(
        train_func,
        scaling_config=ScalingConfig(num_workers=2),
        run_config=RunConfig(
            failure_config=train.FailureConfig(max_failures=max_retries)
        ),
    )

    if not eventual_success:
        # If we gave up due to hitting our max retry attempts,
        # then `trainer.fit` should raise the last error we encountered.
        with pytest.raises(TrainingFailedError):
            trainer.fit()
    else:
        # If we encounter errors but eventually succeed, `trainer.fit` should NOT
        # raise any of those errors.
        result = trainer.fit()
        assert not result.error
        checkpoint = load_dict_checkpoint(result.checkpoint)
        assert checkpoint["iter"] == final_iter


def test_restoration_after_termination(tmp_path):
    """Test that the train loop can be run again if restoring the trainer
    after the run finished running successfully."""

    def train_func_per_worker(config, num_epochs=5):
        ckpt = train.get_checkpoint()
        start_iter = 1
        if ckpt:
            ckpt = load_dict_checkpoint(ckpt)
            start_iter = ckpt["iter"] + 1

        for i in range(start_iter, num_epochs + 1):
            with create_dict_checkpoint(dict(iter=i)) as checkpoint:
                train.report(dict(iter=i), checkpoint=checkpoint)

    name = "exp_name"
    path = tmp_path / name

    trainer = DataParallelTrainer(
        train_loop_per_worker=train_func_per_worker,
        scaling_config=ScalingConfig(num_workers=1),
        run_config=RunConfig(
            name=name,
            storage_path=tmp_path,
            checkpoint_config=CheckpointConfig(num_to_keep=2),
        ),
    )
    result = trainer.fit()
    assert result.metrics["iter"] == 5

    restored_trainer = DataParallelTrainer.restore(
        str(path), train_loop_per_worker=partial(train_func_per_worker, num_epochs=10)
    )
    new_result = restored_trainer.fit()
    assert new_result.metrics["iter"] == 10

    assert new_result.path == result.path
    assert len(list(Path(new_result.path).glob("checkpoint*"))) == 2


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
