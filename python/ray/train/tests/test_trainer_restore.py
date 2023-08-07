from pathlib import Path
import pytest
import warnings

import ray
from ray import train
from ray.train import (
    Checkpoint,
    CheckpointConfig,
    RunConfig,
    ScalingConfig,
    FailureConfig,
)
from ray.air._internal.remote_storage import upload_to_uri
from ray.train.base_trainer import BaseTrainer
from ray.train.constants import LAZY_CHECKPOINT_MARKER_FILE
from ray.train.trainer import TrainingFailedError
from ray.train.data_parallel_trainer import (
    _DataParallelCheckpointManager,
    DataParallelTrainer,
)
from ray.train.torch import TorchTrainer, TorchCheckpoint
from ray.train.xgboost import XGBoostTrainer
from ray.train.lightgbm import LightGBMTrainer
from ray.train.huggingface import TransformersTrainer
from ray.tune import Callback
from ray.data.preprocessors.batch_mapper import BatchMapper
from ray.data.preprocessor import Preprocessor


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
        it = checkpoint.to_dict()["it"] + 1
        print(f"\nLoading from checkpoint, which is at iteration {it}...\n")
    train.report({"it": it}, checkpoint=Checkpoint.from_dict({"it": it}))
    if it == 1:
        raise _TestSpecificError


class FailureInjectionCallback(Callback):
    """Inject failure at the configured iteration number."""

    def __init__(self, num_iters=2):
        self.num_iters = num_iters

    def on_trial_save(self, iteration, trials, trial, **info):
        if trial.last_result["training_iteration"] == self.num_iters:
            print(f"Failing after {self.num_iters} iters...")
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
            local_dir=str(tmpdir),
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
    assert tmpdir / "data_parallel_restore_test" in result.log_dir.parents


@pytest.mark.parametrize("trainer_cls", [XGBoostTrainer, LightGBMTrainer])
def test_gbdt_trainer_restore(ray_start_6_cpus, tmpdir, trainer_cls):
    """Tests restoring gradient boosted decision tree trainers.
    Success criteria:
    - Picks up at the right iteration. 2 before crash. 3 after. 5 total trees.
    - Results are being logged to the same directory as before.
    """
    exp_name = f"{trainer_cls.__name__}_restore_test"
    datasets = {
        "train": ray.data.from_items([{"x": x, "y": x + 1} for x in range(100)])
    }

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
            local_dir=str(tmpdir),
            name=exp_name,
            checkpoint_config=CheckpointConfig(
                num_to_keep=1, checkpoint_frequency=1, checkpoint_at_end=False
            ),
            callbacks=[FailureInjectionCallback(num_iters=2)],
        ),
        num_boost_round=5,
    )
    with pytest.raises(TrainingFailedError):
        result = trainer.fit()

    trainer = trainer_cls.restore(str(tmpdir / exp_name), datasets=datasets)
    result = trainer.fit()
    assert not result.error
    assert result.metrics["training_iteration"] == 5
    assert result.metrics["iterations_since_restore"] == 3
    assert tmpdir / exp_name in result.log_dir.parents


@pytest.mark.parametrize("trainer_cls", [TransformersTrainer])
def test_trainer_with_init_fn_restore(ray_start_4_cpus, tmpdir, trainer_cls):
    """Tests restore for data parallel trainers that take in a `train_init` function
    and config. Success criteria: same as for data parallel trainers."""
    exp_name = f"{trainer_cls.__name__}_restore_test"

    if trainer_cls == TransformersTrainer:
        from ray.train.tests.test_transformers_trainer import (
            train_function as hf_init,
            train_df,
        )

        trainer_init_fn = hf_init
        trainer_init_config = {"epochs": 5, "save_strategy": "epoch"}
        datasets = {"train": ray.data.from_pandas(train_df)}
    # TODO(ml-team): Add MosaicTrainer test after Mosaic checkpointing is supported
    # else:
    #     from ray.train.tests.test_mosaic_trainer import (
    #         trainer_init_per_worker as mosaic_init,
    #     )

    #     trainer_init_fn = mosaic_init
    #     trainer_init_config = {"max_duration": "5ep"}
    #     datasets = {}

    trainer = trainer_cls(
        trainer_init_per_worker=trainer_init_fn,
        trainer_init_config=trainer_init_config,
        datasets=datasets,
        scaling_config=ScalingConfig(num_workers=2),
        run_config=RunConfig(
            local_dir=str(tmpdir),
            name=exp_name,
            checkpoint_config=CheckpointConfig(num_to_keep=1),
            callbacks=[FailureInjectionCallback(num_iters=2)],
        ),
    )
    with pytest.raises(TrainingFailedError):
        result = trainer.fit()

    trainer = trainer_cls.restore(str(tmpdir / exp_name), datasets=datasets)
    result = trainer.fit()
    assert not result.error
    assert result.metrics["training_iteration"] == 5
    assert result.metrics["iterations_since_restore"] == 3
    assert tmpdir / exp_name in result.log_dir.parents


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
        run_config=RunConfig(name="datasets_respecify_test", local_dir=tmpdir),
    )
    trainer._save(tmpdir)

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


@pytest.mark.parametrize("new_preprocessor", [True, False])
def test_preprocessor_restore(ray_start_4_cpus, tmpdir, new_preprocessor):
    """Preprocessors get restored from latest checkpoint if no new one is provided.
    They will not be re-fit if loaded from the checkpoint.
    If a new one is provided on restore, then it will be re-fit.
    """
    datasets = {
        "train": ray.data.from_items([{"x": x, "y": x + 1} for x in range(8)]),
    }

    class MyPreprocessor(Preprocessor):
        def __init__(self, id):
            self.id = id
            self._num_fits = 0

        def _fit(self, dataset):
            self.fitted_ = True
            self._num_fits += 1
            return self

        def _transform_numpy(self, np_data):
            return np_data

    trainer = DataParallelTrainer(
        train_loop_per_worker=_failing_train_fn,
        datasets=datasets,
        preprocessor=MyPreprocessor(id=1),
        scaling_config=ScalingConfig(num_workers=2),
        run_config=RunConfig(name="preprocessor_restore_test", local_dir=str(tmpdir)),
    )
    with pytest.raises(TrainingFailedError) as exc_info:
        result = trainer.fit()
    assert isinstance(exc_info.value.__cause__, _TestSpecificError)

    new_preprocessor = MyPreprocessor(id=2) if new_preprocessor else None
    trainer = DataParallelTrainer.restore(
        str(tmpdir / "preprocessor_restore_test"),
        datasets=datasets,
        preprocessor=new_preprocessor,
    )
    result = trainer.fit()
    preprocessor = result.checkpoint.get_preprocessor()
    assert result.metrics["training_iteration"] == 2
    assert preprocessor and preprocessor._num_fits == 1, (
        "The preprocessor should have been loaded from the checkpoint, "
        "and it should not have been fit again. "
        f"Fit {preprocessor._num_fits} times instead of once."
    )
    if new_preprocessor:
        assert preprocessor and preprocessor.id == 2, "Wrong preprocessor was used."


def test_obj_ref_in_preprocessor_udf(ray_start_4_cpus, tmpdir):
    """Re-specifying the preprocessor allows restoration when the preprocessor
    includes some non-serializable (across clusters) objects.
    In this test, the preprocessor consists of a calls to a dummy preprocessor
    object that is put on the object store.
    NOTE: Capturing a remote actor would actually break this on restore, since
    unpickling an actor handle immediately throws an exception from a new cluster."""

    class ModelPreprocessor:
        def transform(self, x):
            return {k: v + 1 for k, v in x.items()}

    def create_preprocessor():
        model_prep_ref = ray.put(ModelPreprocessor())

        def preprocess_fn(batch):
            batch = ray.get(model_prep_ref).transform(batch)
            return batch

        return BatchMapper(preprocess_fn, batch_format="numpy")

    preprocessor = create_preprocessor()

    def train_fn(config):
        train.report({"score": 1})

    datasets = {"train": ray.data.from_items([{"x": 1}])}
    trainer = DataParallelTrainer(
        train_loop_per_worker=train_fn,
        datasets=datasets,
        scaling_config=ScalingConfig(num_workers=2),
        run_config=RunConfig(name="obj_ref_in_train_config_test", local_dir=tmpdir),
        preprocessor=preprocessor,
    )
    trainer._save(tmpdir)

    # Explicit shutdown. Otherwise, old object references may still be usable
    ray.shutdown()
    ray.init(num_cpus=4)

    datasets = {"train": ray.data.from_items([{"x": 1}])}
    trainer = DataParallelTrainer.restore(
        str(tmpdir), datasets=datasets, preprocessor=create_preprocessor()
    )
    trainer.preprocess_datasets()

    assert trainer.datasets["train"].take()[0]["x"] == 2


def test_restore_with_different_trainer(tmpdir):
    """Tests that an error is raised if trying to restore a XTrainer with
    `YTrainer.restore`"""
    trainer = DataParallelTrainer(
        train_loop_per_worker=lambda config: train.report({"score": 1}),
        scaling_config=ScalingConfig(num_workers=1),
        run_config=RunConfig(name="restore_with_diff_trainer", local_dir=tmpdir),
    )
    trainer._save(tmpdir)

    def attempt_restore(trainer_cls, should_warn: bool, should_raise: bool):
        def check_for_raise():
            if should_raise:
                with pytest.raises(ValueError):
                    trainer_cls.restore(str(tmpdir))
            else:
                trainer_cls.restore(str(tmpdir))

        if should_warn:
            with pytest.warns(Warning) as warn_record:
                check_for_raise()
                assert any(
                    "Invalid trainer type" in str(record.message)
                    for record in warn_record
                )
        else:
            with warnings.catch_warnings():
                warnings.simplefilter("error")
                check_for_raise()

    attempt_restore(BaseTrainer, should_warn=True, should_raise=True)
    attempt_restore(XGBoostTrainer, should_warn=True, should_raise=True)
    # This won't raise because the DataParallelTrainer args can technically
    # be fed into a TorchTrainer.
    attempt_restore(TorchTrainer, should_warn=True, should_raise=False)
    attempt_restore(DataParallelTrainer, should_warn=False, should_raise=False)


def test_restore_from_invalid_dir(tmpdir):
    """Should raise an error if the restore directory doesn't exist or is invalid."""
    with pytest.raises(ValueError):
        BaseTrainer.restore(str(tmpdir))

    with pytest.raises(ValueError):
        BaseTrainer.restore("memory:///not/found")


@pytest.mark.parametrize("upload_dir", [None, "memory:///test/"])
def test_trainer_can_restore_utility(tmp_path, upload_dir):
    """Make sure that `can_restore` detects an existing experiment at a
    local/remote path and only returns True if it's at the Train experiment dir root.
    """
    name = "exp_name"
    path = tmp_path / name
    if upload_dir:
        path = Path(upload_dir) / name

    assert not DataParallelTrainer.can_restore(path)

    trainer = DataParallelTrainer(
        train_loop_per_worker=lambda config: train.report({"score": 1}),
        scaling_config=ScalingConfig(num_workers=1),
        run_config=RunConfig(name=name, local_dir=tmp_path),
    )
    (tmp_path / name).mkdir(exist_ok=True)
    trainer._save(tmp_path / name)
    if upload_dir:
        upload_to_uri(tmp_path / name, str(path))

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
            ckpt = ckpt.to_dict()
            itr = ckpt["iter"] + 1
            restore_count = ckpt["restore_count"] + 1

        for i in range(itr, final_iter + 1):
            train.report(
                dict(test=i, training_iteration=i),
                checkpoint=Checkpoint.from_dict(
                    dict(iter=i, restore_count=restore_count)
                ),
            )
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
        checkpoint = result.checkpoint.to_dict()
        assert checkpoint["iter"] == final_iter


def test_clear_lazy_ckpt_markers(ray_start_4_cpus):
    class PatchedTuneCheckpointManager(_DataParallelCheckpointManager):
        def __init__(self, *args, **kwargs):
            assert not Path(
                train.get_context().get_trial_dir(), LAZY_CHECKPOINT_MARKER_FILE
            ).exists(), "Stale lazy ckpt markers should have been removed!"
            super().__init__(*args, **kwargs)

    class DataParallelTrainerPatched(DataParallelTrainer):
        _checkpoint_manager_cls = PatchedTuneCheckpointManager

    def train_func():
        # We should always have this lazy checkpoint marker in single node training
        assert Path(
            train.get_context().get_trial_dir(), LAZY_CHECKPOINT_MARKER_FILE
        ).exists()

        if not train.get_checkpoint():
            train.report(
                metrics={"a": 1}, checkpoint=TorchCheckpoint.from_dict({"a": 1})
            )
            raise RuntimeError

    trainer = DataParallelTrainerPatched(
        train_func,
        scaling_config=ScalingConfig(num_workers=3, use_gpu=False),
        run_config=RunConfig(failure_config=FailureConfig(max_failures=1)),
    )

    trainer.fit()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
