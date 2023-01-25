import os
import pytest

import ray
from ray.air import Checkpoint, CheckpointConfig, RunConfig, ScalingConfig, session
from ray.train.base_trainer import TrainingFailedError
from ray.train.data_parallel_trainer import DataParallelTrainer
from ray.train.xgboost import XGBoostTrainer
from ray.train.lightgbm import LightGBMTrainer
from ray.tune import Callback, TuneError
from ray.tune.tuner import TunerInternal
from ray.data.preprocessors.batch_mapper import BatchMapper
from ray.data.preprocessor import Preprocessor


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


def _failing_train_fn(config):
    checkpoint = session.get_checkpoint()
    it = 0
    if checkpoint:
        it = checkpoint.to_dict()["it"] + 1
        print("\nLoading from checkpoint, which is at iteration {it}...\n")
    session.report({"it": it}, checkpoint=Checkpoint.from_dict({"it": it}))
    if it == 0:
        raise RuntimeError


def test_data_parallel_trainer_restore(ray_start_4_cpus, tmpdir):
    obj_ref = ray.put({"test": 1})

    def train_fn(config):
        assert ray.get(obj_ref)["test"] == 1
        assert ray.get(config["obj_ref"])["test"] == 1
        ds = session.get_dataset_shard("train")
        assert sum(len(batch) for batch in ds.iter_batches()) == 10

        _failing_train_fn(config)

    train_loop_config = {"obj_ref": obj_ref}
    datasets = {"train": ray.data.from_items([{"feature": i} for i in range(10)])}

    trainer = DataParallelTrainer(
        train_loop_per_worker=train_fn,
        train_loop_config=train_loop_config,
        datasets=datasets,
        scaling_config=ScalingConfig(num_workers=2),
        run_config=RunConfig(
            name="data_parallel_restore_test",
            local_dir=tmpdir,
            checkpoint_config=CheckpointConfig(num_to_keep=1),
        ),
    )
    with pytest.raises(TrainingFailedError):
        result = trainer.fit()

    trainer = DataParallelTrainer.restore(
        str(tmpdir / "data_parallel_restore_test"),
        train_loop_per_worker=train_fn,
        train_loop_config=train_loop_config,
        datasets=datasets,
    )
    result = trainer.fit()
    assert result.metrics["it"] == 1
    assert tmpdir / "data_parallel_restore_test" in result.log_dir.parents


@pytest.mark.parametrize("trainer_cls", [XGBoostTrainer, LightGBMTrainer])
def test_gbdt_trainer_restore(ray_start_4_cpus, tmpdir, trainer_cls):
    class FailureInjectionCallback(Callback):
        """Inject failure at the configured iteration number."""

        def __init__(self, num_iters=5):
            self.num_iters = num_iters

        def on_trial_result(self, iteration, trials, trial, result, **info):
            if iteration == self.num_iters:
                print(f"Failing after {self.num_iters} iters.")
                raise RuntimeError

    exp_name = f"{trainer_cls.__name__}_restore_test"
    datasets = {"train": ray.data.from_items([{"x": x, "y": x + 1} for x in range(32)])}
    trainer = trainer_cls(
        label_column="y",
        params={"objective": "reg:squarederror"},
        datasets=datasets,
        scaling_config=ScalingConfig(num_workers=2),
        run_config=RunConfig(
            local_dir=str(tmpdir),
            name=exp_name,
            checkpoint_config=CheckpointConfig(num_to_keep=1),
            callbacks=[FailureInjectionCallback()],
        ),
        num_boost_round=10,
    )
    with pytest.raises(TuneError):
        result = trainer.fit()

    trainer = trainer_cls.restore(str(tmpdir / exp_name), datasets=datasets)
    result = trainer.fit()
    assert result.metrics["training_iteration"] == 10
    assert tmpdir / exp_name in result.log_dir.parents


def test_restore_with_datasets(tmpdir):
    datasets = {
        "train": ray.data.from_items([{"x": x, "y": x + 1} for x in range(8)]),
        "valid": ray.data.from_items([{"x": x, "y": x + 1} for x in range(8)]),
    }

    trainer = DataParallelTrainer(
        train_loop_per_worker=lambda config: session.report({"score": 1}),
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

    with pytest.raises(AssertionError):
        DataParallelTrainer.restore(
            str(tmpdir),
            datasets={"train": datasets["train"], "invalid_key": datasets["valid"]},
        )

    trainer = DataParallelTrainer.restore(str(tmpdir), datasets=datasets)


def test_preprocessor_restore(ray_start_4_cpus, tmpdir):
    datasets = {
        "train": ray.data.from_items([{"x": x, "y": x + 1} for x in range(8)]),
    }

    class MyPreprocessor(Preprocessor):
        def __init__(self):
            self.num_fits_ = 0

        def _fit(self, dataset):
            self.num_fits_ += 1
            return self

        def _transform_numpy(self, np_data):
            return np_data

    trainer = DataParallelTrainer(
        train_loop_per_worker=_failing_train_fn,
        datasets=datasets,
        preprocessor=MyPreprocessor(),
        scaling_config=ScalingConfig(num_workers=2),
        run_config=RunConfig(name="preprocessor_restore_test", local_dir=tmpdir),
    )
    with pytest.raises(TrainingFailedError):
        trainer.fit()

    trainer = DataParallelTrainer.restore(
        str(tmpdir / "preprocessor_restore_test"), datasets=datasets
    )
    result = trainer.fit()
    preprocessor = result.checkpoint.get_preprocessor()
    assert preprocessor and preprocessor.num_fits_ == 1, (
        "The preprocessor should have been loaded from the checkpoint, "
        "and it should not have been fit again. "
        f"Fit {trainer.preprocessor.num_fits_} times instead of once."
    )


def test_obj_ref_in_train_loop_scope(tmpdir):
    obj_ref = ray.put({"test": 1})

    def train_fn(config):
        print(ray.get(obj_ref))

    trainer = DataParallelTrainer(
        train_loop_per_worker=train_fn,
        datasets={},
        scaling_config=ScalingConfig(num_workers=2),
        run_config=RunConfig(name="obj_ref_in_train_loop_test", local_dir=tmpdir),
    )
    trainer._save(tmpdir)

    # Restore should complain, since the training loop captures an object ref.
    with pytest.raises(ValueError):
        DataParallelTrainer.restore(str(tmpdir))

    trainer = DataParallelTrainer.restore(str(tmpdir), train_loop_per_worker=train_fn)


def test_obj_ref_in_train_loop_config(tmpdir):
    obj_ref = ray.put({"test": 1})

    def train_fn(config):
        session.report({"score": 1})

    train_loop_config = {"obj_ref": obj_ref}
    trainer = DataParallelTrainer(
        train_loop_per_worker=train_fn,
        train_loop_config=train_loop_config,
        datasets={},
        scaling_config=ScalingConfig(num_workers=2),
        run_config=RunConfig(name="obj_ref_in_train_config_test", local_dir=tmpdir),
    )
    trainer._save(tmpdir)

    # Restore should complain, since the training config contains an object ref.
    with pytest.raises(ValueError):
        DataParallelTrainer.restore(str(tmpdir))

    trainer = DataParallelTrainer.restore(
        str(tmpdir), train_loop_config=train_loop_config
    )


def test_obj_ref_in_preprocessor_udf(ray_start_4_cpus, tmpdir):
    class ModelPreprocessor:
        def transform(self, x):
            return {k: v + 1 for k, v in x.items()}

    model_prep_ref = ray.put(ModelPreprocessor())
    model_actor_handle = ray.remote(ModelPreprocessor).remote()

    def preprocess_fn(batch):
        batch = ray.get(model_prep_ref).transform(batch)
        batch = ray.get(model_actor_handle.transform.remote(batch))
        return batch

    preprocessor = BatchMapper(preprocess_fn, batch_format="numpy")

    def train_fn(config):
        session.report({"score": 1})

    datasets = {"train": ray.data.from_items([{"x": 1}])}
    trainer = DataParallelTrainer(
        train_loop_per_worker=train_fn,
        datasets=datasets,
        scaling_config=ScalingConfig(num_workers=2),
        run_config=RunConfig(name="obj_ref_in_train_config_test", local_dir=tmpdir),
        preprocessor=preprocessor,
    )
    trainer._save(tmpdir)

    # Restore should complain, since the preprocessor UDF captures an object ref
    # TODO: get this to work
    # with pytest.raises(ValueError):
    #     DataParallelTrainer.restore(str(tmpdir), datasets=datasets)

    trainer = DataParallelTrainer.restore(
        str(tmpdir), datasets=datasets, preprocessor=preprocessor
    )
    trainer.preprocess_datasets()

    # Applying preprocessor to the dataset 2 times -> 3
    assert trainer.datasets["train"].take()[0]["x"] == 3


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
