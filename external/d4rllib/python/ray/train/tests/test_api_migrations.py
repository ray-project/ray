import sys
import warnings

import pytest

import ray.train
import ray.tune
from ray.train.constants import ENABLE_V2_MIGRATION_WARNINGS_ENV_VAR
from ray.train.data_parallel_trainer import DataParallelTrainer
from ray.util.annotations import RayDeprecationWarning


@pytest.fixture(autouse=True)
def enable_v2_migration_deprecation_messages(monkeypatch):
    monkeypatch.setenv(ENABLE_V2_MIGRATION_WARNINGS_ENV_VAR, "1")
    yield
    monkeypatch.delenv(ENABLE_V2_MIGRATION_WARNINGS_ENV_VAR)


def test_trainer_restore():
    with pytest.warns(RayDeprecationWarning, match="restore"):
        try:
            DataParallelTrainer.restore("dummy")
        except Exception:
            pass

    with pytest.warns(RayDeprecationWarning, match="can_restore"):
        try:
            DataParallelTrainer.can_restore("dummy")
        except Exception:
            pass


def test_trainer_valid_configs(ray_start_4_cpus, tmp_path):
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        DataParallelTrainer(
            lambda _: None,
            scaling_config=ray.train.ScalingConfig(num_workers=1),
            run_config=ray.train.RunConfig(
                storage_path=tmp_path,
                failure_config=ray.train.FailureConfig(max_failures=1),
            ),
        ).fit()

    for warning in w:
        assert not (
            warning.category == RayDeprecationWarning
            and "`RunConfig` class should be imported from `ray.tune`"
            in str(warning.message)
        )


def test_trainer_deprecated_configs():
    with pytest.warns(RayDeprecationWarning, match="metadata"):
        DataParallelTrainer(
            lambda _: None,
            metadata={"dummy": "dummy"},
        )

    with pytest.warns(RayDeprecationWarning, match="resume_from_checkpoint"):
        DataParallelTrainer(
            lambda _: None,
            resume_from_checkpoint=ray.train.Checkpoint.from_directory("dummy"),
        )

    with pytest.warns(RayDeprecationWarning, match="fail_fast"):
        DataParallelTrainer(
            lambda _: None,
            run_config=ray.train.RunConfig(
                failure_config=ray.train.FailureConfig(fail_fast=True)
            ),
        )

    with pytest.warns(RayDeprecationWarning, match="trainer_resources"):
        DataParallelTrainer(
            lambda _: None,
            scaling_config=ray.train.ScalingConfig(trainer_resources={"CPU": 1}),
        )

    with pytest.warns(RayDeprecationWarning, match="verbose"):
        DataParallelTrainer(
            lambda _: None,
            run_config=ray.train.RunConfig(verbose=True),
        )

    with pytest.warns(RayDeprecationWarning, match="log_to_file"):
        DataParallelTrainer(
            lambda _: None,
            run_config=ray.train.RunConfig(log_to_file=True),
        )

    with pytest.warns(RayDeprecationWarning, match="stop"):
        DataParallelTrainer(
            lambda _: None,
            run_config=ray.train.RunConfig(stop={"training_iteration": 1}),
        )

    with pytest.warns(RayDeprecationWarning, match="callbacks"):
        DataParallelTrainer(
            lambda _: None,
            run_config=ray.train.RunConfig(callbacks=[ray.tune.Callback()]),
        )

    with pytest.warns(RayDeprecationWarning, match="progress_reporter"):
        DataParallelTrainer(
            lambda _: None,
            run_config=ray.train.RunConfig(
                progress_reporter=ray.tune.ProgressReporter()
            ),
        )

    with pytest.warns(RayDeprecationWarning, match="sync_config"):
        DataParallelTrainer(
            lambda _: None,
            run_config=ray.train.RunConfig(
                sync_config=ray.train.SyncConfig(sync_artifacts=True)
            ),
        )


def test_train_context_deprecations(ray_start_4_cpus, tmp_path):
    def train_fn_per_worker(config):
        with pytest.warns(RayDeprecationWarning, match="get_trial_dir"):
            ray.train.get_context().get_trial_dir()

        with pytest.warns(RayDeprecationWarning, match="get_trial_id"):
            ray.train.get_context().get_trial_id()

        with pytest.warns(RayDeprecationWarning, match="get_trial_name"):
            ray.train.get_context().get_trial_name()

        with pytest.warns(RayDeprecationWarning, match="get_trial_resources"):
            ray.train.get_context().get_trial_resources()

    trainer = DataParallelTrainer(
        train_fn_per_worker,
        scaling_config=ray.train.ScalingConfig(num_workers=1),
        run_config=ray.train.RunConfig(storage_path=tmp_path),
    )
    trainer.fit()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
