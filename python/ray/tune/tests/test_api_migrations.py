import sys
import warnings

import pytest

import ray.train
import ray.tune
from ray.train.constants import ENABLE_V2_MIGRATION_WARNINGS_ENV_VAR
from ray.util.annotations import RayDeprecationWarning


@pytest.fixture(autouse=True)
def enable_v2_migration_deprecation_messages(monkeypatch):
    monkeypatch.setenv(ENABLE_V2_MIGRATION_WARNINGS_ENV_VAR, "1")
    yield
    monkeypatch.delenv(ENABLE_V2_MIGRATION_WARNINGS_ENV_VAR)


def test_trainable_fn_utils(tmp_path):
    dummy_checkpoint_dir = tmp_path.joinpath("dummy")
    dummy_checkpoint_dir.mkdir()

    def tune_fn(config):
        with pytest.warns(RayDeprecationWarning, match="ray.tune.get_checkpoint"):
            ray.train.get_checkpoint()

        with warnings.catch_warnings():
            ray.tune.get_checkpoint()

        with pytest.warns(RayDeprecationWarning, match="ray.tune.get_context"):
            ray.train.get_context()

        with warnings.catch_warnings():
            ray.tune.get_context()

        with pytest.warns(RayDeprecationWarning, match="ray.tune.report"):
            ray.train.report({"a": 1})

        with warnings.catch_warnings():
            ray.tune.report({"a": 1})

        with pytest.warns(RayDeprecationWarning, match="update your imports"):
            ray.tune.report(
                {"a": 1},
                checkpoint=ray.train.Checkpoint.from_directory(dummy_checkpoint_dir),
            )

        with warnings.catch_warnings():
            ray.tune.report(
                {"a": 1},
                checkpoint=ray.tune.Checkpoint.from_directory(dummy_checkpoint_dir),
            )

    tuner = ray.tune.Tuner(
        tune_fn, run_config=ray.tune.RunConfig(storage_path=tmp_path)
    )
    results = tuner.fit()
    assert not results.errors


def test_configs():
    with pytest.warns(RayDeprecationWarning, match="update your imports"):
        ray.tune.Tuner(lambda c: None, run_config=ray.train.RunConfig())

    with pytest.warns(RayDeprecationWarning, match="update your imports"):
        ray.tune.Tuner(
            lambda c: None,
            run_config=ray.tune.RunConfig(failure_config=ray.train.FailureConfig()),
        )

    with warnings.catch_warnings():
        ray.tune.Tuner(
            lambda c: None,
            run_config=ray.tune.RunConfig(failure_config=ray.tune.FailureConfig()),
        )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
