import functools
import importlib
import sys
import warnings

import pytest

import ray.train
import ray.tune
from ray.train.constants import ENABLE_V2_MIGRATION_WARNINGS_ENV_VAR
from ray.util.annotations import RayDeprecationWarning


@pytest.fixture(autouse=True)
def enable_v2(monkeypatch):
    monkeypatch.setenv("RAY_TRAIN_V2_ENABLED", "1")
    importlib.reload(ray.train)
    yield


@pytest.fixture(autouse=True)
def enable_v2_migration_deprecation_messages(monkeypatch):
    monkeypatch.setenv(ENABLE_V2_MIGRATION_WARNINGS_ENV_VAR, "1")
    yield
    monkeypatch.delenv(ENABLE_V2_MIGRATION_WARNINGS_ENV_VAR)


@pytest.mark.parametrize("v2_enabled", [False, True])
def test_trainable_fn_utils(tmp_path, monkeypatch, v2_enabled):
    monkeypatch.setenv("RAY_TRAIN_V2_ENABLED", str(int(v2_enabled)))
    importlib.reload(ray.train)

    dummy_checkpoint_dir = tmp_path.joinpath("dummy")
    dummy_checkpoint_dir.mkdir()

    asserting_context = (
        functools.partial(pytest.raises, DeprecationWarning)
        if v2_enabled
        else functools.partial(pytest.warns, RayDeprecationWarning)
    )

    def tune_fn(config):
        with asserting_context(match="get_checkpoint"):
            ray.train.get_checkpoint()

        with warnings.catch_warnings():
            ray.tune.get_checkpoint()

        with asserting_context(match="get_context"):
            ray.train.get_context()

        with warnings.catch_warnings():
            ray.tune.get_context()

        with asserting_context(match="report"):
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
