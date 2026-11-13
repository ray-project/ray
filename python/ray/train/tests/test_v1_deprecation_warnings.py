"""Tests for the Ray Train v1 deprecation warnings.

Import-time behavior is tested in subprocesses, because ``ray.train`` is already
imported in this process.
"""

import os
import pickle
import subprocess
import sys
import textwrap
import warnings

import pytest

import ray.cloudpickle
import ray.train
import ray.train.context
import ray.train.error
from ray.train._internal.syncer import SyncConfig as _SyncConfig
from ray.train.constants import ENABLE_V2_MIGRATION_WARNINGS_ENV_VAR
from ray.train.data_parallel_trainer import DataParallelTrainer
from ray.train.torch.torch_trainer import TorchTrainer
from ray.util.annotations import RayDeprecationWarning


def _run_script(script: str, env_overrides: dict) -> str:
    env = {**os.environ, **env_overrides}
    result = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(script)],
        env=env,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    return result.stdout


# Records every RayDeprecationWarning raised while importing the given modules,
# and prints one line per warning: "<filename>|<message>".
_RECORD_IMPORT_WARNINGS = """
import warnings

with warnings.catch_warnings(record=True) as records:
    warnings.simplefilter("always")
    {imports}

from ray.util.annotations import RayDeprecationWarning

for r in records:
    if issubclass(r.category, RayDeprecationWarning):
        print(f"{{r.filename}}|{{r.message}}")
"""


def test_v1_mode_warns_once_on_import():
    out = _run_script(
        _RECORD_IMPORT_WARNINGS.format(
            imports="import ray.train; import ray.tune; import ray.train.torch"
        ),
        {"RAY_TRAIN_V2_ENABLED": "0"},
    )
    v1_lines = [line for line in out.splitlines() if "RAY_TRAIN_V2_ENABLED=0" in line]
    assert len(v1_lines) == 1, out
    filename, message = v1_lines[0].split("|", 1)
    # The warning points at the user's import, not at Ray or importlib.
    assert filename == "<string>", v1_lines
    assert "Ray 3.0" in message


def test_v1_mode_warning_opt_out():
    out = _run_script(
        _RECORD_IMPORT_WARNINGS.format(imports="import ray.train"),
        {"RAY_TRAIN_V2_ENABLED": "0", ENABLE_V2_MIGRATION_WARNINGS_ENV_VAR: "0"},
    )
    assert "RAY_TRAIN_V2_ENABLED=0" not in out


@pytest.mark.parametrize("v2_enabled", ["1", "0"])
def test_internal_imports_do_not_warn(v2_enabled):
    """Importing Ray libraries must not emit the warnings for deprecated paths."""
    out = _run_script(
        _RECORD_IMPORT_WARNINGS.format(
            imports=(
                "import ray.train; import ray.tune; import ray.train.torch; "
                "import ray.train.context; import ray.train.error; "
                "import ray.rllib"
            )
        ),
        {"RAY_TRAIN_V2_ENABLED": v2_enabled},
    )
    lines = [line for line in out.splitlines() if "RAY_TRAIN_V2_ENABLED=0" not in line]
    assert lines == []


@pytest.mark.parametrize(
    "get_attr, expected",
    [
        (lambda: ray.train.SyncConfig, _SyncConfig),
        (lambda: ray.train.context.TrainContext, ray.train.context._TrainContext),
        (lambda: ray.train.context.get_context, ray.train.context._get_context),
        (
            lambda: ray.train.error.SessionMisuseError,
            ray.train.error._SessionMisuseError,
        ),
    ],
)
def test_deprecated_attribute_warns(get_attr, expected):
    with pytest.warns(RayDeprecationWarning, match="Ray 3.0") as records:
        assert get_attr() is expected
    # The warning points at the line of user code.
    assert all(r.filename == __file__ for r in records)


def test_deprecated_from_import_warns():
    with pytest.warns(RayDeprecationWarning, match="ray.train.SyncConfig") as records:
        from ray.train import SyncConfig  # noqa: F401
    assert all(r.filename == __file__ for r in records)

    with pytest.warns(RayDeprecationWarning, match="ray.train.context.TrainContext"):
        from ray.train.context import TrainContext  # noqa: F401


def test_deprecated_attribute_opt_out(monkeypatch):
    monkeypatch.setenv(ENABLE_V2_MIGRATION_WARNINGS_ENV_VAR, "0")
    with warnings.catch_warnings():
        warnings.simplefilter("error", RayDeprecationWarning)
        assert ray.train.SyncConfig is _SyncConfig
        assert ray.train.error.SessionMisuseError is not None


def test_unknown_attribute_raises():
    with pytest.raises(AttributeError, match="has no attribute 'NotAThing'"):
        ray.train.__getattr__("NotAThing")
    assert not hasattr(ray.train, "NotAThing")
    assert not hasattr(ray.train.context, "NotAThing")
    assert not hasattr(ray.train.error, "NotAThing")


def test_deprecated_names_not_in_star_import():
    assert "SyncConfig" not in ray.train.__all__


def test_pickle_sync_config():
    """Pickled `SyncConfig` objects keep resolving through `ray.train.SyncConfig`."""
    sync_config = _SyncConfig(sync_period=10)
    with warnings.catch_warnings():
        warnings.simplefilter("error", RayDeprecationWarning)
        # ray.cloudpickle looks the class up by reference from inside Ray.
        data = ray.cloudpickle.dumps(sync_config)
    assert b"ray.train" in data and b"SyncConfig" in data

    # `loads` is called from this test, which counts as user code, so it can warn.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RayDeprecationWarning)
        assert ray.cloudpickle.loads(data) == sync_config
        assert pickle.loads(pickle.dumps(sync_config)) == sync_config


def test_tune_context_still_subclasses_v1_context():
    import ray.tune

    assert issubclass(ray.tune.TuneContext, ray.train.context._TrainContext)


def _train_fn():
    pass


def test_v1_framework_trainer_warns():
    with pytest.warns(
        RayDeprecationWarning, match="Use `ray.train.torch.TorchTrainer` instead"
    ) as records:
        TorchTrainer(_train_fn)
    v1_records = [r for r in records if "Ray Train v1 trainer" in str(r.message)]
    assert len(v1_records) == 1
    # The warning points at the line that built the trainer, not at Ray's
    # `super().__init__` chain.
    assert v1_records[0].filename == __file__


def test_v1_trainer_subclass_warns():
    class MyTorchTrainer(TorchTrainer):
        pass

    class MyTrainer(DataParallelTrainer):
        pass

    with pytest.warns(RayDeprecationWarning, match="ray.train.torch.TorchTrainer`"):
        MyTorchTrainer(_train_fn)
    with pytest.warns(RayDeprecationWarning, match="a Ray Train v2 trainer"):
        MyTrainer(_train_fn)


def test_v1_trainer_warning_opt_out(monkeypatch):
    monkeypatch.setenv(ENABLE_V2_MIGRATION_WARNINGS_ENV_VAR, "0")
    with warnings.catch_warnings():
        warnings.simplefilter("error", RayDeprecationWarning)
        TorchTrainer(_train_fn)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
