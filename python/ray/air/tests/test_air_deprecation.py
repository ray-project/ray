"""Tests for the deprecation aliases of `ray.air` public paths (removed in Ray 3.0)."""

import importlib
import subprocess
import sys
import textwrap
import warnings

import pytest

import ray.air
import ray.air.config
import ray.air.result
from ray.util.annotations import RayDeprecationWarning

# (deprecated name, object it must resolve to, replacement that must be a drop-in,
#  other replacements named in the message).
# A drop-in replacement is the same object or a subclass, so code that switches to it
# keeps working. The other replacement is the Ray Train class, which is the only one
# Train v2 accepts: Train v2 already rejects the `ray.air` RunConfig/CheckpointConfig.
ALIASES = [
    (
        "RunConfig",
        ray.air.config.RunConfig,
        "ray.tune.RunConfig",
        "ray.train.RunConfig",
    ),
    (
        "CheckpointConfig",
        ray.air.config.CheckpointConfig,
        "ray.tune.CheckpointConfig",
        "ray.train.CheckpointConfig",
    ),
    (
        "FailureConfig",
        ray.air.config.FailureConfig,
        "ray.tune.FailureConfig",
        "ray.train.FailureConfig",
    ),
    ("ScalingConfig", ray.air.config.ScalingConfig, "ray.train.ScalingConfig", None),
    ("Result", ray.air.result.Result, "ray.tune.Result", "ray.train.Result"),
]


def _import_path(path: str):
    module_name, attr = path.rsplit(".", 1)
    return getattr(importlib.import_module(module_name), attr)


def _air_warnings(record):
    return [w for w in record if issubclass(w.category, RayDeprecationWarning)]


@pytest.mark.parametrize("name, expected, drop_in, other", ALIASES)
def test_attribute_access_warns(name, expected, drop_in, other):
    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        obj = getattr(ray.air, name)

    assert obj is expected
    [warning] = _air_warnings(record)
    message = str(warning.message)
    assert f"`ray.air.{name}` is deprecated" in message
    assert "Ray 3.0" in message
    assert f"`{drop_in}`" in message
    if other:
        assert f"`{other}`" in message
    # The warning points at the caller, so users can find the line to fix.
    assert warning.filename == __file__


@pytest.mark.parametrize("name, expected, drop_in, other", ALIASES)
def test_from_import_warns(name, expected, drop_in, other):
    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        namespace = {}
        exec(f"from ray.air import {name}", namespace)

    assert namespace[name] is expected
    # `from ... import` looks the name up twice (`hasattr`, then `getattr`). Both
    # warnings point at the user's line, so the default "module" filter shows one.
    air_warnings = _air_warnings(record)
    assert air_warnings
    assert {w.filename for w in air_warnings} == {"<string>"}


@pytest.mark.parametrize("name, expected, drop_in, other", ALIASES)
def test_replacements_work(name, expected, drop_in, other):
    """The paths named in the warning exist, and switching to them is a drop-in."""
    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        drop_in_obj = _import_path(drop_in)
        if other:
            _import_path(other)

    assert issubclass(drop_in_obj, expected)
    # Using the replacement doesn't warn.
    assert not _air_warnings(record)


def test_unknown_attribute_raises():
    with pytest.raises(AttributeError, match="has no attribute 'DoesNotExist'"):
        _ = ray.air.DoesNotExist


def test_dir_lists_deprecated_names():
    for name, *_ in ALIASES:
        assert name in dir(ray.air)


def test_non_deprecated_names_do_not_warn():
    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        _ = ray.air.DataBatchType
        _ = ray.air.ResourceRequest
        _ = ray.air.AcquiredResources
        _ = ray.air.config.RunConfig
        _ = ray.air.result.Result

    assert not _air_warnings(record)


def test_ray_does_not_trigger_air_warnings():
    """Ray's own imports and a basic Tune run and Train v2 run must not warn.

    Users can't silence a warning that Ray's own code triggers, so any
    `RayDeprecationWarning` about `ray.air` raised here is a bug.
    """
    script = textwrap.dedent(
        """
        import warnings

        from ray.util.annotations import RayDeprecationWarning

        warnings.filterwarnings(
            "error", message=".*`ray\\\\.air", category=RayDeprecationWarning
        )

        import ray
        import ray.data
        import ray.train
        import ray.train.torch
        import ray.tune
        from ray import train, tune

        ray.init(num_cpus=2)

        def objective(config):
            tune.report({"score": config["x"]})

        results = tune.Tuner(
            objective, param_space={"x": tune.grid_search([1, 2])}
        ).fit()
        assert not results.errors

        def train_fn():
            train.report({"loss": 0.0})

        result = ray.train.torch.TorchTrainer(
            train_fn, scaling_config=train.ScalingConfig(num_workers=1)
        ).fit()
        assert result.error is None
        print("OK")
        """
    )
    proc = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=170
    )
    assert proc.returncode == 0 and "OK" in proc.stdout, proc.stderr[-5000:]


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
