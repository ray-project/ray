import importlib
import os
import sys
import warnings

import pytest

import ray._private.telemetry.metric_cardinality as mc
import ray.serve._private.constants as serve_constants

ALIAS_ENV = "RAY_SERVE_CONTROLLER_METRICS_INCLUDE_HIGH_CARDINALITY_TAGS"


def _reload(level, alias=None):
    """Reload serve constants under a cardinality level and optional alias env var.

    The level is read from metric_cardinality's module binding, which is frozen
    at import, so set it there directly rather than via the environment.
    """
    mc.RAY_METRIC_CARDINALITY_LEVEL = level
    mc._CARDINALITY_LEVEL = None
    if alias is None:
        os.environ.pop(ALIAS_ENV, None)
    else:
        os.environ[ALIAS_ENV] = alias
    importlib.reload(serve_constants)
    return serve_constants.RAY_SERVE_CONTROLLER_METRICS_INCLUDE_HIGH_CARDINALITY_TAGS


@pytest.fixture(autouse=True)
def restore_constants():
    orig_level = mc.RAY_METRIC_CARDINALITY_LEVEL
    orig_cache = mc._CARDINALITY_LEVEL
    orig_alias = os.environ.get(ALIAS_ENV)
    yield
    mc.RAY_METRIC_CARDINALITY_LEVEL = orig_level
    mc._CARDINALITY_LEVEL = orig_cache
    if orig_alias is None:
        os.environ.pop(ALIAS_ENV, None)
    else:
        os.environ[ALIAS_ENV] = orig_alias
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        importlib.reload(serve_constants)


@pytest.mark.parametrize(
    "level,include",
    [("legacy", True), ("recommended", True), ("low", False)],
)
def test_default_follows_cardinality_level(level, include):
    # The cardinality level is the canonical control: low drops the tags.
    assert _reload(level) is include


def test_alias_env_var_overrides_level():
    # The deprecated alias still wins when set explicitly.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        assert _reload("low", alias="1") is True
        assert _reload("recommended", alias="0") is False


def test_alias_env_var_emits_deprecation_warning():
    with pytest.warns(DeprecationWarning, match="RAY_metric_cardinality_level"):
        _reload("recommended", alias="0")


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
