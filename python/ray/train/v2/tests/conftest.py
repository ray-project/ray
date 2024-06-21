import logging

import pytest

from ray.train.v2._internal.constants import V2_ENABLED_ENV_VAR


@pytest.fixture(autouse=True)
def setup_logging():
    logger = logging.getLogger("ray.train")
    orig_level = logger.getEffectiveLevel()
    logger.setLevel(logging.INFO)
    yield
    logger.setLevel(orig_level)


@pytest.fixture(autouse=True)
def enable_v2_feature_flag(monkeypatch):
    monkeypatch.setenv(V2_ENABLED_ENV_VAR, "1")
    yield
