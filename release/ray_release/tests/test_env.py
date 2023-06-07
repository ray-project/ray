import os

import pytest
from ray_release.config import DEFAULT_ANYSCALE_PROJECT
from ray_release.env import load_environment, populate_os_env
from ray_release.exception import ReleaseTestConfigError
from ray_release.util import DeferredEnvVar

TEST_ENV_VAR = DeferredEnvVar(
    "TEST_ENV_VAR",
    "value1",
)


def test_deferred_env_var():
    assert str(TEST_ENV_VAR) == "value1"

    os.environ["TEST_ENV_VAR"] = "other2"

    assert str(TEST_ENV_VAR) == "other2"


def test_load_env_invalid():
    with pytest.raises(ReleaseTestConfigError):
        load_environment("invalid")


def test_load_env_changes():
    old_val = str(DEFAULT_ANYSCALE_PROJECT)

    env_dict = load_environment("aws")
    populate_os_env(env_dict)

    new_val = str(DEFAULT_ANYSCALE_PROJECT)

    assert new_val
    assert old_val != new_val
    assert "prj_" in new_val


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
