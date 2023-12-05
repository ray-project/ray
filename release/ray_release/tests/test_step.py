import sys

import pytest

from ray_release.bazel import bazel_runfile
from ray_release.buildkite.step import get_step
from ray_release.configs.global_config import init_global_config
from ray_release.test import Test

init_global_config(bazel_runfile("release/ray_release/configs/oss_config.yaml"))


def _stub_test(val: dict) -> Test:
    """
    A helper function to create a test object with a given dictionary.
    """
    test = Test(
        {
            "name": "test",
            "cluster": {},
        }
    )
    test.update(val)
    return test


def test_get_step():
    step = get_step(_stub_test({}), run_id=2)
    assert step["label"] == "test (None) (2)"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
