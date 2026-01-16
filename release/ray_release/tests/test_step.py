import sys
from unittest.mock import patch

import pytest

from ray_release.bazel import bazel_runfile
from ray_release.buildkite.step import get_step, get_step_for_test_group
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
            "cluster": {
                "byod": {},
            },
            "run": {
                "script": "python test.py",
                "timeout": 100,
                "num_retries": 3,
            },
        }
    )
    test.update(val)
    return test


@patch("ray_release.test.Test.update_from_s3", return_value=None)
def test_get_step(mock):
    with patch.dict("os.environ", {"RAYCI_BUILD_ID": "a1b2c3d4"}):
        step = get_step(_stub_test({}), run_id=2)
    assert step["label"] == "test (None) (2)"
    assert step["retry"]["automatic"][0]["limit"] == 3


@patch("ray_release.test.Test.update_from_s3", return_value=None)
def test_get_step_for_test_group(mock):
    grouped_tests = {
        "group1": [
            (_stub_test({"name": "test1", "repeated_run": 3}), False),
            (_stub_test({"name": "test2"}), False),
        ],
        "group2": [(_stub_test({"name": "test3"}), False)],
    }
    with patch.dict("os.environ", {"RAYCI_BUILD_ID": "a1b2c3d4"}):
        steps = get_step_for_test_group(grouped_tests)
    assert len(steps) == 2
    assert steps[0]["group"] == "group1"
    assert [step["label"] for step in steps[0]["steps"]] == [
        "test1 (None) (0)",
        "test1 (None) (1)",
        "test1 (None) (2)",
        "test2 (None) (0)",
    ]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
