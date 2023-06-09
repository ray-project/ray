import sys
import os
from unittest import mock

import pytest

from ray_release.test import (
    Test,
    _convert_env_list_to_dict,
    DATAPLANE_ECR,
    DATAPLANE_ECR_REPO,
    DATAPLANE_ECR_ML_REPO,
)


def _stub_test(val: dict) -> Test:
    test = Test(
        {
            "name": "test",
            "cluster": {},
        }
    )
    test.update(val)
    return test


def test_is_byod_cluster():
    assert not _stub_test({}).is_byod_cluster()
    assert _stub_test({"cluster": {"byod": {}}}).is_byod_cluster()
    assert _stub_test({"cluster": {"byod": {"type": "gpu"}}}).is_byod_cluster()


def test_convert_env_list_to_dict():
    with mock.patch.dict(os.environ, {"ENV": "env"}):
        assert _convert_env_list_to_dict(["a=b", "c=d=e", "ENV"]) == {
            "a": "b",
            "c": "d=e",
            "ENV": "env",
        }


def test_get_python_version():
    assert _stub_test({}).get_python_version() == "3.7"
    assert _stub_test({"python": "3.8"}).get_python_version() == "3.8"


def test_get_ray_image():
    os.environ["BUILDKITE_BRANCH"] = "master"
    os.environ["BUILDKITE_COMMIT"] = "1234567890"
    assert _stub_test({"python": "3.8"}).get_ray_image() == "rayproject/ray:123456-py38"
    assert (
        _stub_test(
            {
                "python": "3.8",
                "cluster": {
                    "byod": {
                        "type": "gpu",
                    }
                },
            }
        ).get_ray_image()
        == "rayproject/ray-ml:123456-py38-gpu"
    )
    os.environ["BUILDKITE_BRANCH"] = "releases/1.0.0"
    assert _stub_test({}).get_ray_image() == "rayproject/ray:1.0.0.123456-py37"


def test_get_anyscale_byod_image():
    os.environ["BUILDKITE_BRANCH"] = "master"
    os.environ["BUILDKITE_COMMIT"] = "1234567890"
    assert (
        _stub_test({}).get_anyscale_byod_image()
        == f"{DATAPLANE_ECR}/{DATAPLANE_ECR_REPO}:123456-py37"
    )
    assert (
        _stub_test(
            {
                "python": "3.8",
                "cluster": {
                    "byod": {
                        "type": "gpu",
                    }
                },
            }
        ).get_anyscale_byod_image()
        == f"{DATAPLANE_ECR}/{DATAPLANE_ECR_ML_REPO}:123456-py38-gpu"
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
