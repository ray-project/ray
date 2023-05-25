import sys
import os
import pytest
from unittest import mock

from ray_release.result import (
    Result,
    ResultStatus,
)
from ray_release.test import (
    Test,
    TestResult,
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


def test_add_test_result():
    with mock.patch(
        "time.time",
        return_value=0,
    ):
        os.environ["BUILDKITE_COMMIT"] = "1"
        test = _stub_test({})
        test.test_results = []
        assert test.get_test_results() == []
        test.add_test_result(Result(status=ResultStatus.SUCCESS, buildkite_url="url"))
        assert test.get_test_results() == [
            TestResult(status=ResultStatus.SUCCESS, timestamp=0, url="url", commit="1")
        ]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
