import sys
import os
import pytest

from ray_release.test import (
    Test,
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


def test_get_anyscale_byod_image():
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
