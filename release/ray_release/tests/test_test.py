import sys
import os
import pytest

from ray_release.test import Test


def test_get_python_version():
    assert Test().get_python_version() == "3.7"
    assert Test({"python": "3.8"}).get_python_version() == "3.8"


def test_get_ray_image():
    os.environ.pop("BUILDKITE_COMMIT", None)
    assert Test({"python": "3.8"}).get_ray_image() == "rayproject/ray:nightly-py38"
    os.environ["BUILDKITE_COMMIT"] = "1234567890"
    # TODO(can): re-enable this test once we have a custom image
    # assert Test().get_ray_image() == "rayproject/ray:123456-py37"


def test_get_anyscale_byod_image():
    os.environ.pop("BUILDKITE_COMMIT", None)
    assert Test().get_anyscale_byod_image() == "anyscale/ray:nightly-py37"
    os.environ["BUILDKITE_COMMIT"] = "1234567890"
    # TODO(can): re-enable this test once we have a custom image
    # assert Test().get_anyscale_byod_image() == "anyscale/ray:123456-py37"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
