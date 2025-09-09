import os
import tempfile
import sys
import pytest
from unittest import mock
import yaml

from ray_release.custom_byod_build_init_helper import create_custom_build_yaml
from ray_release.configs.global_config import init_global_config
from ray_release.bazel import bazel_runfile
from ray_release.test import Test
from ray_release.configs.global_config import get_global_config


init_global_config(bazel_runfile("release/ray_release/configs/oss_config.yaml"))


@mock.patch("ray_release.custom_byod_build_init_helper.get_images_from_tests")
def test_create_custom_build_yaml(mock_get_images_from_tests):
    config = get_global_config()
    custom_byod_images = [
        (
            "ray-project/ray-ml:abc123-custom",
            "ray-project/ray-ml:abc123-base",
            "custom_script.sh",
        ),
        ("ray-project/ray-ml:abc123-custom", "ray-project/ray-ml:abc123-base", ""),
        (
            "ray-project/ray-ml:nightly-py37-cpu-custom-abcdef123456789abc123456789",
            "ray-project/ray-ml:nightly-py37-cpu-base",
            "custom_script.sh",
        ),  # longer than 40 chars
    ]
    mock_get_images_from_tests.return_value = custom_byod_images

    # List of dummy tests
    tests = [
        Test(
            name="test_1",
            frequency="manual",
            group="test_group",
            team="test_team",
            working_dir="test_working_dir",
        ),
        Test(
            name="test_2",
            frequency="manual",
            group="test_group",
            team="test_team",
            working_dir="test_working_dir",
        ),
    ]
    with tempfile.TemporaryDirectory() as tmpdir:
        create_custom_build_yaml(
            os.path.join(tmpdir, "custom_byod_build.rayci.yml"), tests
        )
        with open(os.path.join(tmpdir, "custom_byod_build.rayci.yml"), "r") as f:
            content = yaml.safe_load(f)
            assert content["group"] == "Custom images build"
            assert len(content["steps"]) == 2
            assert (
                f"--region {config['byod_ecr_region']}"
                in content["steps"][0]["commands"][0]
            )
            assert f"{config['byod_ecr']}" in content["steps"][0]["commands"][0]
            assert (
                f"--image-name {custom_byod_images[0][0]}"
                in content["steps"][0]["commands"][1]
            )
            assert (
                f"--image-name {custom_byod_images[2][0]}"
                in content["steps"][1]["commands"][1]
            )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
