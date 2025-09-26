import os
import tempfile
import sys
import pytest
from unittest import mock
import yaml

from ray_release.custom_byod_build_init_helper import (
    create_custom_build_yaml,
    get_prerequisite_step,
    _get_step_name,
    generate_custom_build_step_key,
)
from ray_release.configs.global_config import init_global_config
from ray_release.bazel import bazel_runfile
from ray_release.test import Test
from ray_release.configs.global_config import get_global_config


init_global_config(bazel_runfile("release/ray_release/configs/oss_config.yaml"))


@mock.patch.dict(os.environ, {"RAY_WANT_COMMIT_IN_IMAGE": "abc123"})
@mock.patch("ray_release.custom_byod_build_init_helper.get_images_from_tests")
def test_create_custom_build_yaml(mock_get_images_from_tests):
    config = get_global_config()
    custom_byod_images = [
        (
            "ray-project/ray-ml:abc123-custom-123456789abc123456789",
            "ray-project/ray-ml:abc123-base",
            "custom_script.sh",
            None,
        ),
        (
            "ray-project/ray-ml:abc123-custom1",
            "ray-project/ray-ml:abc123-base",
            "",
            None,
        ),
        (
            "ray-project/ray-ml:abc123-py37-cpu-custom-abcdef123456789abc123456789",
            "ray-project/ray-ml:abc123-py37-cpu-base",
            "custom_script.sh",
            None,
        ),  # longer than 40 chars
        (
            "ray-project/ray-ml:abc123-py37-cpu-custom-abcdef123456789abc987654321",
            "ray-project/ray-ml:abc123-py37-cpu-base",
            "custom_script.sh",
            "python_depset.lock",
        ),
    ]
    custom_image_test_names_map = {
        "ray-project/ray-ml:abc123-custom-123456789abc123456789": ["test_1"],
        "ray-project/ray-ml:abc123-custom1": ["test_2"],
        "ray-project/ray-ml:abc123-py37-cpu-custom-abcdef123456789abc123456789": [
            "test_1",
            "test_2",
        ],
        "ray-project/ray-ml:abc123-py37-cpu-custom-abcdef123456789abc987654321": [
            "test_1",
            "test_2",
        ],
    }
    mock_get_images_from_tests.return_value = (
        custom_byod_images,
        custom_image_test_names_map,
    )
    step_keys = [
        generate_custom_build_step_key(image) for image, _, _, _ in custom_byod_images
    ]
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
            assert len(content["steps"]) == 3
            assert (
                content["steps"][0]["label"]
                == f":tapioca: build custom: ray-ml:custom ({step_keys[0]}) test_1"
            )
            assert (
                content["steps"][1]["label"]
                == f":tapioca: build custom: ray-ml:py37-cpu-custom ({step_keys[2]}) test_1 test_2"
            )
            assert (
                content["steps"][2]["label"]
                == f":tapioca: build custom: ray-ml:py37-cpu-custom ({step_keys[3]}) test_1 test_2"
            )
            assert (
                "export RAY_WANT_COMMIT_IN_IMAGE=abc123"
                in content["steps"][0]["commands"][0]
            )
            assert (
                f"--region {config['byod_ecr_region']}"
                in content["steps"][0]["commands"][3]
            )
            assert f"{config['byod_ecr']}" in content["steps"][0]["commands"][3]
            assert (
                f"--image-name {custom_byod_images[0][0]}"
                in content["steps"][0]["commands"][4]
            )
            assert (
                f"--image-name {custom_byod_images[2][0]}"
                in content["steps"][1]["commands"][4]
            )


def test_get_prerequisite_step():
    config = get_global_config()
    assert (
        get_prerequisite_step("ray-project/ray-ml:abc123-custom")
        == config["release_image_step_ray_ml"]
    )
    assert (
        get_prerequisite_step("ray-project/ray-llm:abc123-custom")
        == config["release_image_step_ray_llm"]
    )
    assert (
        get_prerequisite_step("ray-project/ray:abc123-custom")
        == config["release_image_step_ray"]
    )


def test_get_step_name():
    test_names = [
        "test_1",
        "test_2",
        "test_3",
    ]
    assert (
        _get_step_name(
            "ray-project/ray-ml:a1b2c3d4-py39-cpu-abcdef123456789abc123456789",
            "abc123",
            test_names,
        )
        == ":tapioca: build custom: ray-ml:py39-cpu (abc123) test_1 test_2"
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
