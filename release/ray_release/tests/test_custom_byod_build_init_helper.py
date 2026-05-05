import os
import sys
import tempfile
from pathlib import Path
from unittest import mock

import pytest
import yaml

from ray_release.bazel import bazel_runfile
from ray_release.configs.global_config import get_global_config, init_global_config
from ray_release.custom_byod_build_init_helper import (
    _get_step_name,
    _short_tag,
    build_short_gpu_map,
    create_custom_build_yaml,
    generate_custom_build_step_key,
    get_prerequisite_step,
)
from ray_release.test import Test
from ray_release.util import AZURE_REGISTRY_NAME

init_global_config(bazel_runfile("release/ray_release/configs/oss_config.yaml"))
_GPU_MAP = build_short_gpu_map(bazel_runfile("ray-images.json"))


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
            None,
        ),
        (
            "ray-project/ray-ml:abc123-custom1",
            "ray-project/ray-ml:abc123-base",
            "",
            None,
            None,
        ),
        (
            "ray-project/ray-ml:abc123-py37-cpu-custom-abcdef123456789abc123456789",
            "ray-project/ray-ml:abc123-py37-cpu-base",
            "custom_script.sh",
            None,
            None,
        ),  # longer than 40 chars
        (
            "ray-project/ray-ml:abc123-py37-cpu-custom-abcdef123456789abc987654321",
            "ray-project/ray-ml:abc123-py37-cpu-base",
            "custom_script.sh",
            "python_depset.lock",
            None,
        ),
        (
            "custom_ecr/ray-ml:abc123-py37-cpu-custom-abcdef123456789abc987654321",
            "anyscale/ray:2.50.0-py37-cpu",
            "custom_script.sh",
            "python_depset.lock",
            None,
        ),
        (
            "ray-project/ray-ml:abc123-envonly-abcdef123456789abc000000000",
            "ray-project/ray-ml:abc123-base",
            None,
            None,
            {"MY_ENV": "my_value", "OTHER_ENV": "other_value"},
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
        "custom_ecr/ray-ml:abc123-py37-cpu-custom-abcdef123456789abc987654321": [
            "test_3",
        ],
        "ray-project/ray-ml:abc123-envonly-abcdef123456789abc000000000": [
            "test_4",
        ],
    }
    mock_get_images_from_tests.return_value = (
        custom_byod_images,
        custom_image_test_names_map,
    )
    step_keys = [
        generate_custom_build_step_key(image)
        for image, _, _, _, _ in custom_byod_images
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
        Test(
            name="test_3",
            frequency="manual",
            group="test_group",
            team="test_team",
            working_dir="test_working_dir",
            cluster={
                "ray_version": "2.50.0",
            },
        ),
        Test(
            name="test_4",
            frequency="manual",
            group="test_group",
            team="test_team",
            working_dir="test_working_dir",
        ),
    ]
    with tempfile.TemporaryDirectory() as tmpdir:
        create_custom_build_yaml(
            os.path.join(tmpdir, "custom_byod_build.rayci.yml"),
            tests,
            _GPU_MAP,
        )
        with open(os.path.join(tmpdir, "custom_byod_build.rayci.yml"), "r") as f:
            content = yaml.safe_load(f)
            assert content["group"] == "Custom images build"
            assert len(content["steps"]) == 5
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
                content["steps"][0]["commands"][1]
                == f"bash release/gcloud_docker_login.sh {config['aws2gce_credentials']}"
            )
            assert content["steps"][0]["commands"][4].startswith(
                "az acr login"
            ) and content["steps"][0]["commands"][4].endswith(AZURE_REGISTRY_NAME)
            assert (
                f"--region {config['byod_ecr_region']}"
                in content["steps"][0]["commands"][5]
            )
            assert f"{config['byod_ecr']}" in content["steps"][0]["commands"][5]
            assert (
                f"--image-name {custom_byod_images[0][0]}"
                in content["steps"][0]["commands"][6]
            )
            assert (
                f"--image-name {custom_byod_images[2][0]}"
                in content["steps"][1]["commands"][6]
            )
            assert (
                f"--image-name {custom_byod_images[3][0]}"
                in content["steps"][2]["commands"][6]
            )
            assert content["steps"][3]["depends_on"] == "forge"
            # Verify env-only image step has --env flags
            env_only_cmd = content["steps"][4]["commands"][6]
            assert f"--image-name {custom_byod_images[5][0]}" in env_only_cmd
            assert "--env MY_ENV=my_value" in env_only_cmd
            assert "--env OTHER_ENV=other_value" in env_only_cmd
            # Env-only step should not have --post-build-script or --python-depset
            assert "--post-build-script" not in env_only_cmd
            assert "--python-depset" not in env_only_cmd


_ECR = get_global_config()["byod_ecr"]


@pytest.mark.parametrize(
    ("image", "base_image", "expected"),
    [
        # Anyscale-prefixed base images → "forge"
        ("anyscale/ray:abc123-custom", "anyscale/ray:abc123-base", "forge"),
        ("custom_ecr/ray-ml:abc123-custom", "anyscale/ray:2.50.0-py310-cpu", "forge"),
        # ray-ml: python dimension only
        (
            "ray-project/ray-ml:a-c",
            "ray-project/ray-ml:a-py310-gpu",
            "anyscalemlbuild--python310",
        ),
        (
            "ray-project/ray-ml:a-c",
            "ray-project/ray-ml:a-py311-gpu",
            "anyscalemlbuild--python311",
        ),
        # ray cpu → anyscalecpubuild (python only)
        (
            "ray-project/ray:a-c",
            "ray-project/ray:a-py310-cpu",
            "anyscalecpubuild--python310",
        ),
        (
            "ray-project/ray:a-c",
            "ray-project/ray:a-py313-cpu",
            "anyscalecpubuild--python313",
        ),
        # ray cuda → anyscalecudabuild (platform + python)
        (
            "ray-project/ray:a-c",
            "ray-project/ray:a-py311-cu123",
            "anyscalecudabuild--gpucu1232cudnn9-python311",
        ),
        (
            "ray-project/ray:a-c",
            "ray-project/ray:a-py312-cu130",
            "anyscalecudabuild--gpucu1300cudnn-python312",
        ),
        # ray-llm
        (
            "ray-project/ray-llm:a-c",
            "ray-project/ray-llm:a-py311-cu128",
            "anyscalellmbuild--gpucu1281cudnn-python311",
        ),
        (
            "ray-project/ray-llm:a-c",
            "ray-project/ray-llm:a-py312-cu130",
            "anyscalellmbuild--gpucu1300cudnn-python312",
        ),
        # ECR-prefixed base_image (production format)
        (
            f"{_ECR}/anyscale/ray:bid-py310-cpu-hash",
            f"{_ECR}/anyscale/ray:bid-py310-cpu",
            "anyscalecpubuild--python310",
        ),
        # Build ID with dashes
        (
            "ray-project/ray:a-b-c-x",
            "ray-project/ray:a-b-c-py312-cpu",
            "anyscalecpubuild--python312",
        ),
        # Unknown platform falls back to raw value
        (
            "ray-project/ray:a-c",
            "ray-project/ray:a-py310-cu999",
            "anyscalecudabuild--gpucu999-python310",
        ),
        # No python version → bare config key (cuda is default for ray without suffix)
        ("ray-project/ray:a-c", "ray-project/ray:a-base", "anyscalecudabuild"),
        ("ray-project/ray-ml:a-c", "ray-project/ray-ml:a-base", "anyscalemlbuild"),
    ],
    ids=[
        "forge-bare",
        "forge-versioned",
        "ray-ml-py310",
        "ray-ml-py311",
        "ray-cpu-py310",
        "ray-cpu-py313",
        "ray-cuda-cu123",
        "ray-cuda-cu130",
        "ray-llm-cu128",
        "ray-llm-cu130",
        "ecr-prefix",
        "dashed-build-id",
        "unknown-platform",
        "no-python-ray",
        "no-python-ray-ml",
    ],
)
def test_get_prerequisite_step(image, base_image, expected):
    assert get_prerequisite_step(image, base_image, _GPU_MAP) == expected


@pytest.mark.parametrize(
    "gpu, expected",
    [
        ("cpu", "cpu"),
        ("tpu", "tpu"),
        ("cu12.3.2-cudnn9", "cu123"),
        ("cu11.8.0-cudnn8", "cu118"),
        ("cu12-cudnn9", "cu12"),
    ],
)
def test_short_tag(gpu, expected):
    assert _short_tag(gpu) == expected


def test_short_gpu_map_built_from_ray_images_json():
    """The map built from ray-images.json has no short-tag collisions."""
    ray_images_path = str(Path(bazel_runfile("ray-images.json")))
    gpu_map = build_short_gpu_map(ray_images_path)
    assert "cpu" in gpu_map
    # Verify at least one CUDA gpu is present.
    cuda_entries = {k: v for k, v in gpu_map.items() if k.startswith("cu")}
    assert len(cuda_entries) > 0
    # Verify round-trip: _short_tag(full) maps back to the same key.
    for short, full in gpu_map.items():
        assert (
            _short_tag(full) == short
        ), f"_short_tag({full!r}) = {_short_tag(full)!r}, expected {short!r}"


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
