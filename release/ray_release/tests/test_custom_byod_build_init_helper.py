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
    _get_step_image_type,
    _get_step_name,
    _short_tag,
    build_short_gpu_map,
    collect_needed_variants,
    create_custom_build_yaml,
    filter_release_build_yaml,
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
        # ray cuda → anyscalecudabuild (gpu + python)
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
        # Unknown gpu falls back to the raw value
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
        "unknown-gpu",
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


def _make_test(name="t", python="3.10", byod_type="cpu"):
    """Helper to create a minimal Test with byod config."""
    t = Test(
        name=name,
        frequency="manual",
        group="g",
        team="team",
        working_dir=".",
    )
    if python != "3.10":
        t["python"] = python
    if byod_type != "cpu":
        t["cluster"] = {"byod": {"type": byod_type}}
    return t


class TestCollectNeededVariants:
    def test_single_cpu_test(self):
        tests = [_make_test(python="3.10", byod_type="cpu")]
        py, types, cuda = collect_needed_variants(tests, _GPU_MAP)
        assert py == {"3.10"}
        assert types == {"ray-cpu"}
        assert cuda == {}

    def test_single_cuda_test(self):
        tests = [_make_test(python="3.11", byod_type="cu123")]
        py, types, cuda = collect_needed_variants(tests, _GPU_MAP)
        assert py == {"3.11"}
        assert types == {"ray-cuda"}
        assert cuda == {"ray-cuda": {"cu12.3.2-cudnn9"}}

    def test_cuda_cu128(self):
        tests = [_make_test(python="3.10", byod_type="cu128")]
        py, types, cuda = collect_needed_variants(tests, _GPU_MAP)
        assert py == {"3.10"}
        assert types == {"ray-cuda"}
        assert cuda == {"ray-cuda": {"cu12.8.1-cudnn"}}

    def test_llm_test(self):
        tests = [_make_test(python="3.12", byod_type="llm-cu130")]
        py, types, cuda = collect_needed_variants(tests, _GPU_MAP)
        assert py == {"3.12"}
        assert types == {"ray-llm"}
        assert cuda == {"ray-llm": {"cu13.0.0-cudnn"}}

    def test_llm_cu128(self):
        tests = [_make_test(python="3.11", byod_type="llm-cu128")]
        py, types, cuda = collect_needed_variants(tests, _GPU_MAP)
        assert py == {"3.11"}
        assert types == {"ray-llm"}
        assert cuda == {"ray-llm": {"cu12.8.1-cudnn"}}

    def test_ml_gpu_test_keeps_all_cuda(self):
        tests = [_make_test(python="3.10", byod_type="gpu")]
        py, types, cuda = collect_needed_variants(tests, _GPU_MAP)
        assert py == {"3.10"}
        assert types == {"ray-ml"}
        assert cuda == {"ray-ml": None}

    def test_mixed_tests(self):
        tests = [
            _make_test(name="a", python="3.10", byod_type="cpu"),
            _make_test(name="b", python="3.12", byod_type="llm-cu130"),
        ]
        py, types, cuda = collect_needed_variants(tests, _GPU_MAP)
        assert py == {"3.10", "3.12"}
        assert types == {"ray-cpu", "ray-llm"}
        assert cuda == {"ray-llm": {"cu13.0.0-cudnn"}}


class TestGetStepImageType:
    def test_cpu_by_name(self):
        assert _get_step_image_type({"name": "raycpubaseextra-testdeps"}) == "ray-cpu"

    def test_cuda_by_name(self):
        assert _get_step_image_type({"name": "raycudabaseextra-testdeps"}) == "ray-cuda"

    def test_llm_by_name(self):
        assert _get_step_image_type({"name": "ray-llmbaseextra-testdeps"}) == "ray-llm"

    def test_ml_by_name(self):
        assert (
            _get_step_image_type({"name": "ray-mlcudabaseextra-testdeps"}) == "ray-ml"
        )

    def test_cpu_by_key(self):
        assert _get_step_image_type({"key": "anyscalecpubuild"}) == "ray-cpu"

    def test_cuda_by_key(self):
        assert _get_step_image_type({"key": "anyscalecudabuild"}) == "ray-cuda"

    def test_llm_by_key(self):
        assert _get_step_image_type({"key": "anyscalellmbuild"}) == "ray-llm"

    def test_ml_by_key(self):
        assert _get_step_image_type({"key": "anyscalemlbuild"}) == "ray-ml"

    def test_image_type_env(self):
        assert _get_step_image_type({"env": {"IMAGE_TYPE": "ray-llm"}}) == "ray-llm"
        assert _get_step_image_type({"env": {"IMAGE_TYPE": "ray-ml"}}) == "ray-ml"

    def test_unknown(self):
        assert _get_step_image_type({"name": "forge"}) is None


# A minimal build.rayci.yml for testing filter_release_build_yaml.
_SAMPLE_BUILD_YAML = {
    "group": "release build",
    "steps": [
        {
            "name": "raycpubaseextra-testdeps",
            "env": {"IMAGE_TYPE": "ray"},
            "array": {
                "python": ["3.10", "3.11", "3.12", "3.13"],
            },
        },
        {
            "name": "raycudabaseextra-testdeps",
            "env": {"IMAGE_TYPE": "ray"},
            "array": {
                "python": ["3.10", "3.11", "3.12", "3.13"],
                "cuda": ["12.3.2-cudnn9"],
                "adjustments": [
                    {"with": {"python": "3.12", "cuda": "13.0.0-cudnn"}},
                ],
            },
        },
        {
            "name": "ray-llmbaseextra-testdeps",
            "env": {"IMAGE_TYPE": "ray-llm"},
            "array": {
                "python": ["3.12"],
                "cuda": ["13.0.0-cudnn"],
                "adjustments": [
                    {"with": {"python": "3.11", "cuda": "12.8.1-cudnn"}},
                ],
            },
        },
        {
            "name": "ray-mlcudabaseextra-testdeps",
            "env": {"IMAGE_TYPE": "ray-ml"},
            "array": {
                "python": ["3.10"],
                "cuda": ["12.1.1-cudnn8"],
            },
        },
        {
            "key": "anyscalecpubuild",
            "array": {
                "python": ["3.10", "3.11", "3.12", "3.13"],
            },
        },
        {
            "key": "anyscalecudabuild",
            "array": {
                "gpu": ["cu12.3.2-cudnn9"],
                "python": ["3.10", "3.11", "3.12", "3.13"],
                "adjustments": [
                    {"with": {"python": "3.12", "gpu": "cu13.0.0-cudnn"}},
                ],
            },
        },
        {
            "key": "anyscalellmbuild",
            "array": {
                "gpu": ["cu13.0.0-cudnn"],
                "python": ["3.12"],
                "adjustments": [
                    {"with": {"python": "3.11", "gpu": "cu12.8.1-cudnn"}},
                ],
            },
        },
        {
            "key": "anyscalemlbuild",
            "array": {
                "python": ["3.10"],
            },
        },
    ],
}


class TestFilterReleaseBuildYaml:
    def _write_and_filter(self, tmpdir, needed_python, needed_image_types, cuda_needs):
        path = os.path.join(tmpdir, "build.rayci.yml")
        with open(path, "w") as f:
            yaml.dump(_SAMPLE_BUILD_YAML, f, default_flow_style=False, sort_keys=False)
        filter_release_build_yaml(path, needed_python, needed_image_types, cuda_needs)
        with open(path) as f:
            return yaml.safe_load(f)

    def test_cpu_py310_only(self):
        """Simulates name:many_nodes.aws — only py310 cpu needed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = self._write_and_filter(
                tmpdir,
                needed_python={"3.10"},
                needed_image_types={"ray-cpu"},
                cuda_needs={},
            )
        step_ids = [s.get("name") or s.get("key") for s in result["steps"]]
        assert "raycpubaseextra-testdeps" in step_ids
        assert "anyscalecpubuild" in step_ids
        # CUDA, LLM, ML steps should be removed.
        assert "raycudabaseextra-testdeps" not in step_ids
        assert "ray-llmbaseextra-testdeps" not in step_ids
        assert "ray-mlcudabaseextra-testdeps" not in step_ids
        assert "anyscalecudabuild" not in step_ids
        assert "anyscalellmbuild" not in step_ids
        assert "anyscalemlbuild" not in step_ids
        # Python should be trimmed to only 3.10.
        cpu_step = next(
            s for s in result["steps"] if s.get("name") == "raycpubaseextra-testdeps"
        )
        assert cpu_step["array"]["python"] == ["3.10"]

    def test_llm_cu130_py312(self):
        """Only ray-llm with cu130 and py312."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = self._write_and_filter(
                tmpdir,
                needed_python={"3.12"},
                needed_image_types={"ray-llm"},
                cuda_needs={"ray-llm": {"cu13.0.0-cudnn"}},
            )
        step_ids = [s.get("name") or s.get("key") for s in result["steps"]]
        assert "ray-llmbaseextra-testdeps" in step_ids
        assert "anyscalellmbuild" in step_ids
        # Non-LLM steps removed.
        assert "raycpubaseextra-testdeps" not in step_ids
        assert "raycudabaseextra-testdeps" not in step_ids
        # The LLM step should have only cu13 base, no cu128 adjustment.
        llm_step = next(
            s for s in result["steps"] if s.get("name") == "ray-llmbaseextra-testdeps"
        )
        assert llm_step["array"]["cuda"] == ["13.0.0-cudnn"]
        assert "adjustments" not in llm_step["array"]

    def test_llm_adjustment_only(self):
        """ray-llm with cu128/py311 — only the adjustment matches."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = self._write_and_filter(
                tmpdir,
                needed_python={"3.11"},
                needed_image_types={"ray-llm"},
                cuda_needs={"ray-llm": {"cu12.8.1-cudnn"}},
            )
        step_ids = [s.get("name") or s.get("key") for s in result["steps"]]
        assert "ray-llmbaseextra-testdeps" in step_ids
        assert "anyscalellmbuild" in step_ids
        # The base arrays should be reconstructed from adjustment values.
        llm_step = next(
            s for s in result["steps"] if s.get("name") == "ray-llmbaseextra-testdeps"
        )
        assert "3.11" in llm_step["array"]["python"]
        assert "12.8.1-cudnn" in llm_step["array"]["cuda"]

    def test_ml_gpu_keeps_all_cuda(self):
        """ray-ml with byod_type 'gpu' (no specific CUDA) keeps all variants."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = self._write_and_filter(
                tmpdir,
                needed_python={"3.10"},
                needed_image_types={"ray-ml"},
                cuda_needs={"ray-ml": None},
            )
        step_ids = [s.get("name") or s.get("key") for s in result["steps"]]
        assert "ray-mlcudabaseextra-testdeps" in step_ids
        assert "anyscalemlbuild" in step_ids
        ml_step = next(
            s
            for s in result["steps"]
            if s.get("name") == "ray-mlcudabaseextra-testdeps"
        )
        assert ml_step["array"]["cuda"] == ["12.1.1-cudnn8"]

    def test_full_build_keeps_everything(self):
        """When all image types and python versions are needed."""
        all_python = {"3.10", "3.11", "3.12", "3.13"}
        all_types = {"ray-cpu", "ray-cuda", "ray-ml", "ray-llm"}
        with tempfile.TemporaryDirectory() as tmpdir:
            result = self._write_and_filter(
                tmpdir,
                needed_python=all_python,
                needed_image_types=all_types,
                cuda_needs={
                    "ray-cuda": None,
                    "ray-llm": None,
                    "ray-ml": None,
                },
            )
        assert len(result["steps"]) == len(_SAMPLE_BUILD_YAML["steps"])

    def test_empty_array_removes_step(self):
        """If no python versions match, step is removed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = self._write_and_filter(
                tmpdir,
                needed_python={"3.14"},
                needed_image_types={"ray-cpu"},
                cuda_needs={},
            )
        step_ids = [s.get("name") or s.get("key") for s in result["steps"]]
        assert "raycpubaseextra-testdeps" not in step_ids
        assert "anyscalecpubuild" not in step_ids


# A minimal _images.rayci.yml for testing filter_by_image_type=False.
_SAMPLE_IMAGES_YAML = {
    "group": "images",
    "steps": [
        {
            "name": "raycpubase",
            "array": {"python": ["3.10", "3.11", "3.12", "3.13"]},
        },
        {
            "name": "raycpubaseextra",
            "env": {"IMAGE_TYPE": "ray"},
            "array": {"python": ["3.10", "3.11", "3.12", "3.13"]},
        },
        {
            "name": "raycudabase",
            "array": {
                "python": ["3.10", "3.11", "3.12", "3.13"],
                "cuda": [
                    "12.3.2-cudnn9",
                    "12.8.1-cudnn",
                    "13.0.0-cudnn",
                ],
            },
        },
        {
            "name": "raycudabaseextra",
            "env": {"IMAGE_TYPE": "ray"},
            "array": {
                "python": ["3.10", "3.11", "3.12", "3.13"],
                "cuda": [
                    "12.3.2-cudnn9",
                    "12.8.1-cudnn",
                    "13.0.0-cudnn",
                ],
            },
        },
        {
            "name": "ray-llmbase",
            "array": {
                "python": ["3.12"],
                "cuda": ["13.0.0-cudnn"],
                "adjustments": [
                    {"with": {"python": "3.11", "cuda": "12.8.1-cudnn"}},
                ],
            },
        },
        {
            "name": "ray-mlcudabase",
            "array": {"python": ["3.10"], "cuda": ["12.1.1-cudnn8"]},
        },
    ],
}

# A minimal _wheel-build.rayci.yml.
_SAMPLE_WHEEL_YAML = {
    "group": "wheel build",
    "steps": [
        {
            "name": "ray-core-build",
            "array": {"python": ["3.10", "3.11", "3.12", "3.13", "3.14"]},
        },
        {"name": "ray-dashboard-build"},
        {
            "name": "ray-wheel-build",
            "array": {"python": ["3.10", "3.11", "3.12", "3.13", "3.14"]},
        },
    ],
}


class TestFilterSharedYaml:
    """Tests for filter_by_image_type=False (shared _images / _wheel-build)."""

    def _write_and_filter(self, tmpdir, sample, needed_python, cuda_needs):
        path = os.path.join(tmpdir, "test.rayci.yml")
        with open(path, "w") as f:
            yaml.dump(sample, f, default_flow_style=False, sort_keys=False)
        filter_release_build_yaml(
            path,
            needed_python,
            needed_image_types=set(),
            cuda_needs=cuda_needs,
            filter_by_image_type=False,
        )
        with open(path) as f:
            return yaml.safe_load(f)

    def test_images_cpu_py310_only(self):
        """CPU-only test: all CUDA steps removed, python trimmed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = self._write_and_filter(
                tmpdir, _SAMPLE_IMAGES_YAML, {"3.10"}, cuda_needs={}
            )
        step_names = [s["name"] for s in result["steps"]]
        # CPU base images kept with python 3.10.
        assert "raycpubase" in step_names
        assert "raycpubaseextra" in step_names
        cpu = next(s for s in result["steps"] if s["name"] == "raycpubase")
        assert cpu["array"]["python"] == ["3.10"]
        # All CUDA steps removed (cuda filtered to empty).
        assert "raycudabase" not in step_names
        assert "raycudabaseextra" not in step_names
        assert "ray-llmbase" not in step_names
        assert "ray-mlcudabase" not in step_names

    def test_images_llm_cu130_py312(self):
        """LLM test: CUDA filtered to cu130 only."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = self._write_and_filter(
                tmpdir,
                _SAMPLE_IMAGES_YAML,
                {"3.12"},
                cuda_needs={"ray-llm": {"cu13.0.0-cudnn"}},
            )
        step_names = [s["name"] for s in result["steps"]]
        assert "raycudabase" in step_names
        cuda_base = next(s for s in result["steps"] if s["name"] == "raycudabase")
        assert cuda_base["array"]["python"] == ["3.12"]
        assert cuda_base["array"]["cuda"] == ["13.0.0-cudnn"]
        # LLM base kept, adjustment (cu128/py311) removed.
        assert "ray-llmbase" in step_names
        llm = next(s for s in result["steps"] if s["name"] == "ray-llmbase")
        assert llm["array"]["cuda"] == ["13.0.0-cudnn"]
        assert "adjustments" not in llm["array"]

    def test_images_ml_gpu_keeps_all_cuda(self):
        """ray-ml 'gpu' → cuda_needs has None → all CUDA kept."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = self._write_and_filter(
                tmpdir,
                _SAMPLE_IMAGES_YAML,
                {"3.10"},
                cuda_needs={"ray-ml": None},
            )
        step_names = [s["name"] for s in result["steps"]]
        assert "raycudabase" in step_names
        cuda_base = next(s for s in result["steps"] if s["name"] == "raycudabase")
        assert cuda_base["array"]["python"] == ["3.10"]
        # All 3 CUDA variants kept.
        assert len(cuda_base["array"]["cuda"]) == 3

    def test_wheel_build_py310_only(self):
        """Wheel builds trimmed to needed python; non-array steps kept."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = self._write_and_filter(
                tmpdir, _SAMPLE_WHEEL_YAML, {"3.10"}, cuda_needs={}
            )
        step_names = [s["name"] for s in result["steps"]]
        assert "ray-core-build" in step_names
        assert "ray-dashboard-build" in step_names
        assert "ray-wheel-build" in step_names
        core = next(s for s in result["steps"] if s["name"] == "ray-core-build")
        assert core["array"]["python"] == ["3.10"]
        wheel = next(s for s in result["steps"] if s["name"] == "ray-wheel-build")
        assert wheel["array"]["python"] == ["3.10"]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
