import os
import sys
import tempfile

import pytest
import yaml

from ray_release.base_image_filter import (
    IMAGE_GROUP_RAY,
    IMAGE_GROUP_RAY_LLM,
    IMAGE_GROUP_RAY_ML,
    _classify_step,
    _filter_matrix,
    create_filtered_build_yaml,
    get_needed_image_groups,
)
from ray_release.bazel import bazel_runfile
from ray_release.configs.global_config import init_global_config
from ray_release.test import Test

init_global_config(bazel_runfile("release/ray_release/configs/oss_config.yaml"))

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SAMPLE_BUILD_YAML = {
    "group": "release build",
    "steps": [
        {
            "name": "raycpubaseextra-testdeps",
            "label": "wanda: ray cpu testdeps",
            "matrix": ["3.10", "3.11", "3.12", "3.13"],
        },
        {
            "name": "raycudabaseextra-testdeps",
            "label": "wanda: ray cuda testdeps",
            "matrix": {
                "setup": {
                    "python": ["3.10", "3.11", "3.12", "3.13"],
                    "cuda": ["12.3.2-cudnn9"],
                },
                "adjustments": [
                    {"with": {"python": "3.12", "cuda": "13.0.0-cudnn"}},
                ],
            },
        },
        {
            "name": "ray-anyscale-cpu-build",
            "label": "wanda: ray-anyscale cpu",
            "matrix": ["3.10", "3.11", "3.12", "3.13"],
        },
        {
            "name": "ray-anyscale-cuda-build",
            "label": "wanda: ray-anyscale cuda",
            "matrix": {
                "setup": {
                    "python": ["3.10", "3.11", "3.12", "3.13"],
                    "cuda": ["12.3.2-cudnn9"],
                },
                "adjustments": [
                    {"with": {"python": "3.12", "cuda": "13.0.0-cudnn"}},
                ],
            },
        },
        {
            "label": "publish: ray-anyscale",
            "key": "anyscalebuild",
            "commands": ["bazel run ..."],
            "depends_on": [
                "ray-anyscale-cpu-build",
                "ray-anyscale-cuda-build",
            ],
            "matrix": {
                "setup": {
                    "python": ["3.10", "3.11", "3.12", "3.13"],
                    "platform": ["cu12.3.2-cudnn9", "cpu"],
                },
                "adjustments": [
                    {"with": {"python": "3.12", "platform": "cu13.0.0-cudnn"}},
                ],
            },
        },
        {
            "name": "ray-llmbaseextra-testdeps",
            "label": "wanda: ray-llm testdeps",
            "matrix": {
                "setup": {"python": ["3.12"], "cuda": ["13.0.0-cudnn"]},
                "adjustments": [
                    {"with": {"python": "3.11", "cuda": "12.8.1-cudnn"}},
                ],
            },
        },
        {
            "name": "ray-llm-anyscale-cuda-build",
            "label": "wanda: ray-llm-anyscale cuda",
            "matrix": {
                "setup": {"python": ["3.12"], "cuda": ["13.0.0-cudnn"]},
                "adjustments": [
                    {"with": {"python": "3.11", "cuda": "12.8.1-cudnn"}},
                ],
            },
        },
        {
            "label": "publish: ray-llm-anyscale",
            "key": "anyscalellmbuild",
            "commands": ["bazel run ..."],
            "matrix": {
                "setup": {"python": ["3.12"], "platform": ["cu13.0.0-cudnn"]},
                "adjustments": [
                    {"with": {"python": "3.11", "platform": "cu12.8.1-cudnn"}},
                ],
            },
        },
        {
            "name": "ray-mlcudabaseextra-testdeps",
            "label": "wanda: ray-ml cuda testdeps",
            "matrix": {
                "setup": {"python": ["3.10"], "cuda": ["12.1.1-cudnn8"]},
            },
        },
        {
            "name": "ray-ml-anyscale-cuda-build",
            "label": "wanda: ray-ml-anyscale cuda",
            "matrix": {
                "setup": {"python": ["3.10"], "cuda": ["12.1.1-cudnn8"]},
            },
        },
        {
            "label": "publish: ray-ml-anyscale",
            "key": "anyscalemlbuild",
            "commands": ["bazel run ..."],
            "matrix": ["3.10"],
        },
    ],
}


def _make_test(**kwargs):
    """Create a minimal Test object."""
    defaults = {
        "name": "test_default",
        "team": "test_team",
        "group": "test_group",
        "frequency": "nightly",
        "working_dir": ".",
        "cluster": {
            "byod": {},
            "cluster_compute": "compute.yaml",
        },
        "run": {"timeout": 100, "script": "echo ok"},
    }
    defaults.update(kwargs)
    return Test(defaults)


def _write_yaml(path, data):
    with open(path, "w") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False)


def _read_yaml(path):
    with open(path) as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# get_needed_image_groups tests
# ---------------------------------------------------------------------------


class TestGetNeededImageGroups:
    def test_cpu_test(self):
        tests = [
            _make_test(cluster={"byod": {"type": "cpu"}, "cluster_compute": "c.yaml"})
        ]
        result = get_needed_image_groups(tests)
        assert IMAGE_GROUP_RAY in result
        assert "3.10" in result[IMAGE_GROUP_RAY]  # default python
        assert IMAGE_GROUP_RAY_ML not in result
        assert IMAGE_GROUP_RAY_LLM not in result

    def test_gpu_test(self):
        tests = [
            _make_test(
                python="3.10",
                cluster={"byod": {"type": "gpu"}, "cluster_compute": "c.yaml"},
            )
        ]
        result = get_needed_image_groups(tests)
        assert IMAGE_GROUP_RAY_ML in result
        assert "3.10" in result[IMAGE_GROUP_RAY_ML]
        assert IMAGE_GROUP_RAY not in result

    def test_llm_test(self):
        tests = [
            _make_test(
                python="3.11",
                cluster={"byod": {"type": "llm-cu128"}, "cluster_compute": "c.yaml"},
            )
        ]
        result = get_needed_image_groups(tests)
        assert IMAGE_GROUP_RAY_LLM in result
        assert "3.11" in result[IMAGE_GROUP_RAY_LLM]
        assert IMAGE_GROUP_RAY not in result

    def test_mixed_groups(self):
        tests = [
            _make_test(
                name="t1",
                cluster={"byod": {"type": "cpu"}, "cluster_compute": "c.yaml"},
            ),
            _make_test(
                name="t2",
                python="3.10",
                cluster={"byod": {"type": "gpu"}, "cluster_compute": "c.yaml"},
            ),
            _make_test(
                name="t3",
                python="3.12",
                cluster={"byod": {"type": "llm-cu130"}, "cluster_compute": "c.yaml"},
            ),
        ]
        result = get_needed_image_groups(tests)
        assert IMAGE_GROUP_RAY in result
        assert IMAGE_GROUP_RAY_ML in result
        assert IMAGE_GROUP_RAY_LLM in result

    def test_skips_dockerhub_tests(self):
        tests = [
            _make_test(
                cluster={
                    "ray_version": "2.50.0",
                    "byod": {},
                    "cluster_compute": "c.yaml",
                },
            )
        ]
        result = get_needed_image_groups(tests)
        assert result == {}

    def test_includes_custom_on_dockerhub(self):
        tests = [
            _make_test(
                python="3.10",
                cluster={
                    "ray_version": "2.50.0",
                    "byod": {"type": "gpu", "post_build_script": "setup.sh"},
                    "cluster_compute": "c.yaml",
                },
            )
        ]
        result = get_needed_image_groups(tests)
        # Custom BYOD on DockerHub still needs the base image build
        assert IMAGE_GROUP_RAY_ML in result

    def test_multiple_python_versions(self):
        tests = [
            _make_test(
                name="t1",
                python="3.10",
                cluster={"byod": {"type": "cpu"}, "cluster_compute": "c.yaml"},
            ),
            _make_test(
                name="t2",
                python="3.12",
                cluster={"byod": {"type": "cpu"}, "cluster_compute": "c.yaml"},
            ),
        ]
        result = get_needed_image_groups(tests)
        assert result[IMAGE_GROUP_RAY] == {"3.10", "3.12"}

    def test_empty_tests(self):
        result = get_needed_image_groups([])
        assert result == {}

    def test_gpu_cu130_maps_to_ray_group(self):
        """gpu-cu130 type is NOT ray-ml, it maps to the ray group."""
        tests = [
            _make_test(
                python="3.12",
                cluster={"byod": {"type": "gpu-cu130"}, "cluster_compute": "c.yaml"},
            )
        ]
        result = get_needed_image_groups(tests)
        assert IMAGE_GROUP_RAY in result
        assert IMAGE_GROUP_RAY_ML not in result


# ---------------------------------------------------------------------------
# _classify_step tests
# ---------------------------------------------------------------------------


class TestClassifyStep:
    def test_ray_steps(self):
        for name in [
            "raycpubaseextra-testdeps",
            "raycudabaseextra-testdeps",
            "ray-anyscale-cpu-build",
            "ray-anyscale-cuda-build",
        ]:
            assert _classify_step({"name": name}) == IMAGE_GROUP_RAY

    def test_ray_publish_step(self):
        assert _classify_step({"key": "anyscalebuild"}) == IMAGE_GROUP_RAY

    def test_ray_llm_steps(self):
        for name in ["ray-llmbaseextra-testdeps", "ray-llm-anyscale-cuda-build"]:
            assert _classify_step({"name": name}) == IMAGE_GROUP_RAY_LLM
        assert _classify_step({"key": "anyscalellmbuild"}) == IMAGE_GROUP_RAY_LLM

    def test_ray_ml_steps(self):
        for name in ["ray-mlcudabaseextra-testdeps", "ray-ml-anyscale-cuda-build"]:
            assert _classify_step({"name": name}) == IMAGE_GROUP_RAY_ML
        assert _classify_step({"key": "anyscalemlbuild"}) == IMAGE_GROUP_RAY_ML

    def test_unknown_step(self):
        assert _classify_step({"name": "some-other-step"}) is None
        assert _classify_step({"label": "something"}) is None


# ---------------------------------------------------------------------------
# _filter_matrix tests
# ---------------------------------------------------------------------------


class TestFilterMatrix:
    def test_simple_list(self):
        result = _filter_matrix(["3.10", "3.11", "3.12", "3.13"], {"3.10", "3.12"})
        assert result == ["3.10", "3.12"]

    def test_simple_list_empty(self):
        result = _filter_matrix(["3.10", "3.11"], {"3.13"})
        assert result is None

    def test_simple_list_single(self):
        result = _filter_matrix(["3.10"], {"3.10"})
        assert result == ["3.10"]

    def test_setup_dict_filters_python(self):
        matrix = {
            "setup": {
                "python": ["3.10", "3.11", "3.12", "3.13"],
                "cuda": ["12.3.2-cudnn9"],
            },
            "adjustments": [
                {"with": {"python": "3.12", "cuda": "13.0.0-cudnn"}},
            ],
        }
        result = _filter_matrix(matrix, {"3.10", "3.12"})
        assert result["setup"]["python"] == ["3.10", "3.12"]
        assert result["setup"]["cuda"] == ["12.3.2-cudnn9"]
        assert len(result["adjustments"]) == 1

    def test_setup_dict_removes_adjustment(self):
        matrix = {
            "setup": {
                "python": ["3.10", "3.11", "3.12", "3.13"],
                "cuda": ["12.3.2-cudnn9"],
            },
            "adjustments": [
                {"with": {"python": "3.12", "cuda": "13.0.0-cudnn"}},
            ],
        }
        result = _filter_matrix(matrix, {"3.10"})
        assert result["setup"]["python"] == ["3.10"]
        assert "adjustments" not in result

    def test_setup_dict_empty(self):
        matrix = {
            "setup": {
                "python": ["3.10"],
                "cuda": ["12.3.2-cudnn9"],
            },
        }
        result = _filter_matrix(matrix, {"3.13"})
        assert result is None

    def test_adjustment_promotion(self):
        """When Python version only exists in adjustments, promote to setup."""
        matrix = {
            "setup": {
                "python": ["3.12"],
                "cuda": ["13.0.0-cudnn"],
            },
            "adjustments": [
                {"with": {"python": "3.11", "cuda": "12.8.1-cudnn"}},
            ],
        }
        result = _filter_matrix(matrix, {"3.11"})
        # 3.12 filtered out of setup, 3.11 promoted from adjustment
        assert result["setup"]["python"] == ["3.11"]
        assert result["setup"]["cuda"] == ["12.8.1-cudnn"]
        assert "adjustments" not in result

    def test_adjustment_promotion_with_remaining_setup(self):
        """When both setup and adjustment versions are needed."""
        matrix = {
            "setup": {
                "python": ["3.12"],
                "cuda": ["13.0.0-cudnn"],
            },
            "adjustments": [
                {"with": {"python": "3.11", "cuda": "12.8.1-cudnn"}},
            ],
        }
        result = _filter_matrix(matrix, {"3.11", "3.12"})
        assert "3.12" in result["setup"]["python"]
        assert len(result["adjustments"]) == 1

    def test_platform_matrix(self):
        """Test filtering a matrix with platform dimension instead of cuda."""
        matrix = {
            "setup": {
                "python": ["3.10", "3.11", "3.12", "3.13"],
                "platform": ["cu12.3.2-cudnn9", "cpu"],
            },
            "adjustments": [
                {"with": {"python": "3.12", "platform": "cu13.0.0-cudnn"}},
            ],
        }
        result = _filter_matrix(matrix, {"3.12"})
        assert result["setup"]["python"] == ["3.12"]
        assert result["setup"]["platform"] == ["cu12.3.2-cudnn9", "cpu"]
        assert len(result["adjustments"]) == 1


# ---------------------------------------------------------------------------
# create_filtered_build_yaml tests
# ---------------------------------------------------------------------------


class TestCreateFilteredBuildYaml:
    def test_removes_ml_and_llm_groups(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "build.rayci.yml")
            _write_yaml(path, SAMPLE_BUILD_YAML)

            tests = [
                _make_test(
                    python="3.10",
                    cluster={"byod": {"type": "cpu"}, "cluster_compute": "c.yaml"},
                )
            ]
            create_filtered_build_yaml(path, tests)

            result = _read_yaml(path)
            assert result["group"] == "release build"

            step_ids = [s.get("name") or s.get("key") for s in result["steps"]]
            # Ray steps should be present
            assert "raycpubaseextra-testdeps" in step_ids
            assert "anyscalebuild" in step_ids
            # ML and LLM steps should be removed
            assert "ray-mlcudabaseextra-testdeps" not in step_ids
            assert "anyscalemlbuild" not in step_ids
            assert "ray-llmbaseextra-testdeps" not in step_ids
            assert "anyscalellmbuild" not in step_ids

    def test_filters_python_versions(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "build.rayci.yml")
            _write_yaml(path, SAMPLE_BUILD_YAML)

            tests = [
                _make_test(
                    python="3.10",
                    cluster={"byod": {"type": "cpu"}, "cluster_compute": "c.yaml"},
                )
            ]
            create_filtered_build_yaml(path, tests)

            result = _read_yaml(path)
            # Check CPU testdeps matrix filtered to py3.10
            cpu_testdeps = next(
                s
                for s in result["steps"]
                if s.get("name") == "raycpubaseextra-testdeps"
            )
            assert cpu_testdeps["matrix"] == ["3.10"]

    def test_empty_groups_produces_empty_steps(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "build.rayci.yml")
            _write_yaml(path, SAMPLE_BUILD_YAML)

            tests = [
                _make_test(
                    cluster={
                        "ray_version": "2.50.0",
                        "byod": {},
                        "cluster_compute": "c.yaml",
                    },
                )
            ]
            create_filtered_build_yaml(path, tests)

            result = _read_yaml(path)
            assert result["group"] == "release build"
            assert result["steps"] == []

    def test_preserves_depends_on(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "build.rayci.yml")
            _write_yaml(path, SAMPLE_BUILD_YAML)

            tests = [
                _make_test(
                    python="3.10",
                    cluster={"byod": {"type": "cpu"}, "cluster_compute": "c.yaml"},
                )
            ]
            create_filtered_build_yaml(path, tests)

            result = _read_yaml(path)
            publish_step = next(
                s for s in result["steps"] if s.get("key") == "anyscalebuild"
            )
            assert publish_step["depends_on"] == [
                "ray-anyscale-cpu-build",
                "ray-anyscale-cuda-build",
            ]

    def test_only_llm_group(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "build.rayci.yml")
            _write_yaml(path, SAMPLE_BUILD_YAML)

            tests = [
                _make_test(
                    python="3.12",
                    cluster={
                        "byod": {"type": "llm-cu130"},
                        "cluster_compute": "c.yaml",
                    },
                )
            ]
            create_filtered_build_yaml(path, tests)

            result = _read_yaml(path)
            step_ids = [s.get("name") or s.get("key") for s in result["steps"]]
            # Only LLM steps should remain
            assert "ray-llmbaseextra-testdeps" in step_ids
            assert "anyscalellmbuild" in step_ids
            # Ray and ML steps removed
            assert "raycpubaseextra-testdeps" not in step_ids
            assert "anyscalebuild" not in step_ids
            assert "anyscalemlbuild" not in step_ids

    def test_all_groups_all_versions(self):
        """When all groups and versions are needed, nothing is removed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "build.rayci.yml")
            _write_yaml(path, SAMPLE_BUILD_YAML)

            tests = [
                _make_test(
                    name="t1",
                    python="3.10",
                    cluster={"byod": {"type": "cpu"}, "cluster_compute": "c.yaml"},
                ),
                _make_test(
                    name="t2",
                    python="3.11",
                    cluster={"byod": {"type": "cpu"}, "cluster_compute": "c.yaml"},
                ),
                _make_test(
                    name="t3",
                    python="3.12",
                    cluster={"byod": {"type": "cpu"}, "cluster_compute": "c.yaml"},
                ),
                _make_test(
                    name="t4",
                    python="3.13",
                    cluster={"byod": {"type": "cpu"}, "cluster_compute": "c.yaml"},
                ),
                _make_test(
                    name="t5",
                    python="3.10",
                    cluster={"byod": {"type": "gpu"}, "cluster_compute": "c.yaml"},
                ),
                _make_test(
                    name="t6",
                    python="3.11",
                    cluster={
                        "byod": {"type": "llm-cu128"},
                        "cluster_compute": "c.yaml",
                    },
                ),
                _make_test(
                    name="t7",
                    python="3.12",
                    cluster={
                        "byod": {"type": "llm-cu130"},
                        "cluster_compute": "c.yaml",
                    },
                ),
            ]
            create_filtered_build_yaml(path, tests)

            result = _read_yaml(path)
            assert len(result["steps"]) == len(SAMPLE_BUILD_YAML["steps"])


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
