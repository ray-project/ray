"""Tests for ci.ray_ci.ray_image.RayImage."""
import sys

import pytest

from ci.ray_ci.configs import DEFAULT_ARCHITECTURE
from ci.ray_ci.docker_container import RayType
from ci.ray_ci.ray_image import IMAGE_TYPE_CONFIG, RayImage, RayImageError


class TestWandaImageName:
    DEFAULT_TEST_CUDA_PLATFORM = "cu12.1.1-cudnn8"

    @pytest.mark.parametrize(
        ("image_type", "python_version", "platform", "architecture", "expected"),
        [
            # CPU images
            (RayType.RAY, "3.10", "cpu", DEFAULT_ARCHITECTURE, "ray-py3.10-cpu"),
            (RayType.RAY, "3.10", "cpu", "aarch64", "ray-py3.10-cpu-aarch64"),
            (
                RayType.RAY_EXTRA,
                "3.10",
                "cpu",
                DEFAULT_ARCHITECTURE,
                "ray-extra-py3.10-cpu",
            ),
            # CUDA images
            (
                RayType.RAY,
                "3.11",
                DEFAULT_TEST_CUDA_PLATFORM,
                DEFAULT_ARCHITECTURE,
                f"ray-py3.11-{DEFAULT_TEST_CUDA_PLATFORM}",
            ),
            (
                RayType.RAY,
                "3.11",
                DEFAULT_TEST_CUDA_PLATFORM,
                "aarch64",
                f"ray-py3.11-{DEFAULT_TEST_CUDA_PLATFORM}-aarch64",
            ),
            (
                RayType.RAY_EXTRA,
                "3.11",
                DEFAULT_TEST_CUDA_PLATFORM,
                DEFAULT_ARCHITECTURE,
                f"ray-extra-py3.11-{DEFAULT_TEST_CUDA_PLATFORM}",
            ),
            (
                RayType.RAY_LLM,
                "3.11",
                DEFAULT_TEST_CUDA_PLATFORM,
                DEFAULT_ARCHITECTURE,
                f"ray-llm-py3.11-{DEFAULT_TEST_CUDA_PLATFORM}",
            ),
            (
                RayType.RAY_LLM_EXTRA,
                "3.11",
                DEFAULT_TEST_CUDA_PLATFORM,
                DEFAULT_ARCHITECTURE,
                f"ray-llm-extra-py3.11-{DEFAULT_TEST_CUDA_PLATFORM}",
            ),
            # ray-ml types
            (RayType.RAY_ML, "3.10", "cpu", DEFAULT_ARCHITECTURE, "ray-ml-py3.10-cpu"),
            (
                RayType.RAY_ML_EXTRA,
                "3.10",
                DEFAULT_TEST_CUDA_PLATFORM,
                DEFAULT_ARCHITECTURE,
                f"ray-ml-extra-py3.10-{DEFAULT_TEST_CUDA_PLATFORM}",
            ),
        ],
    )
    def test_wanda_image_name(
        self, image_type, python_version, platform, architecture, expected
    ):
        img = RayImage(image_type, python_version, platform, architecture)
        assert img.wanda_image_name == expected


class TestArchSuffix:
    def test_default_architecture_empty(self):
        img = RayImage("ray", "3.10", "cpu", DEFAULT_ARCHITECTURE)
        assert img.arch_suffix == ""

    def test_aarch64(self):
        img = RayImage("ray", "3.10", "cpu", "aarch64")
        assert img.arch_suffix == "-aarch64"


class TestRepo:
    @pytest.mark.parametrize(
        ("image_type", "expected"),
        [
            (RayType.RAY, "ray"),
            (RayType.RAY_EXTRA, "ray"),
            (RayType.RAY_ML, "ray-ml"),
            (RayType.RAY_ML_EXTRA, "ray-ml"),
            (RayType.RAY_LLM, "ray-llm"),
            (RayType.RAY_LLM_EXTRA, "ray-llm"),
        ],
    )
    def test_repo(self, image_type, expected):
        img = RayImage(image_type, "3.10", "cpu")
        assert img.repo == expected


class TestVariationSuffix:
    @pytest.mark.parametrize(
        ("image_type", "expected"),
        [
            (RayType.RAY, ""),
            (RayType.RAY_EXTRA, "-extra"),
            (RayType.RAY_ML, ""),
            (RayType.RAY_ML_EXTRA, "-extra"),
            (RayType.RAY_LLM, ""),
            (RayType.RAY_LLM_EXTRA, "-extra"),
        ],
    )
    def test_variation_suffix(self, image_type, expected):
        img = RayImage(image_type, "3.10", "cpu")
        assert img.variation_suffix == expected


class TestValidateValid:
    def test_ray_cpu(self):
        RayImage("ray", "3.10", "cpu").validate()

    def test_ray_cuda(self):
        RayImage("ray", "3.13", "cu12.8.1-cudnn").validate()

    def test_ray_extra(self):
        RayImage("ray-extra", "3.12", "cu11.8.0-cudnn8").validate()

    def test_ray_llm(self):
        RayImage("ray-llm", "3.11", "cu12.8.1-cudnn").validate()

    def test_ray_llm_extra(self):
        RayImage("ray-llm-extra", "3.11", "cu12.8.1-cudnn").validate()

    def test_ray_aarch64(self):
        RayImage("ray", "3.10", "cpu", "aarch64").validate()


class TestValidateInvalid:
    def test_unknown_image_type(self):
        with pytest.raises(RayImageError, match="Unknown image type"):
            RayImage("ray-foo", "3.10", "cpu").validate()

    def test_invalid_python_for_ray_llm(self):
        with pytest.raises(
            RayImageError, match="Invalid python version 3.10 for ray-llm"
        ):
            RayImage("ray-llm", "3.10", "cu12.8.1-cudnn").validate()

    def test_invalid_platform_for_ray_llm(self):
        with pytest.raises(RayImageError, match="Invalid platform cpu for ray-llm"):
            RayImage("ray-llm", "3.11", "cpu").validate()

    def test_invalid_platform_for_ray(self):
        with pytest.raises(RayImageError, match="Invalid platform cu99.9.9 for ray"):
            RayImage("ray", "3.10", "cu99.9.9").validate()

    def test_invalid_architecture_for_ray_llm(self):
        with pytest.raises(
            RayImageError, match="Invalid architecture aarch64 for ray-llm"
        ):
            RayImage("ray-llm", "3.11", "cu12.8.1-cudnn", "aarch64").validate()


class TestImageTypeConfig:
    def test_expected_types_covered(self):
        expected = {"ray", "ray-extra", "ray-llm", "ray-llm-extra"}
        assert set(IMAGE_TYPE_CONFIG.keys()) == expected


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
