import sys

import pytest

from ci.ray_ci.automation.image_tags_lib import (
    ImageTagsError,
    format_platform_tag,
    format_python_tag,
    get_platform_suffixes,
    get_python_suffixes,
    get_variation_suffix,
)
from ci.ray_ci.configs import DEFAULT_PYTHON_TAG_VERSION
from ci.ray_ci.docker_container import GPU_PLATFORM


class TestFormatPlatformTag:
    @pytest.mark.parametrize(
        ("platform", "expected"),
        [
            ("cpu", "-cpu"),
            ("tpu", "-tpu"),
            ("cu11.7.1-cudnn8", "-cu117"),
            ("cu11.8.0-cudnn8", "-cu118"),
            ("cu12.1.1-cudnn8", "-cu121"),
            ("cu12.3.2-cudnn9", "-cu123"),
            ("cu12.8.1-cudnn", "-cu128"),
        ],
    )
    def test_format_platform_tag(self, platform, expected):
        assert format_platform_tag(platform) == expected

    def test_invalid_platform_raises_error(self):
        with pytest.raises(ImageTagsError):
            format_platform_tag("invalid")


class TestFormatPythonTag:
    @pytest.mark.parametrize(
        ("python_version", "expected"),
        [
            ("3.9", "-py39"),
            ("3.10", "-py310"),
            ("3.11", "-py311"),
            ("3.12", "-py312"),
        ],
    )
    def test_format_python_tag(self, python_version, expected):
        assert format_python_tag(python_version) == expected


class TestGetPythonSuffixes:
    def test_default_python_version_includes_empty(self):
        """DEFAULT_PYTHON_TAG_VERSION gets both explicit and empty suffix."""
        assert DEFAULT_PYTHON_TAG_VERSION == "3.10"
        suffixes = get_python_suffixes("3.10")
        assert suffixes == ["-py310", ""]

    def test_non_default_python_version(self):
        """Non-default python versions only get explicit suffix."""
        assert get_python_suffixes("3.11") == ["-py311"]
        assert get_python_suffixes("3.12") == ["-py312"]


class TestGetPlatformSuffixes:
    def test_cpu_ray_includes_empty(self):
        """ray with cpu gets empty platform suffix."""
        assert get_platform_suffixes("cpu", "ray") == ["-cpu", ""]

    def test_cpu_ray_extra_includes_empty(self):
        """ray-extra with cpu gets empty platform suffix."""
        assert get_platform_suffixes("cpu", "ray-extra") == ["-cpu", ""]

    def test_cpu_ray_ml_no_empty(self):
        """ray-ml with cpu does NOT get empty platform suffix."""
        assert get_platform_suffixes("cpu", "ray-ml") == ["-cpu"]

    def test_cpu_ray_llm_no_empty(self):
        """ray-llm with cpu does NOT get empty platform suffix."""
        assert get_platform_suffixes("cpu", "ray-llm") == ["-cpu"]

    def test_gpu_platform_ray_includes_gpu_alias(self):
        """GPU_PLATFORM with ray gets -gpu alias but not empty suffix."""
        suffixes = get_platform_suffixes(GPU_PLATFORM, "ray")
        assert "-cu121" in suffixes
        assert "-gpu" in suffixes
        assert "" not in suffixes

    def test_gpu_platform_ray_ml_includes_empty(self):
        """GPU_PLATFORM with ray-ml gets -gpu alias AND empty suffix."""
        suffixes = get_platform_suffixes(GPU_PLATFORM, "ray-ml")
        assert "-cu121" in suffixes
        assert "-gpu" in suffixes
        assert "" in suffixes

    def test_gpu_platform_ray_ml_extra_includes_empty(self):
        """GPU_PLATFORM with ray-ml-extra gets -gpu alias AND empty suffix."""
        suffixes = get_platform_suffixes(GPU_PLATFORM, "ray-ml-extra")
        assert "-cu121" in suffixes
        assert "-gpu" in suffixes
        assert "" in suffixes

    def test_tpu_no_aliases(self):
        """TPU gets no aliases."""
        assert get_platform_suffixes("tpu", "ray") == ["-tpu"]
        assert get_platform_suffixes("tpu", "ray-extra") == ["-tpu"]

    def test_non_gpu_platform_cuda_no_aliases(self):
        """Non-GPU_PLATFORM CUDA versions get no aliases."""
        suffixes = get_platform_suffixes("cu12.3.2-cudnn9", "ray")
        assert suffixes == ["-cu123"]

    def test_non_gpu_platform_ray_ml_no_aliases(self):
        """Non-GPU_PLATFORM with ray-ml gets no aliases."""
        suffixes = get_platform_suffixes("cu12.3.2-cudnn9", "ray-ml")
        assert suffixes == ["-cu123"]


class TestGetVariationSuffix:
    @pytest.mark.parametrize(
        ("image_type", "expected"),
        [
            ("ray", ""),
            ("ray-ml", ""),
            ("ray-llm", ""),
            ("ray-extra", "-extra"),
            ("ray-ml-extra", "-extra"),
            ("ray-llm-extra", "-extra"),
        ],
    )
    def test_variation_suffix(self, image_type, expected):
        assert get_variation_suffix(image_type) == expected


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
