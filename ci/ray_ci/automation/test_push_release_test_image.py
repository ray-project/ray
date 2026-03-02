import sys

import pytest

from ci.ray_ci.automation.image_tags_lib import (
    format_platform_tag,
    format_python_tag,
)
from ci.ray_ci.automation.push_release_test_image import ReleaseTestImagePushContext
from ci.ray_ci.configs import DEFAULT_PYTHON_TAG_VERSION
from ci.ray_ci.docker_container import GPU_PLATFORM


def make_ctx(
    image_type: str = "ray",
    python_version: str = "3.11",
    platform: str = "cu12.3.2-cudnn9",
    branch: str = "master",
    commit: str = "abc123def456",
    rayci_build_id: str = "build-123",
    pull_request: str = "false",
) -> ReleaseTestImagePushContext:
    """Helper to create a context with defaults."""
    return ReleaseTestImagePushContext(
        image_type=image_type,
        python_version=python_version,
        platform=platform,
        branch=branch,
        commit=commit,
        rayci_build_id=rayci_build_id,
        pull_request=pull_request,
    )


class TestFormatPythonTag:
    @pytest.mark.parametrize(
        ("python_version", "expected"),
        [
            ("3.10", "-py310"),
            ("3.11", "-py311"),
            ("3.12", "-py312"),
            ("3.9", "-py39"),
        ],
    )
    def test_format_python_tag(self, python_version, expected):
        assert format_python_tag(python_version) == expected


class TestFormatPlatformTag:
    @pytest.mark.parametrize(
        ("platform", "expected"),
        [
            ("cpu", "-cpu"),
            ("cu11.7.1-cudnn8", "-cu117"),
            ("cu11.8.0-cudnn8", "-cu118"),
            ("cu12.1.1-cudnn8", "-cu121"),
            ("cu12.3.2-cudnn9", "-cu123"),
            ("cu12.8.1-cudnn", "-cu128"),
        ],
    )
    def test_format_platform_tag(self, platform, expected):
        assert format_platform_tag(platform) == expected


class TestWandaImageName:
    @pytest.mark.parametrize(
        ("python_version", "platform", "image_type", "expected"),
        [
            ("3.10", "cpu", "ray", "ray-anyscale-py3.10-cpu"),
            ("3.11", "cu12.1.1-cudnn8", "ray", "ray-anyscale-py3.11-cu12.1.1-cudnn8"),
            ("3.10", "cpu", "ray-llm", "ray-llm-anyscale-py3.10-cpu"),
            (
                "3.11",
                "cu12.8.1-cudnn",
                "ray-llm",
                "ray-llm-anyscale-py3.11-cu12.8.1-cudnn",
            ),
            ("3.10", "cpu", "ray-ml", "ray-ml-anyscale-py3.10-cpu"),
            (
                "3.11",
                "cu12.3.2-cudnn9",
                "ray-ml",
                "ray-ml-anyscale-py3.11-cu12.3.2-cudnn9",
            ),
        ],
    )
    def test_wanda_image_name(self, python_version, platform, image_type, expected):
        ctx = make_ctx(
            python_version=python_version, platform=platform, image_type=image_type
        )
        assert ctx.wanda_image_name() == expected


class TestDestinationTags:
    def test_master_branch_tags(self):
        ctx = make_ctx(
            python_version="3.11",
            platform="cu12.3.2-cudnn9",
            image_type="ray",
            branch="master",
            commit="abc123def456",
            rayci_build_id="build-123",
        )
        tags = ctx.destination_tags()

        assert tags == [
            "abc123-py311-cu123",
            "build-123-py311-cu123",
        ]

    def test_release_branch_tags(self):
        ctx = make_ctx(
            python_version="3.11",
            platform="cu12.3.2-cudnn9",
            image_type="ray",
            branch="releases/2.44.0",
            commit="abc123def456",
            rayci_build_id="build-456",
        )
        tags = ctx.destination_tags()

        assert tags == [
            "2.44.0.abc123-py311-cu123",
            "build-456-py311-cu123",
        ]

    def test_gpu_platform_includes_alias(self):
        """GPU_PLATFORM gets -gpu alias, other platforms do not."""
        gpu_ctx = make_ctx(
            python_version="3.11", platform=GPU_PLATFORM, image_type="ray"
        )
        other_cuda_ctx = make_ctx(
            python_version="3.11", platform="cu12.3.2-cudnn9", image_type="ray"
        )
        cpu_ctx = make_ctx(python_version="3.11", platform="cpu", image_type="ray")

        gpu_tags = gpu_ctx.destination_tags()
        other_cuda_tags = other_cuda_ctx.destination_tags()
        cpu_tags = cpu_ctx.destination_tags()

        assert any("-gpu" in tag for tag in gpu_tags)
        assert not any("-gpu" in tag for tag in other_cuda_tags)
        assert not any("-gpu" in tag for tag in cpu_tags)

    def test_ray_ml_gpu_platform_includes_empty_platform_suffix(self):
        """ray-ml with GPU_PLATFORM gets -gpu alias AND empty platform suffix."""
        ctx = make_ctx(
            python_version="3.11", platform=GPU_PLATFORM, image_type="ray-ml"
        )
        tags = ctx.destination_tags()

        # Should have: -cu121, -gpu, and empty suffix (no platform)
        assert "abc123-py311-cu121" in tags
        assert "abc123-py311-gpu" in tags
        assert "abc123-py311" in tags
        assert "build-123-py311-cu121" in tags
        assert "build-123-py311-gpu" in tags
        assert "build-123-py311" in tags

    def test_ray_ml_non_gpu_platform_no_empty_suffix(self):
        """ray-ml with non-GPU_PLATFORM does NOT get empty suffix."""
        ctx = make_ctx(
            python_version="3.11", platform="cu12.3.2-cudnn9", image_type="ray-ml"
        )
        tags = ctx.destination_tags()

        assert tags == [
            "abc123-py311-cu123",
            "build-123-py311-cu123",
        ]

    def test_pr_branch_tags(self):
        ctx = make_ctx(
            python_version="3.12",
            platform="cu12.3.2-cudnn9",
            image_type="ray",
            branch="feature-branch",
            commit="abc123def456",
            rayci_build_id="build-789",
            pull_request="123",
        )
        tags = ctx.destination_tags()

        assert tags == [
            "pr-123.abc123-py312-cu123",
            "build-789-py312-cu123",
        ]

    def test_non_pr_feature_branch_tags(self):
        ctx = make_ctx(
            python_version="3.11",
            platform="cu12.3.2-cudnn9",
            image_type="ray",
            branch="feature-branch",
            commit="abc123def456",
            rayci_build_id="build-789",
            pull_request="false",
        )
        tags = ctx.destination_tags()

        assert tags == [
            "abc123-py311-cu123",
            "build-789-py311-cu123",
        ]

    def test_default_python_version_includes_empty_suffix(self):
        """DEFAULT_PYTHON_TAG_VERSION (3.10) gets empty python suffix."""
        assert DEFAULT_PYTHON_TAG_VERSION == "3.10"
        ctx = make_ctx(
            python_version="3.10", platform="cu12.3.2-cudnn9", image_type="ray"
        )
        tags = ctx.destination_tags()

        # Should have both -py310 and empty python suffix
        assert "abc123-py310-cu123" in tags
        assert "abc123-cu123" in tags
        assert "build-123-py310-cu123" in tags
        assert "build-123-cu123" in tags

    def test_cpu_ray_includes_empty_platform_suffix(self):
        """ray with cpu gets empty platform suffix."""
        ctx = make_ctx(python_version="3.11", platform="cpu", image_type="ray")
        tags = ctx.destination_tags()

        # Should have both -cpu and empty platform suffix
        assert "abc123-py311-cpu" in tags
        assert "abc123-py311" in tags
        assert "build-123-py311-cpu" in tags
        assert "build-123-py311" in tags

    def test_cpu_ray_ml_no_empty_platform_suffix(self):
        """ray-ml with cpu does NOT get empty platform suffix."""
        ctx = make_ctx(python_version="3.11", platform="cpu", image_type="ray-ml")
        tags = ctx.destination_tags()

        assert tags == [
            "abc123-py311-cpu",
            "build-123-py311-cpu",
        ]

    def test_extra_image_type_includes_variation_suffix(self):
        """-extra image types get -extra variation suffix."""
        ray_extra_ctx = make_ctx(
            python_version="3.11", platform="cu12.3.2-cudnn9", image_type="ray-extra"
        )
        ray_ml_extra_ctx = make_ctx(
            python_version="3.11", platform="cu12.3.2-cudnn9", image_type="ray-ml-extra"
        )

        ray_extra_tags = ray_extra_ctx.destination_tags()
        ray_ml_extra_tags = ray_ml_extra_ctx.destination_tags()

        assert "abc123-extra-py311-cu123" in ray_extra_tags
        assert "abc123-extra-py311-cu123" in ray_ml_extra_tags

    def test_ray_extra_cpu_includes_empty_platform_suffix(self):
        """ray-extra with cpu gets empty platform suffix (like ray)."""
        ctx = make_ctx(python_version="3.11", platform="cpu", image_type="ray-extra")
        tags = ctx.destination_tags()

        # Should have both -cpu and empty platform suffix
        assert "abc123-extra-py311-cpu" in tags
        assert "abc123-extra-py311" in tags


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
