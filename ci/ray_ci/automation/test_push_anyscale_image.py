import sys
from unittest import mock

import pytest

from ci.ray_ci.automation.push_anyscale_image import (
    GPU_PLATFORM,
    _format_platform_tag,
    _format_python_version_tag,
    _get_image_tags,
    _get_wanda_image_name,
)


class TestFormatPythonVersionTag:
    @pytest.mark.parametrize(
        ("python_version", "expected"),
        [
            ("3.10", "-py310"),
            ("3.11", "-py311"),
            ("3.12", "-py312"),
            ("3.9", "-py39"),
        ],
    )
    def test_format_python_version_tag(self, python_version, expected):
        assert _format_python_version_tag(python_version) == expected


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
        assert _format_platform_tag(platform) == expected


class TestGetWandaImageName:
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
    def test_get_wanda_image_name(self, python_version, platform, image_type, expected):
        assert _get_wanda_image_name(python_version, platform, image_type) == expected


class TestGetImageTags:
    @mock.patch.dict(
        "os.environ",
        {
            "BUILDKITE_BRANCH": "master",
            "BUILDKITE_COMMIT": "abc123def456",
            "RAYCI_BUILD_ID": "build-123",
        },
    )
    def test_master_branch_tags(self):
        tags = _get_image_tags(python_version="3.10", platform="cpu")

        assert tags == [
            "abc123-py310-cpu",
            "build-123-py310-cpu",
        ]

    @mock.patch.dict(
        "os.environ",
        {
            "BUILDKITE_BRANCH": "master",
            "BUILDKITE_COMMIT": "abc123def456",
            "RAYCI_BUILD_ID": "build-123",
        },
    )
    def test_master_branch_gpu_platform_includes_alias(self):
        tags = _get_image_tags(python_version="3.10", platform=GPU_PLATFORM)

        assert tags == [
            "abc123-py310-cu121",
            "abc123-py310-gpu",
            "build-123-py310-cu121",
            "build-123-py310-gpu",
        ]

    @mock.patch.dict(
        "os.environ",
        {
            "BUILDKITE_BRANCH": "master",
            "BUILDKITE_COMMIT": "abc123def456",
            "RAYCI_BUILD_ID": "",
        },
    )
    def test_master_branch_no_build_id(self):
        tags = _get_image_tags(python_version="3.11", platform="cu12.3.2-cudnn9")

        assert tags == ["abc123-py311-cu123"]

    @mock.patch.dict(
        "os.environ",
        {
            "BUILDKITE_BRANCH": "releases/2.44.0",
            "BUILDKITE_COMMIT": "abc123def456",
            "RAYCI_BUILD_ID": "build-456",
        },
    )
    def test_release_branch_tags(self):
        tags = _get_image_tags(python_version="3.10", platform="cpu")

        assert tags == [
            "2.44.0.abc123-py310-cpu",
            "build-456-py310-cpu",
        ]

    @mock.patch.dict(
        "os.environ",
        {
            "BUILDKITE_BRANCH": "releases/2.44.0",
            "BUILDKITE_COMMIT": "abc123def456",
            "RAYCI_BUILD_ID": "build-456",
        },
    )
    def test_release_branch_gpu_platform_includes_alias(self):
        tags = _get_image_tags(python_version="3.10", platform=GPU_PLATFORM)

        assert tags == [
            "2.44.0.abc123-py310-cu121",
            "2.44.0.abc123-py310-gpu",
            "build-456-py310-cu121",
            "build-456-py310-gpu",
        ]

    @mock.patch.dict(
        "os.environ",
        {
            "BUILDKITE_BRANCH": "feature-branch",
            "BUILDKITE_COMMIT": "abc123def456",
            "BUILDKITE_PULL_REQUEST": "123",
            "RAYCI_BUILD_ID": "build-789",
        },
    )
    def test_pr_branch_tags(self):
        tags = _get_image_tags(python_version="3.12", platform="cpu")

        assert tags == [
            "pr-123.abc123-py312-cpu",
            "build-789-py312-cpu",
        ]

    @mock.patch.dict(
        "os.environ",
        {
            "BUILDKITE_BRANCH": "feature-branch",
            "BUILDKITE_COMMIT": "abc123def456",
            "BUILDKITE_PULL_REQUEST": "false",
            "RAYCI_BUILD_ID": "build-789",
        },
    )
    def test_non_pr_feature_branch_tags(self):
        tags = _get_image_tags(python_version="3.10", platform="cpu")

        assert tags == [
            "abc123-py310-cpu",
            "build-789-py310-cpu",
        ]

    @mock.patch.dict(
        "os.environ",
        {
            "BUILDKITE_BRANCH": "feature-branch",
            "BUILDKITE_COMMIT": "abc123def456",
            "BUILDKITE_PULL_REQUEST": "false",
            "RAYCI_BUILD_ID": "",
        },
    )
    def test_feature_branch_no_build_id(self):
        tags = _get_image_tags(python_version="3.10", platform="cpu")

        assert tags == ["abc123-py310-cpu"]


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
