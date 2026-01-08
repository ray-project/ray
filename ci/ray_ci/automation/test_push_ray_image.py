import sys
from unittest import mock

import pytest

from ci.ray_ci.automation.push_ray_image import (
    GPU_PLATFORM,
    _format_architecture_tag,
    _format_platform_tag,
    _format_python_version_tag,
    _generate_image_tags,
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


class TestFormatArchitectureTag:
    @pytest.mark.parametrize(
        ("architecture", "expected"),
        [
            ("x86_64", ""),
            ("aarch64", "-aarch64"),
        ],
    )
    def test_format_architecture_tag(self, architecture, expected):
        assert _format_architecture_tag(architecture) == expected


class TestGetWandaImageName:
    @pytest.mark.parametrize(
        ("image_type", "python_version", "platform", "architecture", "expected"),
        [
            ("ray", "3.10", "cpu", "x86_64", "ray-py3.10-cpu"),
            ("ray", "3.11", "cu12.1.1-cudnn8", "x86_64", "ray-py3.11-cu12.1.1-cudnn8"),
            ("ray", "3.10", "cpu", "aarch64", "ray-py3.10-cpu-aarch64"),
            ("ray-extra", "3.10", "cpu", "x86_64", "ray-extra-py3.10-cpu"),
            (
                "ray-extra",
                "3.11",
                "cu12.8.1-cudnn",
                "x86_64",
                "ray-extra-py3.11-cu12.8.1-cudnn",
            ),
            (
                "ray-llm",
                "3.11",
                "cu12.8.1-cudnn",
                "x86_64",
                "ray-llm-py3.11-cu12.8.1-cudnn",
            ),
            (
                "ray-llm-extra",
                "3.11",
                "cu12.8.1-cudnn",
                "x86_64",
                "ray-llm-extra-py3.11-cu12.8.1-cudnn",
            ),
        ],
    )
    def test_get_wanda_image_name(
        self, image_type, python_version, platform, architecture, expected
    ):
        assert (
            _get_wanda_image_name(image_type, python_version, platform, architecture)
            == expected
        )


class TestGenerateImageTags:
    @mock.patch.dict(
        "os.environ",
        {
            "BUILDKITE_BRANCH": "master",
            "RAYCI_SCHEDULE": "nightly",
        },
    )
    @mock.patch("ci.ray_ci.automation.push_ray_image.datetime")
    def test_nightly_tags(self, mock_datetime):
        mock_datetime.now.return_value.strftime.return_value = "260107"

        tags = _generate_image_tags(
            commit="abc123def456",
            python_version="3.10",
            platform="cpu",
            architecture="x86_64",
        )

        assert tags == ["nightly.260107.abc123-py310-cpu"]

    @mock.patch.dict(
        "os.environ",
        {
            "BUILDKITE_BRANCH": "master",
            "RAYCI_SCHEDULE": "nightly",
        },
    )
    @mock.patch("ci.ray_ci.automation.push_ray_image.datetime")
    def test_nightly_tags_gpu_platform_includes_alias(self, mock_datetime):
        mock_datetime.now.return_value.strftime.return_value = "260107"

        tags = _generate_image_tags(
            commit="abc123def456",
            python_version="3.10",
            platform=GPU_PLATFORM,
            architecture="x86_64",
        )

        assert tags == [
            "nightly.260107.abc123-py310-cu121",
            "nightly.260107.abc123-py310-gpu",
        ]

    @mock.patch.dict(
        "os.environ",
        {
            "BUILDKITE_BRANCH": "releases/2.44.0",
            "RAYCI_SCHEDULE": "",
        },
    )
    def test_release_tags(self):
        tags = _generate_image_tags(
            commit="abc123def456",
            python_version="3.11",
            platform="cu12.3.2-cudnn9",
            architecture="x86_64",
        )

        assert tags == ["2.44.0.abc123-py311-cu123"]

    @mock.patch.dict(
        "os.environ",
        {
            "BUILDKITE_BRANCH": "releases/2.44.0",
            "RAYCI_SCHEDULE": "",
        },
    )
    def test_release_tags_aarch64(self):
        tags = _generate_image_tags(
            commit="abc123def456",
            python_version="3.10",
            platform="cpu",
            architecture="aarch64",
        )

        assert tags == ["2.44.0.abc123-py310-cpu-aarch64"]

    @mock.patch.dict(
        "os.environ",
        {
            "BUILDKITE_BRANCH": "feature-branch",
            "RAYCI_SCHEDULE": "",
        },
    )
    def test_other_branch_tags(self):
        tags = _generate_image_tags(
            commit="abc123def456",
            python_version="3.12",
            platform="cpu",
            architecture="x86_64",
        )

        assert tags == ["abc123-py312-cpu"]

    @mock.patch.dict(
        "os.environ",
        {
            "BUILDKITE_BRANCH": "master",
            "RAYCI_SCHEDULE": "",  # Not nightly
        },
    )
    def test_master_non_nightly_tags(self):
        tags = _generate_image_tags(
            commit="abc123def456",
            python_version="3.10",
            platform="cpu",
            architecture="x86_64",
        )

        assert tags == ["abc123-py310-cpu"]


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
