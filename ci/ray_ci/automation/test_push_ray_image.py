import sys
from unittest import mock

import pytest

from ci.ray_ci.automation.push_ray_image import RayImagePushContext, compact_cuda_suffix
from ci.ray_ci.configs import DEFAULT_ARCHITECTURE, DEFAULT_PYTHON_TAG_VERSION
from ci.ray_ci.docker_container import GPU_PLATFORM, RayType


def make_ctx(**overrides) -> RayImagePushContext:
    """Create a RayImagePushContext with defaults for testing."""
    defaults = {
        "ray_type": RayType.RAY,
        "python_version": DEFAULT_PYTHON_TAG_VERSION,
        "platform": "cpu",
        "architecture": DEFAULT_ARCHITECTURE,
        "branch": "master",
        "commit": "abc123",
        "rayci_schedule": "",
        "rayci_build_id": "build123",
        "pull_request": "false",
    }
    defaults.update(overrides)

    return RayImagePushContext(**defaults)


class TestWandaImageName:
    DEFAULT_TEST_CUDA_PLATFORM = "cu12.1.1-cudnn8"

    @pytest.mark.parametrize(
        ("ray_type", "python_version", "platform", "architecture", "expected"),
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
        ],
    )
    def test_wanda_image_name(
        self, ray_type, python_version, platform, architecture, expected
    ):
        ctx = make_ctx(
            ray_type=ray_type,
            python_version=python_version,
            platform=platform,
            architecture=architecture,
        )
        assert ctx.wanda_image_name() == expected


class TestVariationSuffix:
    @pytest.mark.parametrize(
        ("ray_type", "expected"),
        [
            (RayType.RAY, ""),
            (RayType.RAY_EXTRA, "-extra"),
            (RayType.RAY_ML, ""),
            (RayType.RAY_ML_EXTRA, "-extra"),
            (RayType.RAY_LLM, ""),
            (RayType.RAY_LLM_EXTRA, "-extra"),
        ],
    )
    def test_variation_suffix(self, ray_type, expected):
        ctx = make_ctx(ray_type=ray_type)
        assert ctx._variation_suffix() == expected


class TestPythonSuffixes:
    @pytest.mark.parametrize(
        ("python_version", "expected"),
        [
            (
                DEFAULT_PYTHON_TAG_VERSION,
                ["-py" + DEFAULT_PYTHON_TAG_VERSION.replace(".", ""), ""],
            ),  # default gets empty suffix too
            ("3.99", ["-py399"]),  # non-default gets no empty suffix
        ],
    )
    def test_python_suffixes(self, python_version, expected):
        ctx = make_ctx(python_version=python_version)
        assert ctx._python_suffixes() == expected


class TestPlatformSuffixes:
    @pytest.mark.parametrize(
        ("platform", "ray_type", "expected"),
        [
            # CPU images
            ("cpu", RayType.RAY, ["-cpu", ""]),
            ("cpu", RayType.RAY_EXTRA, ["-cpu", ""]),
            ("cpu", RayType.RAY_ML, ["-cpu"]),  # ray-ml doesn't get empty for cpu
            # CUDA images
            ("cu11.7.1-cudnn8", RayType.RAY, ["-cu117"]),
            ("cu11.8.0-cudnn8", RayType.RAY, ["-cu118"]),
            (GPU_PLATFORM, RayType.RAY, [compact_cuda_suffix(GPU_PLATFORM), "-gpu"]),
            (
                GPU_PLATFORM,
                RayType.RAY_ML,
                [compact_cuda_suffix(GPU_PLATFORM), "-gpu", ""],
            ),  # ray-ml gets empty for GPU_PLATFORM
        ],
    )
    def test_platform_suffixes(self, platform, ray_type, expected):
        ctx = make_ctx(platform=platform, ray_type=ray_type)
        assert ctx._platform_suffixes() == expected


class TestVersions:
    @mock.patch("ci.ray_ci.automation.push_ray_image.datetime")
    def test_nightly_master(self, mock_datetime):
        mock_datetime.now.return_value.strftime.return_value = "260107"
        ctx = make_ctx(branch="master", commit="abc123def456", rayci_schedule="nightly")
        assert ctx._versions() == ["nightly.260107.abc123", "nightly"]

    def test_release_branch(self):
        ctx = make_ctx(branch="releases/2.44.0", commit="abc123def456")
        assert ctx._versions() == ["2.44.0.abc123"]

    def test_pull_request(self):
        ctx = make_ctx(
            branch="feature-branch", commit="abc123def456", pull_request="12345"
        )
        assert ctx._versions() == ["pr-12345.abc123", "build123"]

    def test_other_branch(self):
        ctx = make_ctx(branch="feature-branch", commit="abc123def456")
        assert ctx._versions() == ["abc123", "build123"]

    def test_master_non_nightly(self):
        """Master branch without nightly schedule returns sha tags, not PR tags."""
        ctx = make_ctx(
            branch="master",
            commit="abc123def456",
            rayci_schedule="",
            pull_request="123",
        )
        # Even with pull_request set, master branch should return sha tags
        assert ctx._versions() == ["abc123", "build123"]


class TestDestinationTags:
    """
    Test destination_tags method.

    Tags are formed as: {version}{variation}{python_suffix}{platform}{architecture_suffix}
    """

    @mock.patch("ci.ray_ci.automation.push_ray_image.datetime")
    def test_nightly_cpu_default_python(self, mock_datetime):
        """Test: nightly.260107.abc123-py310-cpu"""
        mock_datetime.now.return_value.strftime.return_value = "260107"
        ctx = make_ctx(branch="master", commit="abc123def456", rayci_schedule="nightly")
        tags = ctx.destination_tags()
        # nightly versions x cpu suffixes x python suffixes
        # ["nightly.260107.abc123", "nightly"] x ["-cpu", ""] x ["-py310", ""]
        assert "nightly.260107.abc123-py310-cpu" in tags
        assert "nightly.260107.abc123-cpu" in tags
        assert "nightly.260107.abc123-py310" in tags
        assert "nightly.260107.abc123" in tags
        assert "nightly-py310-cpu" in tags
        assert "nightly-cpu" in tags
        assert "nightly-py310" in tags
        assert "nightly" in tags

    @mock.patch("ci.ray_ci.automation.push_ray_image.datetime")
    def test_nightly_extra_gpu(self, mock_datetime):
        """Test: nightly-extra-py310-cu121 and nightly.260107.abc123-extra-py310-gpu"""
        mock_datetime.now.return_value.strftime.return_value = "260107"
        ctx = make_ctx(
            ray_type=RayType.RAY_EXTRA,
            platform=GPU_PLATFORM,
            branch="master",
            commit="abc123def456",
            rayci_schedule="nightly",
        )
        tags = ctx.destination_tags()
        # Should include -extra variation and -gpu alias
        assert "nightly.260107.abc123-extra-py310-cu121" in tags
        assert "nightly.260107.abc123-extra-py310-gpu" in tags
        assert "nightly-extra-py310-cu121" in tags
        assert "nightly-extra-py310-gpu" in tags
        assert "nightly.260107.abc123-extra-cu121" in tags
        assert "nightly-extra-gpu" in tags

    @mock.patch("ci.ray_ci.automation.push_ray_image.datetime")
    def test_nightly_gpu_platform_non_default_python(self, mock_datetime):
        """Test: nightly.260107.abc123-py311-cu121"""
        mock_datetime.now.return_value.strftime.return_value = "260107"
        ctx = make_ctx(
            python_version="3.11",
            platform=GPU_PLATFORM,
            branch="master",
            commit="abc123def456",
            rayci_schedule="nightly",
        )
        tags = ctx.destination_tags()
        # Should include -cu121, -gpu aliases but NOT empty python suffix (3.11 is not default)
        assert "nightly.260107.abc123-py311-cu121" in tags
        assert "nightly.260107.abc123-py311-gpu" in tags
        assert "nightly-py311-cu121" in tags
        assert "nightly-py311-gpu" in tags
        # Should NOT have empty python suffix variants
        assert "nightly.260107.abc123-cu121" not in tags
        assert "nightly-gpu" not in tags

    def test_release_gpu(self):
        """Test: 2.53.0.abc123-py310-cu121"""
        ctx = make_ctx(
            platform=GPU_PLATFORM, branch="releases/2.53.0", commit="abc123def456"
        )
        tags = ctx.destination_tags()
        assert "2.53.0.abc123-py310-cu121" in tags
        assert "2.53.0.abc123-py310-gpu" in tags
        # Default python suffix variants
        assert "2.53.0.abc123-cu121" in tags
        assert "2.53.0.abc123-gpu" in tags

    def test_release_extra_gpu(self):
        """Test: 2.53.0.abc123-extra-py310-cu121"""
        ctx = make_ctx(
            ray_type=RayType.RAY_EXTRA,
            platform=GPU_PLATFORM,
            branch="releases/2.53.0",
            commit="abc123def456",
        )
        tags = ctx.destination_tags()
        assert "2.53.0.abc123-extra-py310-cu121" in tags
        assert "2.53.0.abc123-extra-py310-gpu" in tags
        # Default python suffix variants
        assert "2.53.0.abc123-extra-cu121" in tags
        assert "2.53.0.abc123-extra-gpu" in tags

    def test_release_non_gpu_platform_cuda(self):
        """Test release with non-GPU_PLATFORM CUDA version (no -gpu alias)."""
        ctx = make_ctx(
            python_version="3.11",
            platform="cu12.3.2-cudnn9",  # Not GPU_PLATFORM
            branch="releases/2.44.0",
            commit="abc123def456",
        )
        tags = ctx.destination_tags()
        assert "2.44.0.abc123-py311-cu123" in tags
        # Should NOT have -gpu alias since this isn't GPU_PLATFORM
        assert "2.44.0.abc123-py311-gpu" not in tags

    def test_release_cpu_aarch64(self):
        """Test release with architecture suffix."""
        ctx = make_ctx(
            architecture="aarch64",
            branch="releases/2.44.0",
            commit="abc123def456",
        )
        tags = ctx.destination_tags()
        assert "2.44.0.abc123-py310-cpu-aarch64" in tags
        assert "2.44.0.abc123-cpu-aarch64" in tags
        # Empty platform suffix variant (ray cpu alias)
        assert "2.44.0.abc123-py310-aarch64" in tags
        assert "2.44.0.abc123-aarch64" in tags

    def test_pull_request_tags(self):
        """Test PR builds include pr-{number} prefix."""
        ctx = make_ctx(
            branch="feature-branch", commit="abc123def456", pull_request="12345"
        )
        tags = ctx.destination_tags()
        assert "pr-12345.abc123-py310-cpu" in tags
        assert "build123-py310-cpu" in tags

    def test_feature_branch_non_pr(self):
        """Test non-PR feature branch uses sha and build_id."""
        ctx = make_ctx(python_version="3.12", commit="abc123def456")
        tags = ctx.destination_tags()
        assert "abc123-py312-cpu" in tags
        assert "build123-py312-cpu" in tags


class TestShouldUpload:
    """Test _should_upload function."""

    POSTMERGE_PIPELINE_ID = "test-postmerge-pipeline-id"
    NON_POSTMERGE_PIPELINE_ID = "some-other-pipeline-id"

    @mock.patch("ci.ray_ci.automation.push_ray_image.get_global_config")
    def test_non_postmerge_pipeline_returns_false(self, mock_config):
        """Non-postmerge pipelines should not upload."""
        from ci.ray_ci.automation.push_ray_image import _should_upload

        mock_config.return_value = {
            "ci_pipeline_postmerge": [self.POSTMERGE_PIPELINE_ID]
        }
        result = _should_upload(
            pipeline_id=self.NON_POSTMERGE_PIPELINE_ID,
            branch="master",
            rayci_schedule="nightly",
        )
        assert result is False

    @mock.patch("ci.ray_ci.automation.push_ray_image.get_global_config")
    def test_release_branch_returns_true(self, mock_config):
        """Release branches on postmerge should upload."""
        from ci.ray_ci.automation.push_ray_image import _should_upload

        mock_config.return_value = {
            "ci_pipeline_postmerge": [self.POSTMERGE_PIPELINE_ID]
        }
        result = _should_upload(
            pipeline_id=self.POSTMERGE_PIPELINE_ID,
            branch="releases/2.44.0",
            rayci_schedule="",
        )
        assert result is True

    @mock.patch("ci.ray_ci.automation.push_ray_image.get_global_config")
    def test_master_nightly_returns_true(self, mock_config):
        """Master branch with nightly schedule on postmerge should upload."""
        from ci.ray_ci.automation.push_ray_image import _should_upload

        mock_config.return_value = {
            "ci_pipeline_postmerge": [self.POSTMERGE_PIPELINE_ID]
        }
        result = _should_upload(
            pipeline_id=self.POSTMERGE_PIPELINE_ID,
            branch="master",
            rayci_schedule="nightly",
        )
        assert result is True

    @mock.patch("ci.ray_ci.automation.push_ray_image.get_global_config")
    def test_master_non_nightly_returns_false(self, mock_config):
        """Master branch without nightly schedule should not upload."""
        from ci.ray_ci.automation.push_ray_image import _should_upload

        mock_config.return_value = {
            "ci_pipeline_postmerge": [self.POSTMERGE_PIPELINE_ID]
        }
        result = _should_upload(
            pipeline_id=self.POSTMERGE_PIPELINE_ID,
            branch="master",
            rayci_schedule="",
        )
        assert result is False

    @mock.patch("ci.ray_ci.automation.push_ray_image.get_global_config")
    def test_feature_branch_returns_false(self, mock_config):
        """Feature branches should not upload even on postmerge."""
        from ci.ray_ci.automation.push_ray_image import _should_upload

        mock_config.return_value = {
            "ci_pipeline_postmerge": [self.POSTMERGE_PIPELINE_ID]
        }
        result = _should_upload(
            pipeline_id=self.POSTMERGE_PIPELINE_ID,
            branch="andrew/revup/master/feature",
            rayci_schedule="",
        )
        assert result is False

    @mock.patch("ci.ray_ci.automation.push_ray_image.get_global_config")
    def test_pr_branch_returns_false(self, mock_config):
        """PR branches should not upload even on postmerge."""
        from ci.ray_ci.automation.push_ray_image import _should_upload

        mock_config.return_value = {
            "ci_pipeline_postmerge": [self.POSTMERGE_PIPELINE_ID]
        }
        result = _should_upload(
            pipeline_id=self.POSTMERGE_PIPELINE_ID,
            branch="feature-branch",
            rayci_schedule="",
        )
        assert result is False

    @mock.patch("ci.ray_ci.automation.push_ray_image.get_global_config")
    def test_master_with_other_schedule_returns_false(self, mock_config):
        """Master branch with non-nightly schedule should not upload."""
        from ci.ray_ci.automation.push_ray_image import _should_upload

        mock_config.return_value = {
            "ci_pipeline_postmerge": [self.POSTMERGE_PIPELINE_ID]
        }
        result = _should_upload(
            pipeline_id=self.POSTMERGE_PIPELINE_ID,
            branch="master",
            rayci_schedule="weekly",
        )
        assert result is False


class TestCopyImage:
    """Test _copy_image function."""

    @mock.patch("ci.ray_ci.automation.push_ray_image.call_crane_copy")
    def test_copy_image_dry_run_skips_crane(self, mock_copy):
        """Test that dry run mode does not call crane copy."""
        from ci.ray_ci.automation.push_ray_image import _copy_image

        _copy_image("src", "dest", dry_run=True)
        mock_copy.assert_not_called()

    @mock.patch("ci.ray_ci.automation.push_ray_image.call_crane_copy")
    def test_copy_image_calls_crane(self, mock_copy):
        """Test that non-dry-run mode calls crane copy."""
        from ci.ray_ci.automation.push_ray_image import _copy_image

        _copy_image("src", "dest", dry_run=False)
        mock_copy.assert_called_once_with("src", "dest")

    @mock.patch("ci.ray_ci.automation.push_ray_image.call_crane_copy")
    def test_copy_image_raises_on_crane_error(self, mock_copy):
        """Test that crane errors are wrapped in PushRayImageError."""
        from ci.ray_ci.automation.crane_lib import CraneError
        from ci.ray_ci.automation.push_ray_image import PushRayImageError, _copy_image

        mock_copy.side_effect = CraneError("Copy failed")
        with pytest.raises(PushRayImageError, match="Crane copy failed"):
            _copy_image("src", "dest", dry_run=False)


class TestMultiplePlatforms:
    """Test main function handling of multiple platforms."""

    POSTMERGE_PIPELINE_ID = "test-postmerge-pipeline-id"
    WORK_REPO = "123456789.dkr.ecr.us-west-2.amazonaws.com/rayci-work"

    @mock.patch("ci.ray_ci.automation.push_ray_image.ci_init")
    @mock.patch("ci.ray_ci.automation.push_ray_image.ecr_docker_login")
    @mock.patch("ci.ray_ci.automation.push_ray_image._copy_image")
    @mock.patch("ci.ray_ci.automation.push_ray_image._image_exists")
    @mock.patch("ci.ray_ci.automation.push_ray_image.get_global_config")
    def test_multiple_platforms_processed(
        self, mock_config, mock_exists, mock_copy, mock_ecr_login, mock_ci_init
    ):
        """Test that multiple platforms are each processed with correct source refs."""
        from click.testing import CliRunner

        from ci.ray_ci.automation.push_ray_image import main

        mock_config.return_value = {
            "ci_pipeline_postmerge": [self.POSTMERGE_PIPELINE_ID]
        }
        mock_exists.return_value = True

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "--python-version",
                "3.10",
                "--platform",
                "cpu",
                "--platform",
                "cu12.1.1-cudnn8",
                "--image-type",
                "ray",
                "--architecture",
                "x86_64",
                "--rayci-work-repo",
                self.WORK_REPO,
                "--rayci-build-id",
                "build123",
                "--pipeline-id",
                self.POSTMERGE_PIPELINE_ID,
                "--branch",
                "releases/2.44.0",
                "--commit",
                "abc123def456",
            ],
        )

        assert result.exit_code == 0, f"CLI failed: {result.output}"
        # Should check image exists for both platforms
        assert mock_exists.call_count == 2
        exists_calls = [call[0][0] for call in mock_exists.call_args_list]
        assert any("ray-py3.10-cpu" in call for call in exists_calls)
        assert any("ray-py3.10-cu12.1.1-cudnn8" in call for call in exists_calls)
        # Should have tags from both platforms
        copy_calls = [call.args for call in mock_copy.call_args_list]
        assert any(
            "ray-py3.10-cpu" in src and "-cpu" in dest for src, dest in copy_calls
        )
        assert any(
            "ray-py3.10-cu12.1.1-cudnn8" in src and "-cu121" in dest
            for src, dest in copy_calls
        )

    @mock.patch("ci.ray_ci.automation.push_ray_image.ci_init")
    @mock.patch("ci.ray_ci.automation.push_ray_image.ecr_docker_login")
    @mock.patch("ci.ray_ci.automation.push_ray_image._copy_image")
    @mock.patch("ci.ray_ci.automation.push_ray_image._image_exists")
    @mock.patch("ci.ray_ci.automation.push_ray_image.get_global_config")
    def test_multiple_platforms_fails_if_one_missing(
        self, mock_config, mock_exists, mock_copy, mock_ecr_login, mock_ci_init
    ):
        """Test that processing fails if any platform's source image is missing."""
        from click.testing import CliRunner

        from ci.ray_ci.automation.push_ray_image import PushRayImageError, main

        mock_config.return_value = {
            "ci_pipeline_postmerge": [self.POSTMERGE_PIPELINE_ID]
        }
        mock_exists.side_effect = [True, False]  # First exists, second doesn't

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "--python-version",
                "3.10",
                "--platform",
                "cpu",
                "--platform",
                "cu12.1.1-cudnn8",
                "--image-type",
                "ray",
                "--architecture",
                "x86_64",
                "--rayci-work-repo",
                self.WORK_REPO,
                "--rayci-build-id",
                "build123",
                "--pipeline-id",
                self.POSTMERGE_PIPELINE_ID,
                "--branch",
                "releases/2.44.0",
                "--commit",
                "abc123def456",
            ],
        )

        assert result.exit_code != 0
        assert isinstance(result.exception, PushRayImageError)
        assert "Source image not found" in str(result.exception)


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
