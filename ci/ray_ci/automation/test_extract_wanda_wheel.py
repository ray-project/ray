import os
import subprocess
import sys
import tempfile
from pathlib import Path
from unittest import mock

import pytest
from click.testing import CliRunner

from ci.ray_ci.automation.extract_wanda_wheel import (
    WheelExtractionError,
    _extract_wheel,
    _get_arch_suffix,
    main,
)


class TestGetArchSuffix:
    """Tests for _get_arch_suffix function."""

    def test_x86_64_returns_empty_string(self):
        assert _get_arch_suffix("x86_64") == ""

    def test_aarch64_returns_suffix(self):
        assert _get_arch_suffix("aarch64") == "-aarch64"

    def test_unknown_arch_raises_error(self):
        with pytest.raises(WheelExtractionError, match="Unknown architecture"):
            _get_arch_suffix("arm64")

    def test_empty_arch_raises_error(self):
        with pytest.raises(WheelExtractionError, match="Unknown architecture"):
            _get_arch_suffix("")


class TestExtractWheel:
    """Tests for _extract_wheel function."""

    @mock.patch("ci.ray_ci.automation.extract_wanda_wheel.docker_pull")
    @mock.patch("ci.ray_ci.automation.extract_wanda_wheel.subprocess.run")
    def test_extract_wheel_success(self, mock_run, mock_docker_pull):
        """Test successful wheel extraction."""
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir) / "output"
            output_dir.mkdir()

            # Create a fake wheel file that docker cp will "extract"
            docker_temp = Path(temp_dir) / "docker_fs"
            docker_temp.mkdir()
            fake_wheel = docker_temp / "ray-3.0.0-cp310-cp310-linux_x86_64.whl"
            fake_wheel.write_text("fake wheel content")

            # Mock docker create (returns container ID)
            # Mock docker cp (copies files)
            # Mock docker rm (cleanup)
            def mock_subprocess(args, **kwargs):
                result = mock.MagicMock()
                if args[0:2] == ["docker", "create"]:
                    result.returncode = 0
                    result.stdout = "abc123container"
                    result.stderr = ""
                elif args[0:2] == ["docker", "cp"]:
                    # Simulate copying by actually copying the fake wheel
                    # to the destination path provided in the command.
                    import shutil

                    dest_path = Path(args[3])
                    for whl in docker_temp.glob("*.whl"):
                        shutil.copy2(whl, dest_path / whl.name)
                    result.returncode = 0
                    result.stdout = ""
                    result.stderr = ""
                elif args[0:2] == ["docker", "rm"]:
                    result.returncode = 0
                    result.stdout = ""
                    result.stderr = ""
                else:
                    result.returncode = 1
                    result.stderr = f"Unknown command: {args}"
                return result

            mock_run.side_effect = mock_subprocess

            _extract_wheel("test-repo:test-tag", "test-image", output_dir)

            # Verify docker_pull was called
            mock_docker_pull.assert_called_once_with("test-repo:test-tag")

            # Verify wheel was "extracted"
            extracted_wheels = list(output_dir.glob("*.whl"))
            assert len(extracted_wheels) == 1
            assert extracted_wheels[0].name == "ray-3.0.0-cp310-cp310-linux_x86_64.whl"

    @mock.patch("ci.ray_ci.automation.extract_wanda_wheel.docker_pull")
    def test_extract_wheel_pull_failure(self, mock_docker_pull):
        """Test that pull failure raises error."""
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)

            mock_docker_pull.side_effect = subprocess.CalledProcessError(
                1, "docker pull", stderr="Error: image not found"
            )

            with pytest.raises(WheelExtractionError, match="Failed to pull image"):
                _extract_wheel("nonexistent:tag", "test-image", output_dir)

    @mock.patch("ci.ray_ci.automation.extract_wanda_wheel.docker_pull")
    @mock.patch("ci.ray_ci.automation.extract_wanda_wheel.subprocess.run")
    def test_extract_wheel_create_failure(self, mock_run, mock_docker_pull):
        """Test that container create failure raises error."""
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)

            # docker create fails
            mock_run.return_value = mock.MagicMock(
                returncode=1,
                stdout="",
                stderr="Error: cannot create container",
            )

            with pytest.raises(
                WheelExtractionError, match="Failed to create container"
            ):
                _extract_wheel("test:tag", "test-image", output_dir)


class TestMainCLI:
    """Tests for the main CLI function."""

    @mock.patch("ci.ray_ci.automation.extract_wanda_wheel._extract_wheel")
    @mock.patch("ci.ray_ci.automation.extract_wanda_wheel.ecr_docker_login")
    def test_main_extracts_both_wheels(self, mock_ecr_login, mock_extract):
        """Test that main extracts both ray and ray-cpp wheels by default."""
        with tempfile.TemporaryDirectory() as temp_dir:
            runner = CliRunner()
            result = runner.invoke(
                main,
                [
                    "--python-version",
                    "3.10",
                    "--arch",
                    "x86_64",
                    "--rayci-build-id",
                    "test-build-123",
                    "--output-dir",
                    temp_dir,
                ],
            )

            assert result.exit_code == 0
            assert mock_extract.call_count == 2

            # Verify both wheel types were extracted
            call_args = [call[0] for call in mock_extract.call_args_list]
            image_tags = [args[0] for args in call_args]
            assert any("ray-wheel-py3.10" in tag for tag in image_tags)
            assert any("ray-cpp-wheel-py3.10" in tag for tag in image_tags)

    @mock.patch("ci.ray_ci.automation.extract_wanda_wheel._extract_wheel")
    @mock.patch("ci.ray_ci.automation.extract_wanda_wheel.ecr_docker_login")
    def test_main_skip_ray(self, mock_ecr_login, mock_extract):
        """Test --skip-ray flag."""
        with tempfile.TemporaryDirectory() as temp_dir:
            runner = CliRunner()
            result = runner.invoke(
                main,
                [
                    "--python-version",
                    "3.10",
                    "--arch",
                    "x86_64",
                    "--rayci-build-id",
                    "test-build-123",
                    "--output-dir",
                    temp_dir,
                    "--skip-ray",
                ],
            )

            assert result.exit_code == 0
            assert mock_extract.call_count == 1

            # Only ray-cpp should be extracted
            call_args = mock_extract.call_args_list[0][0]
            assert "ray-cpp-wheel" in call_args[0]

    @mock.patch("ci.ray_ci.automation.extract_wanda_wheel._extract_wheel")
    @mock.patch("ci.ray_ci.automation.extract_wanda_wheel.ecr_docker_login")
    def test_main_skip_ray_cpp(self, mock_ecr_login, mock_extract):
        """Test --skip-ray-cpp flag."""
        with tempfile.TemporaryDirectory() as temp_dir:
            runner = CliRunner()
            result = runner.invoke(
                main,
                [
                    "--python-version",
                    "3.10",
                    "--arch",
                    "x86_64",
                    "--rayci-build-id",
                    "test-build-123",
                    "--output-dir",
                    temp_dir,
                    "--skip-ray-cpp",
                ],
            )

            assert result.exit_code == 0
            assert mock_extract.call_count == 1

            # Only ray should be extracted
            call_args = mock_extract.call_args_list[0][0]
            assert "ray-wheel-py3.10" in call_args[0]
            assert "ray-cpp" not in call_args[0]

    @mock.patch("ci.ray_ci.automation.extract_wanda_wheel._extract_wheel")
    @mock.patch("ci.ray_ci.automation.extract_wanda_wheel.ecr_docker_login")
    def test_main_aarch64_arch_suffix(self, mock_ecr_login, mock_extract):
        """Test that aarch64 architecture adds correct suffix."""
        with tempfile.TemporaryDirectory() as temp_dir:
            runner = CliRunner()
            result = runner.invoke(
                main,
                [
                    "--python-version",
                    "3.11",
                    "--arch",
                    "aarch64",
                    "--rayci-build-id",
                    "test-build-123",
                    "--output-dir",
                    temp_dir,
                ],
            )

            assert result.exit_code == 0

            # Verify aarch64 suffix in image tags
            call_args = [call[0] for call in mock_extract.call_args_list]
            image_tags = [args[0] for args in call_args]
            assert any("ray-wheel-py3.11-aarch64" in tag for tag in image_tags)
            assert any("ray-cpp-wheel-py3.11-aarch64" in tag for tag in image_tags)

    def test_main_missing_build_id(self):
        """Test that missing RAYCI_BUILD_ID raises error."""
        runner = CliRunner()

        # Ensure env var is not set
        with mock.patch.dict(os.environ, {}, clear=True):
            result = runner.invoke(
                main,
                [
                    "--python-version",
                    "3.10",
                    "--arch",
                    "x86_64",
                ],
            )

            assert result.exit_code != 0
            assert isinstance(result.exception, WheelExtractionError)
            assert "RAYCI_BUILD_ID" in str(result.exception)

    def test_main_invalid_arch(self):
        """Test that invalid architecture raises error."""
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "--python-version",
                "3.10",
                "--arch",
                "invalid_arch",
                "--rayci-build-id",
                "test-build-123",
            ],
        )

        assert result.exit_code != 0
        assert isinstance(result.exception, WheelExtractionError)
        assert "Unknown architecture" in str(result.exception)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
