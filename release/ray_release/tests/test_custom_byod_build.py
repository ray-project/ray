import sys
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from ray_release.scripts.custom_byod_build import main


@patch("ray_release.scripts.custom_byod_build.build_anyscale_custom_byod_image")
def test_custom_byod_build(mock_build_anyscale_custom_byod_image):
    mock_build_anyscale_custom_byod_image.return_value = None
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "--image-name",
            "test-image",
            "--base-image",
            "test-base-image",
            "--post-build-script",
            "test_post_build_script.sh",
            "--python-depset",
            "python_depset.lock",
        ],
    )
    assert result.exit_code == 0


@patch("ray_release.scripts.custom_byod_build.build_anyscale_custom_byod_image")
def test_custom_byod_build_without_lock_file(
    mock_build_anyscale_custom_byod_image,
):
    mock_build_anyscale_custom_byod_image.return_value = None
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "--image-name",
            "test-image",
            "--base-image",
            "test-base-image",
            "--post-build-script",
            "test_post_build_script.sh",
        ],
    )
    assert result.exit_code == 0


@patch("ray_release.scripts.custom_byod_build.build_anyscale_custom_byod_image")
def test_custom_byod_build_missing_arg(mock_build_anyscale_custom_byod_image):
    mock_build_anyscale_custom_byod_image.return_value = None
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "--base-image",
            "test-base-image",
            "--post-build-script",
            "test_post_build_script.sh",
        ],
    )
    assert result.exit_code == 2
    assert "Error: Missing option '--image-name'" in result.output

    result = runner.invoke(
        main,
        [
            "--image-name",
            "test-image",
            "--post-build-script",
            "test_post_build_script.sh",
        ],
    )
    assert result.exit_code == 2
    assert "Error: Missing option '--base-image'" in result.output

    result = runner.invoke(
        main, ["--image-name", "test-image", "--base-image", "test-base-image"]
    )
    assert result.exit_code == 2
    assert (
        "At least one of post_build_script, python_depset, or env must be provided"
        in result.output
    )

    result = runner.invoke(
        main,
        [
            "--image-name",
            "test-image",
            "--base-image",
            "test-base-image",
            "--python-depset",
            "python_depset.lock",
        ],
    )
    assert result.exit_code == 0


@patch("ray_release.scripts.custom_byod_build.build_anyscale_custom_byod_image")
def test_custom_byod_build_with_env(mock_build_anyscale_custom_byod_image):
    mock_build_anyscale_custom_byod_image.return_value = None
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "--image-name",
            "test-image",
            "--base-image",
            "test-base-image",
            "--env",
            "FOO=bar",
            "--env",
            "BAZ=qux",
        ],
    )
    assert result.exit_code == 0
    build_context = mock_build_anyscale_custom_byod_image.call_args[0][2]
    assert build_context["envs"] == {"FOO": "bar", "BAZ": "qux"}
    assert "post_build_script" not in build_context
    assert "python_depset" not in build_context


@patch("ray_release.scripts.custom_byod_build.build_anyscale_custom_byod_image")
def test_custom_byod_build_with_env_and_script(mock_build_anyscale_custom_byod_image):
    mock_build_anyscale_custom_byod_image.return_value = None
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "--image-name",
            "test-image",
            "--base-image",
            "test-base-image",
            "--post-build-script",
            "test_post_build_script.sh",
            "--env",
            "KEY=value",
        ],
    )
    assert result.exit_code == 0
    build_context = mock_build_anyscale_custom_byod_image.call_args[0][2]
    assert build_context["envs"] == {"KEY": "value"}
    assert build_context["post_build_script"] == "test_post_build_script.sh"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
