import sys
import pytest
from unittest.mock import patch

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
    assert "Error: Missing option '--post-build-script'" in result.output


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
