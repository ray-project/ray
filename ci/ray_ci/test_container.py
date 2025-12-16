import sys

import pytest

from ci.ray_ci.container import _DOCKER_ECR_REPO, get_docker_image


def test_get_docker_image() -> None:
    assert get_docker_image("test-image") == f"{_DOCKER_ECR_REPO}:test-image"
    assert (
        get_docker_image("test-image", "a1b2c3")
        == f"{_DOCKER_ECR_REPO}:a1b2c3-test-image"
    )
    # Test external repo (no build_id prefix when docker_repo != _DOCKER_ECR_REPO)
    assert (
        get_docker_image("1.0.0-jdk-x86_64", docker_repo="rayproject/manylinux2014")
        == "rayproject/manylinux2014:1.0.0-jdk-x86_64"
    )
    # Test that external repo ignores build_id
    assert (
        get_docker_image(
            "1.0.0-jdk-x86_64", "a1b2c3", docker_repo="rayproject/manylinux2014"
        )
        == "rayproject/manylinux2014:1.0.0-jdk-x86_64"
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
