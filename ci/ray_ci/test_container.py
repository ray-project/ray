import sys

import pytest

from ci.ray_ci.container import _DOCKER_ECR_REPO, get_docker_image


def test_get_docker_image() -> None:
    assert get_docker_image("test-image") == f"{_DOCKER_ECR_REPO}:test-image"
    assert (
        get_docker_image("test-image", "a1b2c3")
        == f"{_DOCKER_ECR_REPO}:a1b2c3-test-image"
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
