import sys

import pytest

from ray.tests.conftest import *  # noqa
from ray.tests.conftest_docker import *  # noqa
from ray.tests.conftest_docker import NESTED_IMAGE_NAME, run_in_container

# NOTE(zcin): The actual test code are in python scripts under
# python/ray/serve/tests/runtime_env_image_uri. The scripts are copied over to
# the docker container that's started by the `podman_docker_cluster`
# fixture, so that the tests can be run by invoking the test scripts
# using `python test.py` from within the pytests in this file


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on Linux.")
@pytest.mark.parametrize("use_image_uri_api", [True, False])
def test_serve_basic(podman_docker_cluster, use_image_uri_api):
    """Test Serve deployment."""

    container_id = podman_docker_cluster
    cmd = ["python", "tests/test_serve_basic.py", "--image", NESTED_IMAGE_NAME]
    if use_image_uri_api:
        cmd.append("--use-image-uri-api")
    run_in_container([cmd], container_id)


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on Linux.")
@pytest.mark.parametrize("use_image_uri_api", [True, False])
def test_serve_telemetry(podman_docker_cluster, use_image_uri_api):
    """Test Serve deployment telemetry."""

    container_id = podman_docker_cluster
    cmd = ["python", "tests/test_serve_telemetry.py", "--image", NESTED_IMAGE_NAME]
    if use_image_uri_api:
        cmd.append("--use-image-uri-api")
    run_in_container([cmd], container_id)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
