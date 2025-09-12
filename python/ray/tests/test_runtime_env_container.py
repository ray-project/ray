import sys

import pytest

import ray
from ray.tests.conftest import *  # noqa
from ray.tests.conftest_docker import *  # noqa
from ray.tests.conftest_docker import NESTED_IMAGE_NAME, run_in_container

# NOTE(zcin): The actual test code are in python scripts under
# python/ray/tests/runtime_env_container. The scripts are copied over to
# the docker container that's started by the `podman_docker_cluster`
# fixture, so that the tests can be run by invoking the test scripts
# using `python test.py` from within the pytests in this file


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on Linux.")
@pytest.mark.parametrize("use_image_uri_api", [True, False])
def test_put_get(podman_docker_cluster, use_image_uri_api):
    """Test ray.put and ray.get."""

    container_id = podman_docker_cluster
    cmd = ["python", "tests/test_put_get.py", "--image", NESTED_IMAGE_NAME]
    if use_image_uri_api:
        cmd.append("--use-image-uri-api")
    run_in_container([cmd], container_id)


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on Linux.")
@pytest.mark.parametrize("use_image_uri_api", [True, False])
def test_shared_memory(podman_docker_cluster, use_image_uri_api):
    """Test shared memory."""

    container_id = podman_docker_cluster
    cmd = ["python", "tests/test_shared_memory.py", "--image", NESTED_IMAGE_NAME]
    if use_image_uri_api:
        cmd.append("--use-image-uri-api")
    run_in_container([cmd], container_id)


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on Linux.")
@pytest.mark.parametrize("use_image_uri_api", [True, False])
def test_log_file_exists(podman_docker_cluster, use_image_uri_api):
    """Verify worker log file exists"""

    container_id = podman_docker_cluster
    cmd = ["python", "tests/test_log_file_exists.py", "--image", NESTED_IMAGE_NAME]
    if use_image_uri_api:
        cmd.append("--use-image-uri-api")
    run_in_container([cmd], container_id)


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on Linux.")
@pytest.mark.parametrize("use_image_uri_api", [True, False])
def test_ray_env_vars(podman_docker_cluster, use_image_uri_api):
    """Test that env vars with prefix 'RAY_' are propagated to container."""

    container_id = podman_docker_cluster
    cmd = ["python", "tests/test_ray_env_vars.py", "--image", NESTED_IMAGE_NAME]
    if use_image_uri_api:
        cmd.append("--use-image-uri-api")
    run_in_container([cmd], container_id)


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on Linux.")
def test_container_with_env_vars(podman_docker_cluster):
    """Test blah blah."""

    container_id = podman_docker_cluster
    cmd = ["python", "tests/test_with_env_vars.py", "--image", NESTED_IMAGE_NAME]
    run_in_container([cmd], container_id)


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on Linux.")
@pytest.mark.parametrize("use_image_uri_api", [True, False])
def test_worker_exit_intended_system_exit_and_user_error(
    podman_docker_cluster, use_image_uri_api
):
    """
    INTENDED_SYSTEM_EXIT
    - (not tested, hard to test) Unused resource removed
    - (tested) Pg removed
    - (tested) Idle
    USER_ERROR
    - (tested) Actor init failed
    """

    container_id = podman_docker_cluster
    cmd = [
        "python",
        "tests/test_worker_exit_intended_system_exit_and_user_error.py",
        "--image",
        NESTED_IMAGE_NAME,
    ]
    if use_image_uri_api:
        cmd.append("--use-image-uri-api")
    run_in_container([cmd], container_id)


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
def test_job(podman_docker_cluster):

    container_id = podman_docker_cluster
    cmd = ["python", "tests/test_job.py", "--image", NESTED_IMAGE_NAME]
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


EXPECTED_ERROR = "The '{0}' field currently cannot be used together with"


@pytest.mark.parametrize("api_version", ["container", "image_uri"])
class TestContainerRuntimeEnvWithOtherRuntimeEnv:
    def test_container_with_config(self, api_version):
        """`config` should be allowed with `container`"""

        runtime_env = {"config": {"setup_timeout_seconds": 10}}

        if api_version == "container":
            runtime_env["container"] = {"image": NESTED_IMAGE_NAME}
        else:
            runtime_env["image_uri"] = NESTED_IMAGE_NAME

        @ray.remote(runtime_env=runtime_env)
        def f():
            return ray.put((1, 10))

    def test_container_with_env_vars(self, api_version):
        """`env_vars` should be allowed with `container`"""

        runtime_env = {"env_vars": {"HELLO": "WORLD"}}

        if api_version == "container":
            runtime_env["container"] = {"image": NESTED_IMAGE_NAME}
        else:
            runtime_env["image_uri"] = NESTED_IMAGE_NAME

        @ray.remote(runtime_env=runtime_env)
        def f():
            return ray.put((1, 10))

    def test_container_with_pip(self, api_version):
        with pytest.raises(ValueError, match=EXPECTED_ERROR.format(api_version)):

            runtime_env = {"pip": ["requests"]}

            if api_version == "container":
                runtime_env["container"] = {"image": NESTED_IMAGE_NAME}
            else:
                runtime_env["image_uri"] = NESTED_IMAGE_NAME

            @ray.remote(runtime_env=runtime_env)
            def f():
                return ray.put((1, 10))

    def test_container_with_conda(self, api_version):
        with pytest.raises(ValueError, match=EXPECTED_ERROR.format(api_version)):

            runtime_env = {"conda": ["requests"]}

            if api_version == "container":
                runtime_env["container"] = {"image": NESTED_IMAGE_NAME}
            else:
                runtime_env["image_uri"] = NESTED_IMAGE_NAME

            @ray.remote(runtime_env=runtime_env)
            def f():
                return ray.put((1, 10))

    def test_container_with_py_modules(self, api_version):
        with pytest.raises(ValueError, match=EXPECTED_ERROR.format(api_version)):

            runtime_env = {"py_modules": ["requests"]}

            if api_version == "container":
                runtime_env["container"] = {"image": NESTED_IMAGE_NAME}
            else:
                runtime_env["image_uri"] = NESTED_IMAGE_NAME

            @ray.remote(runtime_env=runtime_env)
            def f():
                return ray.put((1, 10))

    def test_container_with_working_dir(self, api_version):
        with pytest.raises(ValueError, match=EXPECTED_ERROR.format(api_version)):

            runtime_env = {"working_dir": "."}

            if api_version == "container":
                runtime_env["container"] = {"image": NESTED_IMAGE_NAME}
            else:
                runtime_env["image_uri"] = NESTED_IMAGE_NAME

            @ray.remote(runtime_env=runtime_env)
            def f():
                return ray.put((1, 10))

    def test_container_with_pip_and_working_dir(self, api_version):
        with pytest.raises(ValueError, match=EXPECTED_ERROR.format(api_version)):

            runtime_env = {"pip": ["requests"], "working_dir": "."}

            if api_version == "container":
                runtime_env["container"] = {"image": NESTED_IMAGE_NAME}
            else:
                runtime_env["image_uri"] = NESTED_IMAGE_NAME

            @ray.remote(runtime_env=runtime_env)
            def f():
                return ray.put((1, 10))


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
