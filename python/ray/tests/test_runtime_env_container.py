import sys

import pytest

import ray
from ray.tests.conftest import *  # noqa
from ray.tests.conftest_docker import *  # noqa
from ray.tests.conftest_docker import run_in_container, NESTED_IMAGE_NAME


# NOTE(zcin): The actual test code are in python scripts under
# python/ray/tests/runtime_env_container. The scripts are copied over to
# the docker container that's started by the `podman_docker_cluster`
# fixture, so that the tests can be run by invoking the test scripts
# using `python test.py` from within the pytests in this file


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on Linux.")
def test_put_get(podman_docker_cluster):
    """Test ray.put and ray.get."""

    container_id = podman_docker_cluster
    cmd = ["python", "tests/test_put_get.py", "--image", NESTED_IMAGE_NAME]
    run_in_container([cmd], container_id)


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on Linux.")
def test_shared_memory(podman_docker_cluster):
    """Test shared memory."""

    container_id = podman_docker_cluster
    cmd = ["python", "tests/test_shared_memory.py", "--image", NESTED_IMAGE_NAME]
    run_in_container([cmd], container_id)


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on Linux.")
def test_log_file_exists(podman_docker_cluster):
    """Verify worker log file exists"""

    container_id = podman_docker_cluster
    cmd = ["python", "tests/test_log_file_exists.py", "--image", NESTED_IMAGE_NAME]
    run_in_container([cmd], container_id)


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on Linux.")
def test_ray_env_vars(podman_docker_cluster):
    """Test that env vars with prefix 'RAY_' are propagated to container."""

    container_id = podman_docker_cluster
    cmd = ["python", "tests/test_ray_env_vars.py", "--image", NESTED_IMAGE_NAME]
    run_in_container([cmd], container_id)


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on Linux.")
def test_container_with_env_vars(podman_docker_cluster):
    """Test blah blah."""

    container_id = podman_docker_cluster
    cmd = ["python", "tests/test_with_env_vars.py", "--image", NESTED_IMAGE_NAME]
    run_in_container([cmd], container_id)


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on Linux.")
def test_worker_exit_intended_system_exit_and_user_error(podman_docker_cluster):
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
    run_in_container([cmd], container_id)


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on Linux.")
def test_serve_basic(podman_docker_cluster):
    """Test Serve deployment."""

    container_id = podman_docker_cluster
    cmd = ["python", "tests/test_serve_basic.py", "--image", NESTED_IMAGE_NAME]
    run_in_container([cmd], container_id)


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on Linux.")
def test_serve_telemetry(podman_docker_cluster):
    """Test Serve deployment telemetry."""

    import subprocess

    container_id = podman_docker_cluster
    cmd = ["python", "tests/test_serve_telemetry.py", "--image", NESTED_IMAGE_NAME]
    try:
        run_in_container([cmd], container_id)
    except subprocess.CalledProcessError as e:
        print("process didn't complete successfully", e.output)
        raise
    ray.init()


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on Linux.")
def test_job(podman_docker_cluster):
    import subprocess

    container_id = podman_docker_cluster
    # cmd = ["ray", "start", "--head"]
    cmd = [
        "ray",
        "job",
        "submit",
        # "--address",
        # "http://127.0.0.1:8265",
        "--runtime-env-json",
        f'{{"container": {{"image": {NESTED_IMAGE_NAME}}}}}',
        "--",
        "python",
        "-V",
    ]
    try:
        run_in_container([cmd], container_id)
    except subprocess.CalledProcessError as e:
        print("process didn't complete successfully", e.output)
        raise


EXPECTED_ERROR = "The 'container' field currently cannot be used " "together with"


class TestContainerRuntimeEnvWithOtherRuntimeEnv:
    def test_container_with_pip(self):
        with pytest.raises(ValueError, match=EXPECTED_ERROR):

            @ray.remote(
                runtime_env={
                    "container": {
                        "image": NESTED_IMAGE_NAME,
                        "worker_path": "/some/path/to/default_worker.py",
                    },
                    "pip": ["requests"],
                }
            )
            def f():
                return ray.put((1, 10))

    def test_container_with_conda(self):
        with pytest.raises(ValueError, match=EXPECTED_ERROR):

            @ray.remote(
                runtime_env={
                    "container": {
                        "image": NESTED_IMAGE_NAME,
                        "worker_path": "/some/path/to/default_worker.py",
                    },
                    "conda": ["requests"],
                }
            )
            def f():
                return ray.put((1, 10))

    def test_container_with_py_modules(self):
        with pytest.raises(ValueError, match=EXPECTED_ERROR):

            @ray.remote(
                runtime_env={
                    "container": {
                        "image": NESTED_IMAGE_NAME,
                        "worker_path": "/some/path/to/default_worker.py",
                    },
                    "py_modules": ["requests"],
                }
            )
            def f():
                return ray.put((1, 10))

    def test_container_with_working_dir(self):
        with pytest.raises(ValueError, match=EXPECTED_ERROR):

            @ray.remote(
                runtime_env={
                    "container": {
                        "image": NESTED_IMAGE_NAME,
                        "worker_path": "/some/path/to/default_worker.py",
                    },
                    "working_dir": ".",
                }
            )
            def f():
                return ray.put((1, 10))

    def test_container_with_pip_and_working_dir(self):
        with pytest.raises(ValueError, match=EXPECTED_ERROR):

            @ray.remote(
                runtime_env={
                    "container": {
                        "image": NESTED_IMAGE_NAME,
                        "worker_path": "/some/path/to/default_worker.py",
                    },
                    "pip": ["requests"],
                    "working_dir": ".",
                }
            )
            def f():
                return ray.put((1, 10))


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
