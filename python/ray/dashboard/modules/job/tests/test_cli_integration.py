import json
import logging
import os
import subprocess
import sys
from contextlib import contextmanager
from typing import Optional, Tuple

import pytest

import ray

logger = logging.getLogger(__name__)


@pytest.fixture
def shutdown_only():
    yield None
    # The code after the yield will run as teardown code.
    ray.shutdown()
    # Delete the cluster address just in case.
    ray._common.utils.reset_ray_address()


@contextmanager
def set_env_var(key: str, val: Optional[str] = None):
    old_val = os.environ.get(key, None)
    if val is not None:
        os.environ[key] = val
    elif key in os.environ:
        del os.environ[key]

    try:
        yield
    finally:
        if key in os.environ:
            del os.environ[key]
        if old_val is not None:
            os.environ[key] = old_val


@pytest.fixture
def ray_start_stop():
    subprocess.check_output(["ray", "start", "--head"])
    try:
        with set_env_var("RAY_ADDRESS", "http://127.0.0.1:8265"):
            yield
    finally:
        subprocess.check_output(["ray", "stop", "--force"])


@contextmanager
def ray_cluster_manager():
    """
    Used not as fixture in case we want to set RAY_ADDRESS first.
    """
    subprocess.check_output(["ray", "start", "--head"])
    try:
        yield
    finally:
        subprocess.check_output(["ray", "stop", "--force"])


def _run_cmd(cmd: str, should_fail=False) -> Tuple[str, str]:
    """Convenience wrapper for subprocess.run.

    We always run with shell=True to simulate the CLI.

    Asserts that the process succeeds/fails depending on should_fail.

    Returns (stdout, stderr).
    """
    print(f"Running command: '{cmd}'")
    p: subprocess.CompletedProcess = subprocess.run(
        cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    if p.returncode == 0:
        print("Command succeeded.")
        if should_fail:
            raise RuntimeError(
                f"Expected command to fail, but got exit code: {p.returncode}."
            )
    else:
        print(f"Command failed with exit code: {p.returncode}.")
        if not should_fail:
            raise RuntimeError(
                f"Expected command to succeed, but got exit code: {p.returncode}."
            )

    return p.stdout.decode("utf-8"), p.stderr.decode("utf-8")


class TestJobSubmitHook:
    """Tests the RAY_JOB_SUBMIT_HOOK env var."""

    def test_hook(self, ray_start_stop):
        with set_env_var("RAY_JOB_SUBMIT_HOOK", "ray._private.test_utils.job_hook"):
            stdout, _ = _run_cmd("ray job submit -- echo hello")
            assert "hook intercepted: echo hello" in stdout


class TestRayJobHeaders:
    """
    Integration version of job CLI test that ensures interaction with the
    following components are working as expected:
    1) Ray client: use of RAY_JOB_HEADERS and ray.init() in job_head.py
    2) Ray dashboard: `ray start --head`
    """

    def test_empty_ray_job_headers(self, ray_start_stop):
        with set_env_var("RAY_JOB_HEADERS", None):
            stdout, _ = _run_cmd("ray job submit -- echo hello")
            assert "hello" in stdout
            assert "succeeded" in stdout

    @pytest.mark.parametrize("ray_job_headers", ['{"key": "value"}'])
    def test_ray_job_headers(self, ray_start_stop, ray_job_headers: str):
        with set_env_var("RAY_JOB_HEADERS", ray_job_headers):
            _run_cmd("ray job submit -- echo hello", should_fail=False)

    @pytest.mark.parametrize("ray_job_headers", ["{key value}"])
    def test_ray_incorrectly_formatted_job_headers(
        self, ray_start_stop, ray_job_headers: str
    ):
        with set_env_var("RAY_JOB_HEADERS", ray_job_headers):
            _run_cmd("ray job submit -- echo hello", should_fail=True)


class TestRayAddress:
    """
    Integration version of job CLI test that ensures interaction with the
    following components are working as expected:

    1) Ray client: use of RAY_ADDRESS and ray.init() in job_head.py
    2) Ray dashboard: `ray start --head`
    """

    def test_empty_ray_address(self, ray_start_stop):
        with set_env_var("RAY_ADDRESS", None):
            stdout, _ = _run_cmd("ray job submit -- echo hello")
            assert "hello" in stdout
            assert "succeeded" in stdout

    @pytest.mark.parametrize(
        "ray_api_server_address,should_fail",
        [
            ("http://127.0.0.1:8265", False),  # correct API server
            ("127.0.0.1:8265", True),  # wrong format without http
            ("http://127.0.0.1:9999", True),  # wrong port
        ],
    )
    def test_ray_api_server_address(
        self,
        ray_start_stop,
        ray_api_server_address: str,
        should_fail: bool,
    ):
        # Set a `RAY_ADDRESS` that would not work with the `ray job submit` CLI because it uses the `ray://` prefix.
        # This verifies that the `RAY_API_SERVER_ADDRESS` env var takes precedence.
        with set_env_var("RAY_ADDRESS", "ray://127.0.0.1:8265"):
            with set_env_var("RAY_API_SERVER_ADDRESS", ray_api_server_address):
                _run_cmd("ray job submit -- echo hello", should_fail=should_fail)

    @pytest.mark.parametrize(
        "ray_client_address,should_fail",
        [
            ("127.0.0.1:8265", True),
            ("ray://127.0.0.1:8265", True),
            ("http://127.0.0.1:8265", False),
        ],
    )
    def test_ray_client_address(
        self, ray_start_stop, ray_client_address: str, should_fail: bool
    ):
        with set_env_var("RAY_ADDRESS", ray_client_address):
            _run_cmd("ray job submit -- echo hello", should_fail=should_fail)

    def test_valid_http_ray_address(self, ray_start_stop):
        stdout, _ = _run_cmd("ray job submit -- echo hello")
        assert "hello" in stdout
        assert "succeeded" in stdout


class TestJobSubmit:
    def test_basic_submit(self, ray_start_stop):
        """Should tail logs and wait for process to exit."""
        cmd = "sleep 1 && echo hello && sleep 1 && echo hello"
        stdout, _ = _run_cmd(f"ray job submit -- bash -c '{cmd}'")

        # 'hello' should appear four times: twice when we print the entrypoint, then
        # two more times in the logs from the `echo`.
        assert stdout.count("hello") == 4
        assert "succeeded" in stdout

    def test_submit_no_wait(self, ray_start_stop):
        """Should exit immediately w/o printing logs."""
        cmd = "echo hello && sleep 1000"
        stdout, _ = _run_cmd(f"ray job submit --no-wait -- bash -c '{cmd}'")
        assert "hello" not in stdout
        assert "Tailing logs until the job exits" not in stdout

    def test_submit_with_logs_instant_job(self, ray_start_stop):
        """Should exit immediately and print logs even if job returns instantly."""
        cmd = "echo hello"
        stdout, _ = _run_cmd(f"ray job submit -- bash -c '{cmd}'")

        # 'hello' should appear twice: once when we print the entrypoint, then
        # again from the `echo`.
        assert stdout.count("hello") == 2

    def test_multiple_ray_init(self, ray_start_stop):
        cmd = (
            "python -c 'import ray; ray.init(); ray.shutdown(); "
            "ray.init(); ray.shutdown();'"
        )
        stdout, _ = _run_cmd(f"ray job submit -- {cmd}")
        assert "succeeded" in stdout

    def test_metadata(self, ray_start_stop):
        cmd = "echo hello"
        stdout, _ = _run_cmd(
            f'ray job submit --metadata-json=\'{{"key": "value"}}\' -- {cmd}'
        )
        assert "hello" in stdout
        assert "succeeded" in stdout

    def test_job_failed(self, ray_start_stop):
        cmd = "python -c 'import ray; ray.init(); assert 1 == 2;'"
        _run_cmd(f"ray job submit -- {cmd}", should_fail=True)


class TestRuntimeEnv:
    def test_bad_runtime_env(self, ray_start_stop):
        """Should fail with helpful error if runtime env setup fails."""
        stdout, _ = _run_cmd(
            'ray job submit --runtime-env-json=\'{"pip": '
            '["does-not-exist"]}\' -- echo hi',
            should_fail=True,
        )
        assert "Tailing logs until the job exits" in stdout
        assert "runtime_env setup failed" in stdout
        assert "No matching distribution found for does-not-exist" in stdout


class TestJobStop:
    def test_basic_stop(self, ray_start_stop):
        """Should wait until the job is stopped."""
        cmd = "sleep 1000"
        job_id = "test_basic_stop"
        _run_cmd(f"ray job submit --no-wait --job-id={job_id} -- {cmd}")

        stdout, _ = _run_cmd(f"ray job stop {job_id}")
        assert "Waiting for job" in stdout
        assert f"Job '{job_id}' was stopped" in stdout

    def test_stop_no_wait(self, ray_start_stop):
        """Should not wait until the job is stopped."""
        cmd = "echo hello && sleep 1000"
        job_id = "test_stop_no_wait"
        _run_cmd(f"ray job submit --no-wait --job-id={job_id} -- bash -c '{cmd}'")

        stdout, _ = _run_cmd(f"ray job stop --no-wait {job_id}")
        assert "Waiting for job" not in stdout
        assert f"Job '{job_id}' was stopped" not in stdout


class TestJobList:
    def test_empty(self, ray_start_stop):
        stdout, _ = _run_cmd("ray job list")
        assert "[]" in stdout

    def test_list(self, ray_start_stop):
        _run_cmd("ray job submit --job-id='hello_id' -- echo hello")

        runtime_env = {"env_vars": {"TEST": "123"}}
        _run_cmd(
            "ray job submit --job-id='hi_id' "
            f"--runtime-env-json='{json.dumps(runtime_env)}' -- echo hi"
        )
        stdout, _ = _run_cmd("ray job list")
        assert "123" in stdout
        assert "hello_id" in stdout
        assert "hi_id" in stdout


class TestJobDelete:
    def test_basic_delete(self, ray_start_stop):
        cmd = "sleep 1000"
        job_id = "test_basic_delete"
        _run_cmd(f"ray job submit --no-wait --submission-id={job_id} -- {cmd}")

        # Job shouldn't be able to be deleted because it is not in a terminal state.
        stdout, stderr = _run_cmd(f"ray job delete {job_id}", should_fail=True)
        assert "it is in a non-terminal state" in stderr

        # Submit a job that finishes quickly.
        cmd = "echo hello"
        job_id = "test_basic_delete_quick"
        _run_cmd(f"ray job submit --submission-id={job_id} -- bash -c '{cmd}'")

        # Job should be able to be deleted because it is finished.
        stdout, _ = _run_cmd(f"ray job delete {job_id}")
        assert f"Job '{job_id}' deleted successfully" in stdout


class TestJobStatus:
    # `ray job status` should exit with 0 if the job exists and non-zero if it doesn't.
    # This is the contract between Ray and KubRay v1.3.0.
    def test_status_job_exists(self, ray_start_stop):
        cmd = "echo hello"
        job_id = "test_job_id"
        _run_cmd(
            f"ray job submit --submission-id={job_id} -- bash -c '{cmd}'",
            should_fail=False,
        )
        _run_cmd(f"ray job status {job_id}", should_fail=False)

    def test_status_job_does_not_exist(self, ray_start_stop):
        job_id = "test_job_id"
        _run_cmd(f"ray job status {job_id}", should_fail=True)


def test_quote_escaping(ray_start_stop):
    cmd = "echo \"hello 'world'\""
    job_id = "test_quote_escaping"
    stdout, _ = _run_cmd(
        f"ray job submit --job-id={job_id} -- {cmd}",
    )
    assert "hello 'world'" in stdout


def test_resources(shutdown_only):
    ray.init(num_cpus=1, num_gpus=1, resources={"Custom": 1}, _memory=4)

    # Check the case of too many resources.
    for id, arg in [
        ("entrypoint_num_cpus", "--entrypoint-num-cpus=2"),
        ("entrypoint_num_gpus", "--entrypoint-num-gpus=2"),
        ("entrypoint_memory", "--entrypoint-memory=5"),
        ("entrypoint_resources", "--entrypoint-resources='{\"Custom\": 2}'"),
    ]:
        _run_cmd(f"ray job submit --submission-id={id} --no-wait {arg} -- echo hi")
        stdout, _ = _run_cmd(f"ray job status {id}")
        assert "waiting for resources" in stdout

    # Check the case of sufficient resources.
    stdout, _ = _run_cmd(
        "ray job submit --entrypoint-num-cpus=1 "
        "--entrypoint-num-gpus=1 --entrypoint-memory=4 --entrypoint-resources='{"
        '"Custom": 1}\' -- echo hello',
    )
    assert "hello" in stdout


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
