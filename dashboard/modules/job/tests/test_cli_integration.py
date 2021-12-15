from contextlib import contextmanager
import os
import logging
import sys
import subprocess
from typing import Optional

import pytest

logger = logging.getLogger(__name__)


@contextmanager
def set_env_var(key: str, val: Optional[str] = None):
    old_val = os.environ.get(key, None)
    if val is not None:
        os.environ[key] = val
    elif key in os.environ:
        del os.environ[key]

    yield

    if key in os.environ:
        del os.environ[key]
    if old_val is not None:
        os.environ[key] = old_val


@pytest.fixture
def ray_start_stop():
    subprocess.check_output(["ray", "start", "--head"])
    try:
        with set_env_var("RAY_ADDRESS", "127.0.0.1:8265"):
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


class TestRayAddress:
    """
    Integration version of job CLI test that ensures interaction with the
    following components are working as expected:

    1) Ray client: use of RAY_ADDRESS and ray.init() in job_head.py
    2) Ray dashboard: `ray start --head`
    """

    def test_empty_ray_address(self, ray_start_stop):
        with set_env_var("RAY_ADDRESS", None):
            completed_process = subprocess.run(
                ["ray", "job", "submit", "--", "echo hello"],
                stderr=subprocess.PIPE)
            stderr = completed_process.stderr.decode("utf-8")
            # Current dashboard module that raises no exception from requests..
            assert ("Address must be specified using either the "
                    "--address flag or RAY_ADDRESS environment") in stderr

    def test_ray_client_address(self, ray_start_stop):
        completed_process = subprocess.run(
            ["ray", "job", "submit", "--", "echo hello"],
            stdout=subprocess.PIPE)
        stdout = completed_process.stdout.decode("utf-8")
        assert "hello" in stdout
        assert "succeeded" in stdout

    def test_valid_http_ray_address(self, ray_start_stop):
        completed_process = subprocess.run(
            ["ray", "job", "submit", "--", "echo hello"],
            stdout=subprocess.PIPE)
        stdout = completed_process.stdout.decode("utf-8")
        assert "hello" in stdout
        assert "succeeded" in stdout

    def test_set_ray_http_address_first(self):
        with set_env_var("RAY_ADDRESS", "http://127.0.0.1:8265"):
            with ray_cluster_manager():
                completed_process = subprocess.run(
                    ["ray", "job", "submit", "--", "echo hello"],
                    stdout=subprocess.PIPE)
                stdout = completed_process.stdout.decode("utf-8")
                assert "hello" in stdout
                assert "succeeded" in stdout

    def test_set_ray_client_address_first(self):
        with set_env_var("RAY_ADDRESS", "127.0.0.1:8265"):
            with ray_cluster_manager():
                completed_process = subprocess.run(
                    ["ray", "job", "submit", "--", "echo hello"],
                    stdout=subprocess.PIPE)
                stdout = completed_process.stdout.decode("utf-8")
                assert "hello" in stdout
                assert "succeeded" in stdout


class TestJobSubmit:
    def test_basic_submit(self, ray_start_stop):
        """Should tail logs and wait for process to exit."""
        cmd = "sleep 1 && echo hello && sleep 1 && echo hello"
        completed_process = subprocess.run(
            ["ray", "job", "submit", "--", cmd], stdout=subprocess.PIPE)
        stdout = completed_process.stdout.decode("utf-8")
        assert "hello\nhello" in stdout
        assert "succeeded" in stdout

    def test_submit_no_wait(self, ray_start_stop):
        """Should exit immediately w/o printing logs."""
        cmd = "echo hello && sleep 1000"
        completed_process = subprocess.run(
            ["ray", "job", "submit", "--no-wait", "--", cmd],
            stdout=subprocess.PIPE)
        stdout = completed_process.stdout.decode("utf-8")
        assert "hello" not in stdout
        assert "Tailing logs until the job exits" not in stdout


class TestJobStop:
    def test_basic_stop(self, ray_start_stop):
        """Should wait until the job is stopped."""
        cmd = "sleep 1000"
        job_id = "test_basic_stop"
        completed_process = subprocess.run([
            "ray", "job", "submit", "--no-wait", f"--job-id={job_id}", "--",
            cmd
        ])

        completed_process = subprocess.run(
            ["ray", "job", "stop", job_id], stdout=subprocess.PIPE)
        stdout = completed_process.stdout.decode("utf-8")
        assert "Waiting for job" in stdout
        assert f"Job '{job_id}' was stopped" in stdout

    def test_stop_no_wait(self, ray_start_stop):
        """Should not wait until the job is stopped."""
        cmd = "echo hello && sleep 1000"
        job_id = "test_stop_no_wait"
        completed_process = subprocess.run([
            "ray", "job", "submit", "--no-wait", f"--job-id={job_id}", "--",
            cmd
        ])

        completed_process = subprocess.run(
            ["ray", "job", "stop", "--no-wait", job_id],
            stdout=subprocess.PIPE)
        stdout = completed_process.stdout.decode("utf-8")
        assert "Waiting for job" not in stdout
        assert f"Job '{job_id}' was stopped" not in stdout


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
