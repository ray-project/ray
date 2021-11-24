from contextlib import contextmanager
import logging
import sys
import subprocess

import pytest

from ray.dashboard.modules.job.tests.test_cli import set_env_var

logger = logging.getLogger(__name__)


@pytest.fixture
def ray_start_stop():
    subprocess.check_output(["ray", "start", "--head"])
    yield
    subprocess.check_output(["ray", "stop", "--force"])


@contextmanager
def ray_cluster_manager():
    """
    Used not as fixture in case we want to set RAY_ADDRESS first.
    """
    subprocess.check_output(["ray", "start", "--head"])
    yield
    subprocess.check_output(["ray", "stop", "--force"])


class TestSubmitIntegration:
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

    def test_ray_client_adress(self, ray_start_stop):
        with set_env_var("RAY_ADDRESS", "127.0.0.1:8265"):
            completed_process = subprocess.run(
                ["ray", "job", "submit", "--", "echo hello"],
                stderr=subprocess.PIPE)
            stderr = completed_process.stderr.decode("utf-8")
            # Current dashboard module that raises no exception from requests..
            assert "Query the status of the job" in stderr

    def test_valid_http_ray_address(self, ray_start_stop):
        with set_env_var("RAY_ADDRESS", "http://127.0.0.1:8265"):
            completed_process = subprocess.run(
                ["ray", "job", "submit", "--", "echo hello"],
                stderr=subprocess.PIPE)
            stderr = completed_process.stderr.decode("utf-8")
            # Current dashboard module that raises no exception from requests..
            assert "Query the status of the job" in stderr

    def test_set_ray_http_address_first(self):
        with set_env_var("RAY_ADDRESS", "http://127.0.0.1:8265"):
            with ray_cluster_manager():
                completed_process = subprocess.run(
                    ["ray", "job", "submit", "--", "echo hello"],
                    stderr=subprocess.PIPE)
                stderr = completed_process.stderr.decode("utf-8")
                # Current dashboard module that raises no exception from
                # requests..
                assert "Query the status of the job" in stderr

    def test_set_ray_client_address_first(self):
        with set_env_var("RAY_ADDRESS", "127.0.0.1:8265"):
            with ray_cluster_manager():
                completed_process = subprocess.run(
                    ["ray", "job", "submit", "--", "echo hello"],
                    stderr=subprocess.PIPE)
                stderr = completed_process.stderr.decode("utf-8")
                # Current dashboard module that raises no exception from
                # requests..
                assert "Query the status of the job" in stderr


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
