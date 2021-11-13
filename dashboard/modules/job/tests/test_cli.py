from contextlib import contextmanager
import logging
import os
from unittest import mock

from click.testing import CliRunner

import pytest

from ray.dashboard.modules.job.cli import job_cli_group

logger = logging.getLogger(__name__)


@pytest.fixture
def mock_sdk_client():
    if "RAY_ADDRESS" in os.environ:
        del os.environ["RAY_ADDRESS"]
    with mock.patch("ray.dashboard.modules.job.cli.JobSubmissionClient"
                    ) as mock_client:
        yield mock_client


@contextmanager
def set_env_var(key: str, val: str):
    old_val = os.environ.get(key, None)
    os.environ[key] = val
    yield
    del os.environ[key]
    if old_val is not None:
        os.environ[key] = old_val


class TestSubmit:
    def test_address(self, mock_sdk_client):
        runner = CliRunner()

        # Test passing address via command line.
        result = runner.invoke(
            job_cli_group,
            ["submit", "--address=arg_addr", "--", "echo hello"])
        assert mock_sdk_client.called_with("arg_addr")
        assert result.exit_code == 0

        # Test passing address via env var.
        with set_env_var("RAY_ADDRESS", "env_addr"):
            result = runner.invoke(job_cli_group,
                                   ["submit", "--", "echo hello"])
            assert result.exit_code == 0
            assert mock_sdk_client.called_with("env_addr")

        # Test passing no address.
        result = runner.invoke(job_cli_group, ["submit", "--", "echo hello"])
        assert result.exit_code == 1
        assert "Address must be specified" in str(result.exception)

    def test_runtime_env(self, mock_sdk_client):
        mock_client_instance = mock_sdk_client.return_value

        runner = CliRunner()

        with set_env_var("RAY_ADDRESS", "env_addr"):
            result = runner.invoke(job_cli_group,
                                   ["submit", "--", "echo hello"])
            assert result.exit_code == 0
            assert mock_client_instance.called_with(runtime_env={})

            result = runner.invoke(
                job_cli_group,
                ["submit", "--", "--working-dir", "blah", "--", "echo hello"])
            assert result.exit_code == 0
            assert mock_client_instance.called_with(
                runtime_env={"working_dir": "blah"})

            result = runner.invoke(
                job_cli_group,
                ["submit", "--", "--working-dir='.'", "--", "echo hello"])
            assert result.exit_code == 0
            assert mock_client_instance.called_with(
                runtime_env={"working_dir": "."})

    def test_job_id(self, mock_sdk_client):
        mock_client_instance = mock_sdk_client.return_value

        runner = CliRunner()

        with set_env_var("RAY_ADDRESS", "env_addr"):
            result = runner.invoke(job_cli_group,
                                   ["submit", "--", "echo hello"])
            assert result.exit_code == 0
            assert mock_client_instance.called_with(job_id=None)

            result = runner.invoke(
                job_cli_group,
                ["submit", "--", "--job-id=my_job_id", "echo hello"])
            assert result.exit_code == 0
            assert mock_client_instance.called_with(job_id="my_job_id")


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
