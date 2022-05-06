from contextlib import contextmanager
import json
import logging
from pathlib import Path
import tempfile
import os
from unittest import mock
import yaml
from typing import Optional

from click.testing import CliRunner

import pytest

from ray.dashboard.modules.job.cli import job_cli_group

logger = logging.getLogger(__name__)


@pytest.fixture
def mock_sdk_client():
    class AsyncIterator:
        def __init__(self, seq):
            self.iter = iter(seq)

        def __aiter__(self):
            return self

        async def __anext__(self):
            try:
                return next(self.iter)
            except StopIteration:
                raise StopAsyncIteration

    if "RAY_ADDRESS" in os.environ:
        del os.environ["RAY_ADDRESS"]
    with mock.patch("ray.dashboard.modules.job.cli.JobSubmissionClient") as mock_client:
        # In python 3.6 it will fail with error
        # 'async for' requires an object with __aiter__ method, got MagicMock"
        mock_client().tail_job_logs.return_value = AsyncIterator(range(10))

        yield mock_client


@pytest.fixture
def runtime_env_formats():
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir)

        test_env = {
            "working_dir": "s3://bogus.zip",
            "conda": "conda_env",
            "pip": ["pip-install-test"],
            "env_vars": {"hi": "hi2"},
        }

        yaml_file = path / "env.yaml"
        with yaml_file.open(mode="w") as f:
            yaml.dump(test_env, f)

        yield test_env, json.dumps(test_env), yaml_file


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


def _job_cli_group_test_address(mock_sdk_client, cmd, *args):
    runner = CliRunner()

    # Test passing address via command line.
    result = runner.invoke(job_cli_group, [cmd, "--address=arg_addr", *args])
    assert mock_sdk_client.called_with("arg_addr")
    assert result.exit_code == 0
    # Test passing address via env var.
    with set_env_var("RAY_ADDRESS", "env_addr"):
        result = runner.invoke(job_cli_group, [cmd, *args])
        assert result.exit_code == 0
        assert mock_sdk_client.called_with("env_addr")
    # Test passing no address.
    result = runner.invoke(job_cli_group, [cmd, *args])
    assert result.exit_code == 1
    assert "Address must be specified" in str(result.exception)


class TestList:
    def test_address(self, mock_sdk_client):
        _job_cli_group_test_address(mock_sdk_client, "list")

    def test_list(self, mock_sdk_client):
        runner = CliRunner()
        with set_env_var("RAY_ADDRESS", "env_addr"):
            result = runner.invoke(
                job_cli_group,
                ["list"],
            )
            assert result.exit_code == 0

            result = runner.invoke(job_cli_group, ["submit", "--", "echo hello"])
            assert result.exit_code == 0

            result = runner.invoke(
                job_cli_group,
                ["list"],
            )
            assert result.exit_code == 0


class TestSubmit:
    def test_address(self, mock_sdk_client):
        _job_cli_group_test_address(mock_sdk_client, "submit", "--", "echo", "hello")

    def test_working_dir(self, mock_sdk_client):
        runner = CliRunner()
        mock_client_instance = mock_sdk_client.return_value

        with set_env_var("RAY_ADDRESS", "env_addr"):
            result = runner.invoke(job_cli_group, ["submit", "--", "echo hello"])
            assert result.exit_code == 0
            assert mock_client_instance.called_with(runtime_env={})

            result = runner.invoke(
                job_cli_group,
                ["submit", "--", "--working-dir", "blah", "--", "echo hello"],
            )
            assert result.exit_code == 0
            assert mock_client_instance.called_with(runtime_env={"working_dir": "blah"})

            result = runner.invoke(
                job_cli_group, ["submit", "--", "--working-dir='.'", "--", "echo hello"]
            )
            assert result.exit_code == 0
            assert mock_client_instance.called_with(runtime_env={"working_dir": "."})

    def test_runtime_env(self, mock_sdk_client, runtime_env_formats):
        runner = CliRunner()
        mock_client_instance = mock_sdk_client.return_value
        env_dict, env_json, env_yaml = runtime_env_formats

        with set_env_var("RAY_ADDRESS", "env_addr"):
            # Test passing via file.
            result = runner.invoke(
                job_cli_group, ["submit", "--runtime-env", env_yaml, "--", "echo hello"]
            )
            assert result.exit_code == 0
            assert mock_client_instance.called_with(runtime_env=env_dict)

            # Test passing via json.
            result = runner.invoke(
                job_cli_group,
                ["submit", "--runtime-env-json", env_json, "--", "echo hello"],
            )
            assert result.exit_code == 0
            assert mock_client_instance.called_with(runtime_env=env_dict)

            # Test passing both throws an error.
            result = runner.invoke(
                job_cli_group,
                [
                    "submit",
                    "--runtime-env",
                    env_yaml,
                    "--runtime-env-json",
                    env_json,
                    "--",
                    "echo hello",
                ],
            )
            assert result.exit_code == 1
            assert "Only one of" in str(result.exception)

            # Test overriding working_dir.
            env_dict.update(working_dir=".")
            result = runner.invoke(
                job_cli_group,
                [
                    "submit",
                    "--runtime-env",
                    env_yaml,
                    "--working-dir",
                    ".",
                    "--",
                    "echo hello",
                ],
            )
            assert result.exit_code == 0
            assert mock_client_instance.called_with(runtime_env=env_dict)

            result = runner.invoke(
                job_cli_group,
                [
                    "submit",
                    "--runtime-env-json",
                    env_json,
                    "--working-dir",
                    ".",
                    "--",
                    "echo hello",
                ],
            )
            assert result.exit_code == 0
            assert mock_client_instance.called_with(runtime_env=env_dict)

    def test_job_id(self, mock_sdk_client):
        runner = CliRunner()
        mock_client_instance = mock_sdk_client.return_value

        with set_env_var("RAY_ADDRESS", "env_addr"):
            result = runner.invoke(job_cli_group, ["submit", "--", "echo hello"])
            assert result.exit_code == 0
            assert mock_client_instance.called_with(job_id=None)

            result = runner.invoke(
                job_cli_group, ["submit", "--", "--job-id=my_job_id", "echo hello"]
            )
            assert result.exit_code == 0
            assert mock_client_instance.called_with(job_id="my_job_id")


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
