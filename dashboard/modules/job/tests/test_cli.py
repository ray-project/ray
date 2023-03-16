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
            self._seq = seq
            self.iter = iter(self._seq)

        def __aiter__(self):
            return self

        async def __anext__(self):
            try:
                return next(self.iter)
            except StopIteration:
                self.iter = iter(self._seq)
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


def check_exit_code(result, exit_code):
    assert result.exit_code == exit_code, result.output


def _job_cli_group_test_address(mock_sdk_client, cmd, *args):
    runner = CliRunner()

    create_cluster_if_needed = True if cmd == "submit" else False
    # Test passing address via command line.
    result = runner.invoke(job_cli_group, [cmd, "--address=arg_addr", *args])
    mock_sdk_client.assert_called_with("arg_addr", create_cluster_if_needed)
    with pytest.raises(AssertionError):
        mock_sdk_client.assert_called_with("some_other_addr", True)
    check_exit_code(result, 0)
    # Test passing address via env var.
    with set_env_var("RAY_ADDRESS", "env_addr"):
        result = runner.invoke(job_cli_group, [cmd, *args])
        check_exit_code(result, 0)
        # RAY_ADDRESS is read inside the SDK client.
        mock_sdk_client.assert_called_with(None, create_cluster_if_needed)
    # Test passing no address.
    result = runner.invoke(job_cli_group, [cmd, *args])
    check_exit_code(result, 0)
    mock_sdk_client.assert_called_with(None, create_cluster_if_needed)


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
            check_exit_code(result, 0)

            result = runner.invoke(job_cli_group, ["submit", "--", "echo hello"])
            check_exit_code(result, 0)

            result = runner.invoke(
                job_cli_group,
                ["list"],
            )
            check_exit_code(result, 0)


class TestSubmit:
    def test_address(self, mock_sdk_client):
        _job_cli_group_test_address(mock_sdk_client, "submit", "--", "echo", "hello")

    def test_working_dir(self, mock_sdk_client):
        runner = CliRunner()
        mock_client_instance = mock_sdk_client.return_value

        with set_env_var("RAY_ADDRESS", "env_addr"):
            result = runner.invoke(job_cli_group, ["submit", "--", "echo hello"])
            check_exit_code(result, 0)
            mock_client_instance.submit_job.assert_called_with(
                entrypoint='"echo hello"',
                submission_id=None,
                runtime_env={},
                entrypoint_num_cpus=None,
                entrypoint_num_gpus=None,
                entrypoint_resources=None,
            )

            result = runner.invoke(
                job_cli_group,
                ["submit", "--working-dir", "blah", "--", "echo hello"],
            )
            check_exit_code(result, 0)
            mock_client_instance.submit_job.assert_called_with(
                entrypoint='"echo hello"',
                submission_id=None,
                runtime_env={"working_dir": "blah"},
                entrypoint_num_cpus=None,
                entrypoint_num_gpus=None,
                entrypoint_resources=None,
            )

            result = runner.invoke(
                job_cli_group, ["submit", "--working-dir='.'", "--", "echo hello"]
            )
            check_exit_code(result, 0)
            mock_client_instance.submit_job.assert_called_with(
                entrypoint='"echo hello"',
                submission_id=None,
                runtime_env={"working_dir": "'.'"},
                entrypoint_num_cpus=None,
                entrypoint_num_gpus=None,
                entrypoint_resources=None,
            )

    def test_runtime_env(self, mock_sdk_client, runtime_env_formats):
        runner = CliRunner()
        mock_client_instance = mock_sdk_client.return_value
        env_dict, env_json, env_yaml = runtime_env_formats

        with set_env_var("RAY_ADDRESS", "env_addr"):
            # Test passing via file.
            result = runner.invoke(
                job_cli_group, ["submit", "--runtime-env", env_yaml, "--", "echo hello"]
            )
            check_exit_code(result, 0)
            mock_client_instance.submit_job.assert_called_with(
                entrypoint='"echo hello"',
                submission_id=None,
                runtime_env=env_dict,
                entrypoint_num_cpus=None,
                entrypoint_num_gpus=None,
                entrypoint_resources=None,
            )

            # Test passing via json.
            result = runner.invoke(
                job_cli_group,
                ["submit", "--runtime-env-json", env_json, "--", "echo hello"],
            )
            check_exit_code(result, 0)
            mock_client_instance.submit_job.assert_called_with(
                entrypoint='"echo hello"',
                submission_id=None,
                runtime_env=env_dict,
                entrypoint_num_cpus=None,
                entrypoint_num_gpus=None,
                entrypoint_resources=None,
            )

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
            check_exit_code(result, 1)
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
            check_exit_code(result, 0)
            mock_client_instance.submit_job.assert_called_with(
                entrypoint='"echo hello"',
                submission_id=None,
                runtime_env=env_dict,
                entrypoint_num_cpus=None,
                entrypoint_num_gpus=None,
                entrypoint_resources=None,
            )

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
            check_exit_code(result, 0)
            mock_client_instance.submit_job.assert_called_with(
                entrypoint='"echo hello"',
                submission_id=None,
                runtime_env=env_dict,
                entrypoint_num_cpus=None,
                entrypoint_num_gpus=None,
                entrypoint_resources=None,
            )

    def test_job_id(self, mock_sdk_client):
        runner = CliRunner()
        mock_client_instance = mock_sdk_client.return_value

        with set_env_var("RAY_ADDRESS", "env_addr"):
            result = runner.invoke(job_cli_group, ["submit", "--", "echo hello"])
            check_exit_code(result, 0)
            mock_client_instance.submit_job.assert_called_with(
                entrypoint='"echo hello"',
                submission_id=None,
                runtime_env={},
                entrypoint_num_cpus=None,
                entrypoint_num_gpus=None,
                entrypoint_resources=None,
            )

            result = runner.invoke(
                job_cli_group,
                ["submit", "--submission-id=my_job_id", "--", "echo hello"],
            )
            check_exit_code(result, 0)
            mock_client_instance.submit_job.assert_called_with(
                entrypoint='"echo hello"',
                submission_id="my_job_id",
                runtime_env={},
                entrypoint_num_cpus=None,
                entrypoint_num_gpus=None,
                entrypoint_resources=None,
            )

    def test_entrypoint_num_cpus(self, mock_sdk_client):
        runner = CliRunner()
        mock_client_instance = mock_sdk_client.return_value

        with set_env_var("RAY_ADDRESS", "env_addr"):
            result = runner.invoke(
                job_cli_group,
                ["submit", "--entrypoint-num-cpus=2", "--", "echo hello"],
            )
            assert result.exit_code == 0
            mock_client_instance.submit_job.assert_called_with(
                entrypoint='"echo hello"',
                submission_id=None,
                runtime_env={},
                entrypoint_num_cpus=2,
                entrypoint_num_gpus=None,
                entrypoint_resources=None,
            )

    def test_entrypoint_num_gpus(self, mock_sdk_client):
        runner = CliRunner()
        mock_client_instance = mock_sdk_client.return_value

        with set_env_var("RAY_ADDRESS", "env_addr"):
            result = runner.invoke(
                job_cli_group,
                ["submit", "--entrypoint-num-gpus=2", "--", "echo hello"],
            )
            assert result.exit_code == 0
            mock_client_instance.submit_job.assert_called_with(
                entrypoint='"echo hello"',
                submission_id=None,
                runtime_env={},
                entrypoint_num_cpus=None,
                entrypoint_num_gpus=2,
                entrypoint_resources=None,
            )

    @pytest.mark.parametrize(
        "resources",
        [
            ("--entrypoint-num-cpus=2", {"entrypoint_num_cpus": 2}),
            ("--entrypoint-num-gpus=2", {"entrypoint_num_gpus": 2}),
            (
                """--entrypoint-resources={"Custom":3}""",
                {"entrypoint_resources": {"Custom": 3}},
            ),
        ],
    )
    def test_entrypoint_resources(self, mock_sdk_client, resources):
        runner = CliRunner()
        mock_client_instance = mock_sdk_client.return_value

        with set_env_var("RAY_ADDRESS", "env_addr"):
            result = runner.invoke(
                job_cli_group,
                ["submit", resources[0], "--", "echo hello"],
            )
            print(result.output)
            assert result.exit_code == 0
            expected_kwargs = {
                "entrypoint": '"echo hello"',
                "submission_id": None,
                "runtime_env": {},
                "entrypoint_num_cpus": None,
                "entrypoint_num_gpus": None,
                "entrypoint_resources": None,
            }
            expected_kwargs.update(resources[1])
            mock_client_instance.submit_job.assert_called_with(**expected_kwargs)

    def test_entrypoint_resources_invalid_json(self, mock_sdk_client):
        runner = CliRunner()

        with set_env_var("RAY_ADDRESS", "env_addr"):
            result = runner.invoke(
                job_cli_group,
                [
                    "submit",
                    """--entrypoint-resources={"Custom":3""",
                    "--",
                    "echo hello",
                ],
            )
            print(result.output)
            assert result.exit_code == 1
            assert "not a valid JSON string" in result.output


class TestDelete:
    def test_address(self, mock_sdk_client):
        _job_cli_group_test_address(mock_sdk_client, "delete", "fake_job_id")

    def test_delete(self, mock_sdk_client):
        runner = CliRunner()
        mock_client_instance = mock_sdk_client.return_value

        with set_env_var("RAY_ADDRESS", "env_addr"):
            result = runner.invoke(job_cli_group, ["delete", "job_id"])
            check_exit_code(result, 0)
            mock_client_instance.delete_job.assert_called_with("job_id")


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
