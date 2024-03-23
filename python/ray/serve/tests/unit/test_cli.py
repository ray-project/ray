import json
import sys
from tempfile import NamedTemporaryFile
from typing import Dict, Optional
from unittest.mock import Mock, patch

import click
import pytest
import yaml
from click.testing import CliRunner

from ray.serve.schema import ServeApplicationSchema
from ray.serve.scripts import convert_args_to_dict, deploy


def test_convert_args_to_dict():
    assert convert_args_to_dict(tuple()) == {}

    with pytest.raises(
        click.ClickException, match="Invalid application argument 'bad_arg'"
    ):
        convert_args_to_dict(("bad_arg",))

    assert convert_args_to_dict(("key1=val1", "key2=val2")) == {
        "key1": "val1",
        "key2": "val2",
    }


class FakeServeSubmissionClient:
    def __init__(self):
        self._deployed_config: Optional[Dict] = None

    @property
    def deployed_config(self) -> Optional[Dict]:
        return self._deployed_config

    def deploy_applications(self, config: Dict):
        self._deployed_config = config


@pytest.fixture
def fake_serve_client() -> FakeServeSubmissionClient:
    fake_client = FakeServeSubmissionClient()
    with patch(
        "ray.serve.scripts.ServeSubmissionClient",
        new=Mock(return_value=fake_client),
    ):
        yield fake_client


class TestDeploy:
    def test_deploy_basic(self, fake_serve_client):
        runner = CliRunner()
        result = runner.invoke(deploy, ["my_module:my_app"])
        assert result.exit_code == 0, result.output

        assert fake_serve_client.deployed_config["applications"] == [
            ServeApplicationSchema(
                import_path="my_module:my_app",
                args={},
                runtime_env={},
            ).dict(exclude_unset=True)
        ]

    def test_deploy_with_name(self, fake_serve_client):
        runner = CliRunner()
        result = runner.invoke(deploy, ["my_module:my_app", "--name", "test-name"])
        assert result.exit_code == 0, result.output

        assert fake_serve_client.deployed_config["applications"] == [
            ServeApplicationSchema(
                import_path="my_module:my_app",
                name="test-name",
                args={},
                runtime_env={},
            ).dict(exclude_unset=True)
        ]

    def test_deploy_with_args(self, fake_serve_client):
        runner = CliRunner()
        result = runner.invoke(deploy, ["my_module:my_app", "arg1=val1", "arg2=val2"])
        assert result.exit_code == 0, result.output

        assert fake_serve_client.deployed_config["applications"] == [
            ServeApplicationSchema(
                import_path="my_module:my_app",
                args={"arg1": "val1", "arg2": "val2"},
                runtime_env={},
            ).dict(exclude_unset=True)
        ]

    @pytest.mark.skipif(sys.platform == "win32", reason="Tempfile not working.")
    @pytest.mark.parametrize(
        "runtime_env",
        [
            {"env_vars": {"hi": "123"}},
            {
                "working_dir": "s3://some_bucket/pkg.zip",
                "py_modules": ["s3://some_other_bucket/pkg.zip"],
            },
        ],
    )
    @pytest.mark.parametrize("use_json", [False, True])
    def test_deploy_with_runtime_env(
        self, fake_serve_client, runtime_env: Dict, use_json: bool
    ):
        runner = CliRunner()
        with NamedTemporaryFile("w") as f:
            if use_json:
                runtime_env_args = ["--runtime-env-json", json.dumps(runtime_env)]
            else:
                yaml.dump(runtime_env, f, default_flow_style=False)
                runtime_env_args = ["--runtime-env", f.name]

            result = runner.invoke(deploy, ["my_module:my_app"] + runtime_env_args)

        assert result.exit_code == 0, result.output

        assert fake_serve_client.deployed_config["applications"] == [
            ServeApplicationSchema(
                import_path="my_module:my_app",
                args={},
                runtime_env=runtime_env,
            ).dict(exclude_unset=True)
        ]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
