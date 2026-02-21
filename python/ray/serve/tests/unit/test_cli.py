import json
import os
import pathlib
import sys
from tempfile import NamedTemporaryFile
from typing import Dict, Optional
from unittest.mock import Mock, patch

import click
import pytest
import yaml
from click.testing import CliRunner

from ray.serve.schema import ServeApplicationSchema
from ray.serve.scripts import build, convert_args_to_dict, deploy


def test_convert_args_to_dict():
    assert convert_args_to_dict(tuple()) == {}

    with pytest.raises(
        click.ClickException, match="Invalid application argument 'bad_arg'"
    ):
        convert_args_to_dict(("bad_arg",))

    with pytest.raises(
        click.ClickException, match="Invalid application argument 'bad_arg='"
    ):
        convert_args_to_dict(("bad_arg=",))

    assert convert_args_to_dict(("key1=val1", "key2=val2", "key3=nested=val")) == {
        "key1": "val1",
        "key2": "val2",
        "key3": "nested=val",
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


class TestEnumSerialization:
    """Test that enum representer correctly serializes enums in YAML dumps."""

    def test_build_command_with_enum_serialization(self):
        """Test that serve build correctly serializes AggregationFunction enum."""
        runner = CliRunner()
        with NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write(
                "from ray import serve\n"
                "from ray.serve.config import AggregationFunction, AutoscalingConfig\n\n"
                "@serve.deployment(\n"
                "    autoscaling_config=AutoscalingConfig(\n"
                "        min_replicas=1,\n"
                "        max_replicas=2,\n"
                "        aggregation_function=AggregationFunction.MEAN,\n"
                "    )\n"
                ")\n"
                "def test_deployment():\n"
                "    return 'ok'\n\n"
                "app = test_deployment.bind()\n"
            )
            temp_path = f.name

        output_path = None
        try:
            import_path = f"{pathlib.Path(temp_path).stem}:app"
            with NamedTemporaryFile(
                mode="w", suffix=".yaml", delete=False
            ) as output_file:
                output_path = output_file.name

            result = runner.invoke(
                build,
                [
                    import_path,
                    "--app-dir",
                    str(pathlib.Path(temp_path).parent),
                    "--output-path",
                    output_path,
                ],
            )
            assert result.exit_code == 0, result.output

            with open(output_path, "r") as f:
                config = yaml.safe_load(f)
            agg_func = config["applications"][0]["deployments"][0][
                "autoscaling_config"
            ]["aggregation_function"]
            assert agg_func == "mean"
            assert isinstance(agg_func, str)
        finally:
            os.unlink(temp_path)
            if output_path:
                os.unlink(output_path)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
