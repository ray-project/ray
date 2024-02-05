import json
import sys
from tempfile import NamedTemporaryFile
from typing import Dict

import click
import pytest
import yaml
from click.testing import CliRunner

from ray.serve._private.deploy_provider import DeployOptions
from ray.serve.schema import ServeApplicationSchema, _skip_validating_runtime_env_uris
from ray.serve.scripts import convert_args_to_dict, deploy
from ray.serve.tests.unit.fake_deploy_provider import get_ray_serve_deploy_provider

TEST_PROVIDER_ARG = "--provider=ray.serve.tests.unit.fake_deploy_provider"


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


class TestDeploy:
    def test_deploy_basic(self):
        deploy_provider = get_ray_serve_deploy_provider()

        runner = CliRunner()
        result = runner.invoke(deploy, [TEST_PROVIDER_ARG, "my_module:my_app"])
        assert result.exit_code == 0, result.output

        assert deploy_provider.deployed_config.applications == [
            ServeApplicationSchema(
                import_path="my_module:my_app",
                args={},
                runtime_env={},
            )
        ]
        assert deploy_provider.deployed_options == DeployOptions(
            address="http://localhost:8265",
        )

    def test_deploy_with_address(self):
        deploy_provider = get_ray_serve_deploy_provider()

        runner = CliRunner()
        result = runner.invoke(
            deploy,
            [TEST_PROVIDER_ARG, "my_module:my_app", "--address", "http://magic.com"],
        )
        assert result.exit_code == 0, result.output

        assert deploy_provider.deployed_config.applications == [
            ServeApplicationSchema(
                import_path="my_module:my_app",
                args={},
                runtime_env={},
            )
        ]
        assert deploy_provider.deployed_options == DeployOptions(
            address="http://magic.com",
        )

    def test_deploy_with_name(self):
        deploy_provider = get_ray_serve_deploy_provider()

        runner = CliRunner()
        result = runner.invoke(
            deploy, [TEST_PROVIDER_ARG, "my_module:my_app", "--name", "test-name"]
        )
        assert result.exit_code == 0, result.output

        assert deploy_provider.deployed_config.applications == [
            ServeApplicationSchema(
                import_path="my_module:my_app",
                name="test-name",
                args={},
                runtime_env={},
            )
        ]
        assert deploy_provider.deployed_options == DeployOptions(
            address="http://localhost:8265",
            name="test-name",
        )

    def test_deploy_with_base_image(self):
        deploy_provider = get_ray_serve_deploy_provider()

        runner = CliRunner()
        result = runner.invoke(
            deploy,
            [TEST_PROVIDER_ARG, "my_module:my_app", "--base-image", "test-image"],
        )
        assert result.exit_code == 0, result.output

        assert deploy_provider.deployed_config.applications == [
            ServeApplicationSchema(
                import_path="my_module:my_app",
                args={},
                runtime_env={},
            )
        ]
        assert deploy_provider.deployed_options == DeployOptions(
            address="http://localhost:8265",
            base_image="test-image",
        )

    def test_deploy_with_args(self):
        deploy_provider = get_ray_serve_deploy_provider()

        runner = CliRunner()
        result = runner.invoke(
            deploy, [TEST_PROVIDER_ARG, "my_module:my_app", "arg1=val1", "arg2=val2"]
        )
        assert result.exit_code == 0, result.output

        assert deploy_provider.deployed_config.applications == [
            ServeApplicationSchema(
                import_path="my_module:my_app",
                args={"arg1": "val1", "arg2": "val2"},
                runtime_env={},
            )
        ]
        assert deploy_provider.deployed_options == DeployOptions(
            address="http://localhost:8265",
        )

    @pytest.mark.skipif(sys.platform == "win32", reason="Tempfile not working.")
    @pytest.mark.parametrize(
        "runtime_env",
        [
            {"env_vars": {"hi": "123"}},
            {"working_dir": ".", "py_modules": ["/some/path"]},
            {
                "working_dir": "s3://some_bucket/pkg.zip",
                "py_modules": ["s3://some_other_bucket/pkg.zip"],
            },
        ],
    )
    @pytest.mark.parametrize("use_json", [False, True])
    @pytest.mark.parametrize("override_working_dir", [False, True])
    def test_deploy_with_runtime_env(
        self, runtime_env: Dict, use_json: bool, override_working_dir: bool
    ):
        deploy_provider = get_ray_serve_deploy_provider()
        runner = CliRunner()
        with NamedTemporaryFile("w") as f:
            if use_json:
                runtime_env_args = ["--runtime-env-json", json.dumps(runtime_env)]
            else:
                yaml.dump(runtime_env, f, default_flow_style=False)
                runtime_env_args = ["--runtime-env", f.name]

            if override_working_dir:
                runtime_env_args.extend(["--working-dir", "./override"])

            result = runner.invoke(
                deploy, [TEST_PROVIDER_ARG, "my_module:my_app"] + runtime_env_args
            )

        assert result.exit_code == 0, result.output

        if override_working_dir:
            runtime_env["working_dir"] = "./override"

        with _skip_validating_runtime_env_uris():
            assert deploy_provider.deployed_config.applications == [
                ServeApplicationSchema(
                    import_path="my_module:my_app",
                    args={},
                    runtime_env=runtime_env,
                )
            ]
        assert deploy_provider.deployed_options == DeployOptions(
            address="http://localhost:8265",
        )

    @pytest.mark.parametrize("supported", [False, True])
    def test_deploy_provider_supports_runtime_env_local_uri(self, supported: bool):
        deploy_provider = get_ray_serve_deploy_provider()
        deploy_provider.set_supports_local_uris(supported)

        runner = CliRunner()
        result = runner.invoke(
            deploy, [TEST_PROVIDER_ARG, "my_module:my_app", "--working-dir", "."]
        )

        if supported:
            assert result.exit_code == 0, result.output
        else:
            assert result.exit_code == 1, result.output


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
