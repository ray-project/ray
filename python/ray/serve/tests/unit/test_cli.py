import json
import sys
from tempfile import NamedTemporaryFile
from typing import Dict, Optional
from unittest.mock import Mock, patch

import click
import pytest
import yaml
from click.testing import CliRunner

from ray.serve.schema import (
    HTTPOptionsSchema,
    ServeApplicationSchema,
    ServeDeploySchema,
    ServeInstanceDetails,
)
from ray.serve.scripts import _validate_deployment_config, convert_args_to_dict, deploy


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


def test_no_existing_proxy_location_allows_any_new():
    config = ServeDeploySchema(
        proxy_location="EveryNode",
        http_options=HTTPOptionsSchema(host="0.0.0.0", port=8000),
    )
    serve_details = ServeInstanceDetails()
    _validate_deployment_config(serve_details, config)


# def test_same_proxy_location_ok():
#     config = MockDeploySchema(proxy_location="HeadOnly", http_options=MockHTTPOptions(host="0.0.0.0", port=8000))
#     details = MockServeDetails(proxy_location="HeadOnly", http_options=None)
#     _validate_deployment_config(config, details)
#
#
# def test_different_proxy_location_raises():
#     config = MockDeploySchema(proxy_location="EveryNode", http_options=MockHTTPOptions(host="0.0.0.0", port=8000))
#     details = MockServeDetails(proxy_location="HeadOnly", http_options=None)
#     with pytest.raises(RayServeException):
#         _validate_deployment_config(config, details)
#
#
# def test_no_existing_http_options_skip_check():
#     config = MockDeploySchema(
#         proxy_location="EveryNode",
#         http_options=MockHTTPOptions(host="1.2.3.4", port=9000),
#     )
#     details = MockServeDetails(proxy_location="EveryNode", http_options=None)
#     _validate_deployment_config(config, details)
#
#
# def test_same_http_options_ok():
#     config = MockDeploySchema(
#         proxy_location="EveryNode",
#         http_options=MockHTTPOptions(host="0.0.0.0", port=8000),
#     )
#     details = MockServeDetails(
#         proxy_location="EveryNode",
#         http_options=MockHTTPOptions(host="0.0.0.0", port=8000),
#     )
#     _validate_deployment_config(config, details)
#
#
# def test_changed_http_option_raises():
#     config = MockDeploySchema(
#         proxy_location="EveryNode",
#         http_options=MockHTTPOptions(host="127.0.0.1", port=8000),
#     )
#     details = MockServeDetails(
#         proxy_location="EveryNode",
#         http_options=MockHTTPOptions(host="0.0.0.0", port=8000),
#     )
#     with pytest.raises(RayServeException):
#         _validate_deployment_config(config, details)
#
#
# def test_multiple_changed_http_options_collect_all():
#     config = MockDeploySchema(
#         proxy_location="EveryNode",
#         http_options=MockHTTPOptions(host="127.0.0.1", port=9001),
#     )
#     details = MockServeDetails(
#         proxy_location="EveryNode",
#         http_options=MockHTTPOptions(host="0.0.0.0", port=8000),
#     )
#     with pytest.raises(RayServeException) as exc:
#         _validate_deployment_config(config, details)
#     msg = str(exc.value)
#     assert "host" in msg and "port" in msg


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
