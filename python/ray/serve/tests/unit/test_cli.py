import json
import sys
from tempfile import NamedTemporaryFile
from typing import Dict, Optional
from unittest.mock import Mock, patch

import click
import pytest
import yaml
from click.testing import CliRunner

from ray.serve.config import ProxyLocation
from ray.serve.exceptions import RayServeException
from ray.serve.schema import (
    HTTPOptionsSchema,
    ServeActorDetails,
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

    def get_serve_details(self) -> Dict:
        return {
            "controller_info": ServeActorDetails().dict(),
            "proxies": {},
            "applications": {},
            "proxy_location": ProxyLocation.EveryNode,  # current default
            "http_options": HTTPOptionsSchema().dict(),
        }


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

    def test_deploy_fails_on_proxy_location_changed(self, fake_serve_client, tmp_path):
        runner = CliRunner()
        config_path = tmp_path / "config.yaml"
        config = {
            "proxy_location": "HeadOnly",
            "applications": [{"import_path": "my_module:my_app"}],
        }
        with open(config_path, "w") as f:
            yaml.dump(config, f)

        result = runner.invoke(deploy, [str(config_path)])
        assert result.exit_code != 0, result.output
        assert (
            "Attempt to update `proxy_location` from `EveryNode` to `HeadOnly`"
            in str(result.exception)
        )

    def test_deploy_fails_on_http_options_changed(self, fake_serve_client, tmp_path):
        runner = CliRunner()
        config_path = tmp_path / "config.yaml"
        config = {
            "http_options": {"host": "1.2.3.4"},
            "applications": [{"import_path": "my_module:my_app"}],
        }
        with open(config_path, "w") as f:
            yaml.dump(config, f)
        result = runner.invoke(deploy, [str(config_path)])
        assert result.exit_code != 0, result.output
        assert "Attempt to update `http_options` has been detected!" in str(
            result.exception
        )
        assert "'host': {'previous': '0.0.0.0', 'new': '1.2.3.4'}" in str(
            result.exception
        )

    def test_deploy_success_if_proxy_location_and_http_options_empty_in_config(
        self, fake_serve_client, tmp_path
    ):
        runner = CliRunner()
        config_path = tmp_path / "config.yaml"
        config = {
            "applications": [{"import_path": "my_module:my_app"}],
        }
        with open(config_path, "w") as f:
            yaml.dump(config, f)

        result = runner.invoke(deploy, [str(config_path)])
        assert result.exit_code == 0, result.output
        assert fake_serve_client.deployed_config["applications"] == [
            ServeApplicationSchema(
                import_path="my_module:my_app",
            ).dict(exclude_unset=True)
        ]


class TestValidateDeploymentConfig:
    def empty_new_schema(self) -> ServeDeploySchema:
        return ServeDeploySchema(
            applications=[ServeApplicationSchema(import_path="dummy.module:dummy_app")]
        )

    def new_schema_with_proxy(self, proxy_location: ProxyLocation) -> ServeDeploySchema:
        return ServeDeploySchema(
            proxy_location=proxy_location,
            applications=[ServeApplicationSchema(import_path="dummy.module:dummy_app")],
        )

    def new_schema_with_http(
        self, http_options: HTTPOptionsSchema
    ) -> ServeDeploySchema:
        return ServeDeploySchema(
            http_options=http_options,
            applications=[ServeApplicationSchema(import_path="dummy.module:dummy_app")],
        )

    def empty_existing_details(self) -> ServeInstanceDetails:
        return ServeInstanceDetails(
            controller_info=ServeActorDetails(), proxies={}, applications={}
        )

    def existing_details_with_proxy(
        self, proxy_location: ProxyLocation
    ) -> ServeInstanceDetails:
        return ServeInstanceDetails(
            proxy_location=proxy_location,
            controller_info=ServeActorDetails(),
            proxies={},
            applications={},
        )

    def existing_details_with_http(
        self, http_options: HTTPOptionsSchema
    ) -> ServeInstanceDetails:
        return ServeInstanceDetails(
            http_options=http_options,
            controller_info=ServeActorDetails(),
            proxies={},
            applications={},
        )

    def test_wrong_params(self):
        with pytest.raises(
            AssertionError,
            match="curr_serve_details must be `ServeInstanceDetails`, "
            "got `ServeDeploySchema`",
        ):
            _validate_deployment_config(
                self.empty_new_schema(),
                self.empty_existing_details(),
            )

        with pytest.raises(
            AssertionError,
            match="new_config must be `ServeDeploySchema`, "
            "got `ServeInstanceDetails`",
        ):
            _validate_deployment_config(
                self.empty_existing_details(),
                self.empty_existing_details(),
            )

        with pytest.raises(
            AssertionError,
            match="curr_serve_details must be `ServeInstanceDetails`, "
            "got `NoneType`",
        ):
            _validate_deployment_config(None, self.empty_new_schema())

        _validate_deployment_config(
            self.empty_existing_details(), self.empty_new_schema()
        )

    def test_no_existing_proxy_location_allows_any_new(self):
        # happens when the deployment is served for the first time
        assert self.empty_existing_details().proxy_location is None
        _validate_deployment_config(
            self.empty_existing_details(), self.empty_new_schema()
        )

        new_proxy_location = ProxyLocation.EveryNode
        _validate_deployment_config(
            self.empty_existing_details(),
            self.new_schema_with_proxy(new_proxy_location),
        )

        new_proxy_location = ProxyLocation.HeadOnly
        _validate_deployment_config(
            self.empty_existing_details(),
            self.new_schema_with_proxy(new_proxy_location),
        )

    def test_same_proxy_location_ok(self):
        curr_proxy_location = ProxyLocation.EveryNode
        new_proxy_location = ProxyLocation.EveryNode
        _validate_deployment_config(
            self.existing_details_with_proxy(curr_proxy_location),
            self.new_schema_with_proxy(new_proxy_location),
        )

        curr_proxy_location = ProxyLocation.Disabled
        new_proxy_location = ProxyLocation.Disabled
        _validate_deployment_config(
            self.existing_details_with_proxy(curr_proxy_location),
            self.new_schema_with_proxy(new_proxy_location),
        )

    def test_different_proxy_location_raises(self):
        curr_proxy_location = ProxyLocation.EveryNode
        new_proxy_location = ProxyLocation.HeadOnly
        with pytest.raises(RayServeException, match="from `EveryNode` to `HeadOnly`"):
            _validate_deployment_config(
                self.existing_details_with_proxy(curr_proxy_location),
                self.new_schema_with_proxy(new_proxy_location),
            )

        curr_proxy_location = ProxyLocation.Disabled
        new_proxy_location = ProxyLocation.EveryNode
        with pytest.raises(RayServeException, match="from `Disabled` to `EveryNode`"):
            _validate_deployment_config(
                self.existing_details_with_proxy(curr_proxy_location),
                self.new_schema_with_proxy(new_proxy_location),
            )

    def test_no_existing_http_options_skip_check(self):
        # happens when the deployment is served for the first time
        assert self.empty_existing_details().proxy_location is None
        # happens when user doesn't specify `proxy_location`
        assert (
            self.empty_new_schema().dict(exclude_unset=True).get("proxy_location")
            is None
        )
        _validate_deployment_config(
            self.empty_existing_details(), self.empty_new_schema()
        )

        new_http_options = HTTPOptionsSchema()
        _validate_deployment_config(
            self.empty_existing_details(), self.new_schema_with_http(new_http_options)
        )

    def test_same_http_options_ok(self):
        curr_http_options = HTTPOptionsSchema()
        new_http_options = HTTPOptionsSchema()
        _validate_deployment_config(
            self.existing_details_with_http(curr_http_options),
            self.new_schema_with_http(new_http_options),
        )

        curr_http_options = HTTPOptionsSchema(port=8001)
        new_http_options = HTTPOptionsSchema(port=8001)
        _validate_deployment_config(
            self.existing_details_with_http(curr_http_options),
            self.new_schema_with_http(new_http_options),
        )

    def test_changed_http_option_raises(self):
        curr_http_options = HTTPOptionsSchema(host="127.0.0.1", port=8000)
        new_http_options = HTTPOptionsSchema(host="0.0.0.0", port=8000)
        with pytest.raises(
            RayServeException,
            match="Attempt to update `http_options` has been detected!",
        ):
            _validate_deployment_config(
                self.existing_details_with_http(curr_http_options),
                self.new_schema_with_http(new_http_options),
            )

        curr_http_options = HTTPOptionsSchema(host="127.0.0.1", port=8000)
        new_http_options = HTTPOptionsSchema(host="127.0.0.1", port=8001)
        with pytest.raises(
            RayServeException,
            match="Attempt to update `http_options` has been detected!",
        ):
            _validate_deployment_config(
                self.existing_details_with_http(curr_http_options),
                self.new_schema_with_http(new_http_options),
            )

        curr_http_options = HTTPOptionsSchema(host="127.0.0.1", port=8000)
        new_http_options = HTTPOptionsSchema(host="0.0.0.0", port=8001)
        with pytest.raises(RayServeException) as ex:
            _validate_deployment_config(
                self.existing_details_with_http(curr_http_options),
                self.new_schema_with_http(new_http_options),
            )
        msg = str(ex.value)
        assert "Attempt to update `http_options` has been detected!" in msg
        assert "'host': {'previous': '127.0.0.1', 'new': '0.0.0.0'}" in msg
        assert "'port': {'previous': 8000, 'new': 8001}" in msg
        assert (
            "HTTP config is global to your Ray cluster, and you can't update it during runtime."
            in msg
        )
        assert "Please restart Ray Serve to apply the change." in msg


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
