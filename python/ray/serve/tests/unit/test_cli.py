import json
import sys
from tempfile import NamedTemporaryFile
from typing import Dict

import pytest
import yaml
from click.testing import CliRunner

import ray
from ray.serve.scripts import publish
from ray.serve.tests.unit.publish_provider import get_ray_serve_publish_provider

TEST_PROVIDER_ARG = "--provider=ray.serve.tests.unit.publish_provider"


class TestPublish:
    def test_publish_basic(self):
        publish_provider = get_ray_serve_publish_provider()

        runner = CliRunner()
        result = runner.invoke(publish, [TEST_PROVIDER_ARG, "my_module:my_app"])
        assert result.exit_code == 0, result.output

        assert publish_provider.published_config["applications"] == [
            {
                "import_path": "my_module:my_app",
                "args": {},
                "runtime_env": {},
            }
        ]
        assert publish_provider.published_name is None
        assert publish_provider.published_ray_version == ray.__version__
        assert publish_provider.published_base_image is None

    def test_publish_with_name(self):
        publish_provider = get_ray_serve_publish_provider()

        runner = CliRunner()
        result = runner.invoke(
            publish, [TEST_PROVIDER_ARG, "my_module:my_app", "--name", "test-name"]
        )
        assert result.exit_code == 0, result.output

        assert publish_provider.published_config["applications"] == [
            {
                "import_path": "my_module:my_app",
                "args": {},
                "runtime_env": {},
            }
        ]
        assert publish_provider.published_name == "test-name"
        assert publish_provider.published_ray_version == ray.__version__
        assert publish_provider.published_base_image is None

    def test_publish_with_base_image(self):
        publish_provider = get_ray_serve_publish_provider()

        runner = CliRunner()
        result = runner.invoke(
            publish,
            [TEST_PROVIDER_ARG, "my_module:my_app", "--base-image", "test-image"],
        )
        assert result.exit_code == 0, result.output

        assert publish_provider.published_config["applications"] == [
            {
                "import_path": "my_module:my_app",
                "args": {},
                "runtime_env": {},
            }
        ]
        assert publish_provider.published_name is None
        assert publish_provider.published_ray_version == ray.__version__
        assert publish_provider.published_base_image == "test-image"

    def test_publish_with_args(self):
        publish_provider = get_ray_serve_publish_provider()

        runner = CliRunner()
        result = runner.invoke(
            publish, [TEST_PROVIDER_ARG, "my_module:my_app", "arg1=val1", "arg2=val2"]
        )
        assert result.exit_code == 0, result.output

        assert publish_provider.published_config["applications"] == [
            {
                "import_path": "my_module:my_app",
                "args": {"arg1": "val1", "arg2": "val2"},
                "runtime_env": {},
            }
        ]
        assert publish_provider.published_name is None
        assert publish_provider.published_ray_version == ray.__version__
        assert publish_provider.published_base_image is None

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
    def test_publish_with_runtime_env(
        self, runtime_env: Dict, use_json: bool, override_working_dir: bool
    ):
        publish_provider = get_ray_serve_publish_provider()
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
                publish, [TEST_PROVIDER_ARG, "my_module:my_app"] + runtime_env_args
            )

        assert result.exit_code == 0, result.output

        if override_working_dir:
            runtime_env["working_dir"] = "./override"

        assert publish_provider.published_config["applications"] == [
            {
                "import_path": "my_module:my_app",
                "args": {},
                "runtime_env": runtime_env,
            }
        ]
        assert publish_provider.published_name is None
        assert publish_provider.published_ray_version == ray.__version__
        assert publish_provider.published_base_image is None


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
