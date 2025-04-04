import pytest
from ray import serve

from ray.llm._internal.serve.configs.server_models import (
    LLMServingArgs,
    LLMConfig,
    ModelLoadingConfig,
)
from ray.llm._internal.serve.builders.application_builders import (
    build_openai_app,
    build_llm_deployment,
)
from ray.llm._internal.serve.configs.constants import (
    RAYLLM_ROUTER_TARGET_ONGOING_REQUESTS,
)
from ray.serve.config import AutoscalingConfig
import subprocess
import yaml
import os
import tempfile
import signal
import sys

from ray._private.test_utils import wait_for_condition


@pytest.fixture
def get_llm_serve_args(llm_config):
    yield LLMServingArgs(llm_configs=[llm_config])


@pytest.fixture()
def serve_config_separate_model_config_files(model_pixtral_12b):
    with tempfile.TemporaryDirectory() as config_dir:
        serve_config_filename = "llm_app_separate_model_config_files.yaml"
        config_root = os.path.join(os.path.dirname(__file__), "test_config_files")
        serve_config_src = os.path.join(config_root, serve_config_filename)
        serve_config_dst = os.path.join(config_dir, serve_config_filename)

        with open(serve_config_src, "r") as f:
            serve_config_yaml = yaml.safe_load(f)

        for application in serve_config_yaml["applications"]:
            llm_configs = application["args"]["llm_configs"]
            tmp_llm_config_files = []
            for llm_config in llm_configs:
                llm_config_src = llm_config.replace(".", config_root, 1)
                llm_config_dst = llm_config.replace(".", config_dir, 1)
                tmp_llm_config_files.append(llm_config_dst)

                with open(llm_config_src, "r") as f:
                    llm_config_yaml = yaml.safe_load(f)
                llm_config_yaml["model_loading_config"]["model_id"] = model_pixtral_12b

                os.makedirs(os.path.dirname(llm_config_dst), exist_ok=True)
                with open(llm_config_dst, "w") as f:
                    yaml.dump(llm_config_yaml, f)

            application["args"]["llm_configs"] = tmp_llm_config_files

        with open(serve_config_dst, "w") as f:
            yaml.dump(serve_config_yaml, f)

        yield serve_config_dst


class TestBuildOpenaiApp:
    def test_build_openai_app(
        self, get_llm_serve_args, shutdown_ray_and_serve, use_mock_vllm_engine
    ):
        """Test `build_openai_app` can build app and run it with Serve."""

        app = build_openai_app(
            llm_serving_args=get_llm_serve_args,
        )
        assert isinstance(app, serve.Application)
        serve.run(app)

    def test_build_openai_app_with_config(
        self,
        serve_config_separate_model_config_files,
        shutdown_ray_and_serve,
        use_mock_vllm_engine,
    ):
        """Test `build_openai_app` can be used in serve config."""

        def deployments_healthy():
            status_response = subprocess.check_output(["serve", "status"])
            serve_status = yaml.safe_load(status_response)["applications"][
                "llm-endpoint"
            ]
            assert len(serve_status["deployments"]) == 2
            deployment_status = serve_status["deployments"].values()
            assert all([status["status"] == "HEALTHY" for status in deployment_status])
            return True

        p = subprocess.Popen(["serve", "run", serve_config_separate_model_config_files])
        wait_for_condition(deployments_healthy, timeout=30)

        p.send_signal(signal.SIGINT)  # Equivalent to ctrl-C
        p.wait()

    def test_router_built_with_autoscaling_configs(self):
        """Test that the router is built with the correct autoscaling configs that
        will scale.
        """
        llm_config_no_autoscaling_configured = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="model_id_1"),
            accelerator_type="L4",
        )
        llm_config_autoscaling_default = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="model_id_2"),
            accelerator_type="L4",
            deployment_config={"autoscaling_config": AutoscalingConfig()},
        )
        llm_config_autoscaling_non_default = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="model_id_3"),
            accelerator_type="L4",
            deployment_config={
                "autoscaling_config": AutoscalingConfig(
                    min_replicas=2,
                    initial_replicas=3,
                    max_replicas=4,
                )
            },
        )

        app = build_openai_app(
            LLMServingArgs(
                llm_configs=[
                    llm_config_no_autoscaling_configured,
                    llm_config_autoscaling_default,
                    llm_config_autoscaling_non_default,
                ]
            )
        )
        router_autoscaling_config = (
            app._bound_deployment._deployment_config.autoscaling_config
        )
        assert router_autoscaling_config.min_replicas == 8  # (1 + 1 + 2) * 2
        assert router_autoscaling_config.initial_replicas == 10  # (1 + 1 + 3) * 2
        assert router_autoscaling_config.max_replicas == 12  # (1 + 1 + 4) * 2
        assert (
            router_autoscaling_config.target_ongoing_requests
            == RAYLLM_ROUTER_TARGET_ONGOING_REQUESTS
        )


class TestBuildVllmDeployment:
    def test_build_llm_deployment(
        self,
        llm_config,
        shutdown_ray_and_serve,
        use_mock_vllm_engine,
    ):
        """Test `build_llm_deployment` can build a vLLM deployment."""

        app = build_llm_deployment(llm_config)
        assert isinstance(app, serve.Application)
        serve.run(app)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
