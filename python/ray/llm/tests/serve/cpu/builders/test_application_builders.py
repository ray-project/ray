import pytest
from ray import serve
from ray.serve.schema import ApplicationStatus, DeploymentStatus

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
def llm_config_with_mock_engine(llm_config):
    # Make sure engine is mocked.
    if llm_config.runtime_env is None:
        llm_config.runtime_env = {}
    llm_config.runtime_env.setdefault("env_vars", {})[
        "RAYLLM_VLLM_ENGINE_CLS"
    ] = "ray.llm.tests.serve.mocks.mock_vllm_engine.MockVLLMEngine"
    yield llm_config


@pytest.fixture
def get_llm_serve_args(llm_config_with_mock_engine):
    yield LLMServingArgs(llm_configs=[llm_config_with_mock_engine])


@pytest.fixture()
def serve_config_separate_model_config_files():
    # with tempfile.TemporaryDirectory() as config_dir:
    config_dir = tempfile.mkdtemp()
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

            # Make sure engine is mocked.
            if llm_config_yaml.get("runtime_env", None) is None:
                llm_config_yaml["runtime_env"] = {}
            llm_config_yaml["runtime_env"]["env_vars"] = {
                "RAYLLM_VLLM_ENGINE_CLS": "ray.llm.tests.serve.mocks.mock_vllm_engine.MockVLLMEngine"
            }

            os.makedirs(os.path.dirname(llm_config_dst), exist_ok=True)
            with open(llm_config_dst, "w") as f:
                yaml.dump(llm_config_yaml, f)

        application["args"]["llm_configs"] = tmp_llm_config_files

    with open(serve_config_dst, "w") as f:
        yaml.dump(serve_config_yaml, f)

    yield serve_config_dst


class TestBuildOpenaiApp:
    def test_build_openai_app(self, get_llm_serve_args, shutdown_ray_and_serve):
        """Test `build_openai_app` can build app and run it with Serve."""

        app = build_openai_app(
            llm_serving_args=get_llm_serve_args,
        )
        assert isinstance(app, serve.Application)
        serve.run(app)

    def test_build_openai_app_with_config(
        self, serve_config_separate_model_config_files, shutdown_ray_and_serve
    ):
        """Test `build_openai_app` can be used in serve config."""

        def deployments_healthy():
            status = serve.status()
            app_status = status.applications.get("llm-endpoint")
            if not app_status or app_status.status != ApplicationStatus.RUNNING:
                return False

            deployments = app_status.deployments
            if len(deployments) != 2:
                return False

            all_healthy = all(
                dep.status == DeploymentStatus.HEALTHY for dep in deployments.values()
            )
            if not all_healthy:
                unhealthy_deployments = {name: dep.status for name, dep in deployments.items() if dep.status != DeploymentStatus.HEALTHY}
                return False

            print("[TEST] All deployments healthy.")
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
        llm_config_with_mock_engine,
        shutdown_ray_and_serve,
    ):
        """Test `build_llm_deployment` can build a vLLM deployment."""

        app = build_llm_deployment(llm_config_with_mock_engine)
        assert isinstance(app, serve.Application)
        serve.run(app)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
