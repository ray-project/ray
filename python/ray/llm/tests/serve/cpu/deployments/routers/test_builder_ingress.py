import os
import re
import signal
import subprocess
import sys
import tempfile

import pytest
import yaml

from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.llm._internal.serve.configs.constants import (
    DEFAULT_LLM_ROUTER_TARGET_ONGOING_REQUESTS,
)
from ray.llm._internal.serve.configs.server_models import (
    LLMConfig,
    ModelLoadingConfig,
)
from ray.llm._internal.serve.deployments.routers.builder_ingress import (
    LLMServingArgs,
    build_openai_app,
)
from ray.serve.config import AutoscalingConfig


@pytest.fixture
def get_llm_serve_args(llm_config_with_mock_engine):
    yield LLMServingArgs(llm_configs=[llm_config_with_mock_engine])


@pytest.fixture()
def serve_config_separate_model_config_files():
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

            # Explicitly set accelerator_type to None to avoid GPU placement groups
            llm_config_yaml["accelerator_type"] = None

            # Explicitly set resources_per_bundle to use CPU instead of GPU
            llm_config_yaml["resources_per_bundle"] = {"CPU": 1}

            os.makedirs(os.path.dirname(llm_config_dst), exist_ok=True)
            with open(llm_config_dst, "w") as f:
                yaml.dump(llm_config_yaml, f)

        application["args"]["llm_configs"] = tmp_llm_config_files

    with open(serve_config_dst, "w") as f:
        yaml.dump(serve_config_yaml, f)

    yield serve_config_dst


class TestBuildOpenaiApp:
    def test_build_openai_app(
        self, get_llm_serve_args, shutdown_ray_and_serve, disable_placement_bundles
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
        disable_placement_bundles,
    ):
        """Test `build_openai_app` can be used in serve config."""

        def deployments_healthy():
            status_response = subprocess.check_output(["serve", "status"])
            print("[TEST] Status response: ", status_response)
            applications = extract_applications_from_output(status_response)

            if "llm-endpoint" not in applications:
                print("[TEST] Application 'llm-endpoint' not found.")
                return False

            llm_endpoint_status = applications["llm-endpoint"]
            if len(llm_endpoint_status["deployments"]) != 2:
                print(
                    f"[TEST] Expected 2 deployments, found {len(llm_endpoint_status['deployments'])}"
                )
                return False

            deployment_status = llm_endpoint_status["deployments"].values()
            if not all([status["status"] == "HEALTHY" for status in deployment_status]):
                print(f"[TEST] Not all deployments healthy: {deployment_status}")
                return False

            print("[TEST] All deployments healthy.")
            return True

        p = subprocess.Popen(["serve", "run", serve_config_separate_model_config_files])
        wait_for_condition(deployments_healthy, timeout=60, retry_interval_ms=1000)

        p.send_signal(signal.SIGINT)  # Equivalent to ctrl-C
        p.wait()

    def test_router_built_with_autoscaling_configs(self, disable_placement_bundles):
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
            == DEFAULT_LLM_ROUTER_TARGET_ONGOING_REQUESTS
        )


def extract_applications_from_output(output: bytes) -> dict:
    """
    Extracts the 'applications' block from mixed output and returns it as a dict.
    """
    # 1. Decode bytes to string
    text = output.decode("utf-8", errors="ignore")

    # 2. Regex to find the 'applications:' block and its indented content
    #    This matches 'applications:' and all following lines that are indented (YAML block)
    match = re.search(r"(^applications:\n(?:^(?: {2,}|\t).*\n?)+)", text, re.MULTILINE)
    if not match:
        raise ValueError("Could not find 'applications:' block in output.")

    applications_block = match.group(1)

    # 3. Parse the YAML block
    applications_dict = yaml.safe_load(applications_block)
    return applications_dict["applications"]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
