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
from ray.llm._internal.serve.constants import DEFAULT_MAX_TARGET_ONGOING_REQUESTS
from ray.llm._internal.serve.core.configs.llm_config import (
    LLMConfig,
    ModelLoadingConfig,
)
from ray.llm._internal.serve.core.ingress.builder import (
    IngressClsConfig,
    LLMServingArgs,
    build_openai_app,
)
from ray.llm._internal.serve.core.ingress.ingress import OpenAiIngress
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

            # Use placement_group_config to specify CPU-only bundles
            llm_config_yaml["placement_group_config"] = {
                "bundles": [{"CPU": 1, "GPU": 0}]
            }

            os.makedirs(os.path.dirname(llm_config_dst), exist_ok=True)
            with open(llm_config_dst, "w") as f:
                yaml.dump(llm_config_yaml, f)

        application["args"]["llm_configs"] = tmp_llm_config_files

    with open(serve_config_dst, "w") as f:
        yaml.dump(serve_config_yaml, f)

    yield serve_config_dst


class TestLLMServingArgs:
    """Test suite for LLMServingArgs data model."""

    @pytest.fixture
    def llm_config(self):
        """Basic LLMConfig for testing."""
        return LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id="test-model", model_source="test-source"
            )
        )

    def test_basic_creation_and_defaults(self, llm_config):
        """Test creation with minimal config and verify defaults."""
        args = LLMServingArgs(llm_configs=[llm_config])

        # Verify llm_configs
        assert len(args.llm_configs) == 1
        assert isinstance(args.llm_configs[0], LLMConfig)

        # Verify defaults
        assert isinstance(args.ingress_cls_config, IngressClsConfig)
        assert args.ingress_cls_config.ingress_cls == OpenAiIngress
        assert args.ingress_deployment_config == {}

    def test_flexible_input_types(self, llm_config):
        """Test accepts dicts, objects, and mixed types for llm_configs."""
        config_dict = {
            "model_loading_config": {
                "model_id": "test-model-2",
                "model_source": "test-source-2",
            }
        }
        args = LLMServingArgs(llm_configs=[llm_config, config_dict])
        assert len(args.llm_configs) == 2
        assert all(isinstance(c, LLMConfig) for c in args.llm_configs)

    def test_ingress_config_flexibility(self, llm_config):
        """Test ingress_cls_config: defaults, dict input, object input, and class loading."""
        # Test defaults
        args_default = LLMServingArgs(llm_configs=[llm_config])
        assert isinstance(args_default.ingress_cls_config, IngressClsConfig)
        assert args_default.ingress_cls_config.ingress_cls == OpenAiIngress
        assert args_default.ingress_cls_config.ingress_extra_kwargs == {}

        # Test as dict with custom kwargs
        args_dict = LLMServingArgs(
            llm_configs=[llm_config],
            ingress_cls_config={"ingress_extra_kwargs": {"key": "value"}},
        )
        assert isinstance(args_dict.ingress_cls_config, IngressClsConfig)
        assert args_dict.ingress_cls_config.ingress_extra_kwargs == {"key": "value"}

        # Test as object
        args_obj = LLMServingArgs(
            llm_configs=[llm_config],
            ingress_cls_config=IngressClsConfig(ingress_extra_kwargs={"key": "value"}),
        )
        assert isinstance(args_obj.ingress_cls_config, IngressClsConfig)
        assert args_obj.ingress_cls_config.ingress_extra_kwargs == {"key": "value"}

        # Test class loading from string
        args_str = LLMServingArgs(
            llm_configs=[llm_config],
            ingress_cls_config={
                "ingress_cls": "ray.llm._internal.serve.core.ingress.ingress:OpenAiIngress"
            },
        )
        assert args_str.ingress_cls_config.ingress_cls == OpenAiIngress

    def test_validation_rules(self):
        """Test validation: unique model IDs and non-empty list."""
        # Duplicate model IDs
        config1 = LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id="same-id", model_source="source1"
            )
        )
        config2 = LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id="same-id", model_source="source2"
            )
        )
        with pytest.raises(ValueError, match="Duplicate models found"):
            LLMServingArgs(llm_configs=[config1, config2])

        # Empty list
        with pytest.raises(ValueError, match="List of models is empty"):
            LLMServingArgs(llm_configs=[])


class TestBuildOpenaiApp:
    @pytest.fixture
    def llm_config(self):
        """Basic LLMConfig for testing."""
        return LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id="test-model", model_source="test-source"
            )
        )

    def test_build_openai_app(
        self, get_llm_serve_args, shutdown_ray_and_serve, disable_placement_bundles
    ):
        """Test `build_openai_app` can build app and run it with Serve."""

        app = build_openai_app(
            get_llm_serve_args,
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
                ],
                ingress_deployment_config={
                    "autoscaling_config": {
                        "min_replicas": 8,
                        "initial_replicas": 10,
                        "max_replicas": 12,
                        "target_ongoing_requests": 10,
                    }
                },
            )
        )
        router_autoscaling_config = (
            app._bound_deployment._deployment_config.autoscaling_config
        )
        assert router_autoscaling_config.min_replicas == 8  # (1 + 1 + 2) * 2
        assert router_autoscaling_config.initial_replicas == 10  # (1 + 1 + 3) * 2
        assert router_autoscaling_config.max_replicas == 12  # (1 + 1 + 4) * 2
        assert router_autoscaling_config.target_ongoing_requests == 10

    def test_ingress_deployment_config_merging(
        self, llm_config, disable_placement_bundles
    ):
        """Test that ingress_deployment_config is properly merged with default options.

        This test ensures that deep_merge_dicts return value is properly assigned
        and that nested dictionaries are properly deep-merged without losing default values.
        """
        # Build app with custom ingress deployment config including nested options
        app = build_openai_app(
            dict(
                llm_configs=[llm_config],
                ingress_deployment_config={
                    "num_replicas": 3,
                    "ray_actor_options": {
                        "num_cpus": 4,
                        "memory": 1024,
                    },
                    "max_ongoing_requests": 200,  # Override default
                },
            )
        )

        # Verify the custom config was applied
        deployment = app._bound_deployment
        assert deployment._deployment_config.num_replicas == 3
        assert deployment.ray_actor_options["num_cpus"] == 4
        assert deployment.ray_actor_options["memory"] == 1024
        assert deployment._deployment_config.max_ongoing_requests == 200

    def test_default_autoscaling_config_included_without_num_replicas(
        self, llm_config, disable_placement_bundles
    ):
        """Test that default autoscaling_config with target_ongoing_requests is included
        when num_replicas is not specified.
        """
        app = build_openai_app(
            dict(
                llm_configs=[llm_config],
            )
        )

        deployment = app._bound_deployment
        autoscaling_config = deployment._deployment_config.autoscaling_config
        assert autoscaling_config is not None
        assert (
            autoscaling_config.target_ongoing_requests
            == DEFAULT_MAX_TARGET_ONGOING_REQUESTS
        )

    def test_autoscaling_config_removed_from_defaults_when_num_replicas_specified(
        self, llm_config, disable_placement_bundles
    ):
        """Test that autoscaling_config from defaults is removed when user specifies
        num_replicas, since Ray Serve does not allow both.
        """
        app = build_openai_app(
            dict(
                llm_configs=[llm_config],
                ingress_deployment_config={
                    "num_replicas": 2,
                },
            )
        )

        deployment = app._bound_deployment
        assert deployment._deployment_config.num_replicas == 2
        # autoscaling_config should be None since num_replicas is set
        assert deployment._deployment_config.autoscaling_config is None

    def test_user_target_ongoing_requests_respected(
        self, llm_config, disable_placement_bundles
    ):
        """Test that user-specified target_ongoing_requests is respected and not
        overridden by defaults.
        """
        user_target = 50
        app = build_openai_app(
            dict(
                llm_configs=[llm_config],
                ingress_deployment_config={
                    "autoscaling_config": {
                        "target_ongoing_requests": user_target,
                    },
                },
            )
        )

        deployment = app._bound_deployment
        autoscaling_config = deployment._deployment_config.autoscaling_config
        assert autoscaling_config is not None
        assert autoscaling_config.target_ongoing_requests == user_target


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
