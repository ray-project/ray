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
    LoraConfig,
    ModelLoadingConfig,
)
from ray.llm._internal.serve.core.ingress.builder import (
    IngressClsConfig,
    LLMServingArgs,
    build_openai_app,
)
from ray.llm._internal.serve.core.ingress.dev_ingress import DevIngress
from ray.llm._internal.serve.core.ingress.ingress import (
    DirectStreamingIngress,
    OpenAiIngress,
)
from ray.llm._internal.serve.routing_policies.kv_aware.kv_aware_router import (
    KVAwareRouter,
)
from ray.llm._internal.serve.serving_patterns.data_parallel.builder import (
    build_dp_openai_app,
)
from ray.llm._internal.serve.serving_patterns.data_parallel.dp_server import (
    DPServer,
)
from ray.llm._internal.serve.serving_patterns.prefill_decode.builder import (
    build_pd_openai_app,
)
from ray.llm._internal.serve.serving_patterns.prefill_decode.pd_server import (
    DPPDDecodeServer,
    DPPDPrefillServer,
    PDDecodeServer,
    PDPrefillServer,
)
from ray.serve._private.http_util import ASGIAppReplicaWrapper
from ray.serve.config import AutoscalingConfig, RequestRouterConfig
from ray.serve.experimental.consistent_hash_router import ConsistentHashRouter
from ray.serve.experimental.round_robin_router import RoundRobinRouter


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


class TestDirectStreamingOpenAiApp:
    """`build_openai_app` topology when direct streaming is enabled.

        DirectStreamingIngress      app root; discovery routes only
        |- LLMServer:<model>        one per LLMConfig, _direct_http=True
        `- LLMRouter                ingress request router

    The same shape is built for one model and for many; the single-model case
    is not a special case of the builder, only of what the validators allow.
    """

    @pytest.fixture(name="enable_direct_streaming", autouse=True)
    def _enable_direct_streaming(self, monkeypatch):
        monkeypatch.setattr(
            "ray.llm._internal.serve.core.ingress.builder."
            "RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING",
            True,
        )
        # Multi-model selection reads the `model` field off the request body,
        # which HAProxy only forwards when this is on.
        monkeypatch.setattr(
            "ray.llm._internal.serve.core.ingress.builder."
            "RAY_SERVE_INGRESS_REQUEST_ROUTER_FORWARD_BODY",
            True,
        )

    @pytest.fixture(name="llm_configs")
    def _llm_configs(self, llm_config):
        """Two models, the second with a `/` in its id as real HF ids have."""
        second = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="meta-llama/other-model")
        )
        return [llm_config, second]

    @staticmethod
    def _build(llm_configs, **kwargs):
        return build_openai_app(LLMServingArgs(llm_configs=llm_configs, **kwargs))

    @pytest.mark.parametrize("num_models", [1, 2])
    def test_control_ingress_is_the_app_root(
        self, llm_configs, disable_placement_bundles, num_models
    ):
        app = self._build(llm_configs[:num_models])

        assert app._bound_deployment.name == "DirectStreamingIngress"
        assert issubclass(app._bound_deployment.func_or_class, DirectStreamingIngress)
        # It is an ASGI deployment, but not the model server: it must not be
        # `_direct_http`, or the app would have no front door.
        assert issubclass(app._bound_deployment.func_or_class, ASGIAppReplicaWrapper)
        assert app._bound_deployment._direct_http is False

    @pytest.mark.parametrize("num_models", [1, 2])
    def test_every_model_deployment_is_direct_http(
        self, llm_configs, disable_placement_bundles, num_models
    ):
        app = self._build(llm_configs[:num_models])

        llm_deployments = app._bound_deployment.init_kwargs["llm_deployments"]
        assert sorted(llm_deployments) == sorted(
            c.model_id for c in llm_configs[:num_models]
        )
        for model_app in llm_deployments.values():
            assert model_app._bound_deployment._direct_http is True
            assert issubclass(
                model_app._bound_deployment.func_or_class, ASGIAppReplicaWrapper
            )

    @pytest.mark.parametrize("num_models", [1, 2])
    def test_router_gets_every_server_and_the_control_ingress(
        self, llm_configs, disable_placement_bundles, num_models
    ):
        app = self._build(llm_configs[:num_models])
        router = app._ingress_request_router

        assert router is not None
        assert router._bound_deployment.name == "LLMRouter"
        assert sorted(router._bound_deployment.init_kwargs["servers"]) == sorted(
            c.model_id for c in llm_configs[:num_models]
        )
        assert router._bound_deployment.init_kwargs["ingress"] is app

    @pytest.mark.parametrize("num_models", [1, 2])
    def test_ingress_and_router_share_the_same_model_applications(
        self, llm_configs, disable_placement_bundles, num_models
    ):
        """Serve's graph traversal dedupes on object identity.

        Rebuilding the model deployments for the router would produce a second
        copy of every model, and `build_app` would reject the router for
        introducing more than one deployment of its own.
        """
        app = self._build(llm_configs[:num_models])

        ingress_servers = app._bound_deployment.init_kwargs["llm_deployments"]
        router_servers = app._ingress_request_router._bound_deployment.init_kwargs[
            "servers"
        ]
        assert set(ingress_servers) == set(router_servers)
        for model_id, model_app in ingress_servers.items():
            assert router_servers[model_id] is model_app

    def test_model_cards_and_lora_paths_reach_the_control_ingress(
        self, llm_config, disable_placement_bundles
    ):
        llm_config.lora_config = LoraConfig(
            dynamic_lora_loading_path="s3://fake-bucket/lora"
        )
        app = self._build([llm_config])

        init_kwargs = app._bound_deployment.init_kwargs
        assert sorted(init_kwargs["model_cards"]) == [llm_config.model_id]
        assert init_kwargs["model_cards"][llm_config.model_id].id == (
            llm_config.model_id
        )
        assert init_kwargs["lora_paths"] == {
            llm_config.model_id: "s3://fake-bucket/lora"
        }

    def test_ingress_deployment_config_is_applied(
        self, llm_config, disable_placement_bundles
    ):
        """Unlike DP/PD, there is a real ingress deployment to configure."""
        app = self._build(
            [llm_config],
            ingress_deployment_config={"num_replicas": 3, "max_ongoing_requests": 17},
        )

        deployment_config = app._bound_deployment._deployment_config
        assert deployment_config.num_replicas == 3
        assert deployment_config.max_ongoing_requests == 17

    def test_default_request_router_still_applies_to_model_deployments(
        self, llm_config, disable_placement_bundles
    ):
        app = self._build([llm_config])

        (model_app,) = app._bound_deployment.init_kwargs["llm_deployments"].values()
        request_router_config = (
            model_app._bound_deployment._deployment_config.request_router_config
        )
        assert request_router_config.request_router_class == (
            f"{RoundRobinRouter.__module__}.{RoundRobinRouter.__name__}"
        )

    def test_user_request_router_config_wins(
        self, llm_config, disable_placement_bundles
    ):
        """A user-supplied `request_router_config` must survive the wiring."""
        llm_config.deployment_config["request_router_config"] = RequestRouterConfig(
            request_router_class=ConsistentHashRouter,
        )
        app = self._build([llm_config])

        (model_app,) = app._bound_deployment.init_kwargs["llm_deployments"].values()
        request_router_config = (
            model_app._bound_deployment._deployment_config.request_router_config
        )
        assert request_router_config.request_router_class == (
            f"{ConsistentHashRouter.__module__}.{ConsistentHashRouter.__name__}"
        )

    def test_single_kv_aware_model_is_passed_to_the_router(
        self, llm_config, disable_placement_bundles
    ):
        """KV-aware routing still works, but only for a lone model."""
        llm_config.deployment_config["request_router_config"] = RequestRouterConfig(
            request_router_class=KVAwareRouter,
        )
        app = self._build([llm_config])

        assert (
            app._ingress_request_router._bound_deployment.init_kwargs["llm_config"]
            is llm_config
        )

    def test_non_kv_aware_model_passes_no_llm_config(
        self, llm_config, disable_placement_bundles
    ):
        app = self._build([llm_config])

        assert (
            app._ingress_request_router._bound_deployment.init_kwargs["llm_config"]
            is None
        )


class TestDirectStreamingOpenAiAppRejections:
    """Configurations the multi-model builder must refuse rather than mis-serve."""

    @pytest.fixture(name="enable_direct_streaming", autouse=True)
    def _enable_direct_streaming(self, monkeypatch):
        monkeypatch.setattr(
            "ray.llm._internal.serve.core.ingress.builder."
            "RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING",
            True,
        )
        monkeypatch.setattr(
            "ray.llm._internal.serve.core.ingress.builder."
            "RAY_SERVE_INGRESS_REQUEST_ROUTER_FORWARD_BODY",
            True,
        )

    @staticmethod
    def _second_config(**kwargs) -> LLMConfig:
        return LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="other-model"), **kwargs
        )

    @pytest.mark.parametrize(
        ("ingress_deployment_config", "match"),
        [
            (
                {"autoscaling_config": {"min_replicas": 0}},
                "cannot scale to zero",
            ),
            (
                {"autoscaling_config": AutoscalingConfig(min_replicas=0)},
                "cannot scale to zero",
            ),
        ],
    )
    def test_rejects_a_scale_to_zero_control_ingress(
        self,
        llm_config,
        disable_placement_bundles,
        ingress_deployment_config,
        match,
    ):
        with pytest.raises(ValueError, match=match):
            build_openai_app(
                LLMServingArgs(
                    llm_configs=[llm_config],
                    ingress_deployment_config=ingress_deployment_config,
                )
            )

    def test_allows_an_explicit_non_zero_min_replicas(
        self, llm_config, disable_placement_bundles
    ):
        app = build_openai_app(
            LLMServingArgs(
                llm_configs=[llm_config],
                ingress_deployment_config={
                    "autoscaling_config": {"min_replicas": 2, "max_replicas": 4}
                },
            )
        )
        assert (
            app._bound_deployment._deployment_config.autoscaling_config.min_replicas
            == 2
        )

    @pytest.mark.parametrize(
        "ingress_cls_config",
        [
            {"ingress_extra_kwargs": {"key": "value"}},
            {"ingress_cls": DevIngress},
        ],
    )
    def test_rejects_a_custom_ingress_class_or_kwargs(
        self, llm_config, disable_placement_bundles, ingress_cls_config
    ):
        with pytest.raises(ValueError, match="does not support ingress_cls_config"):
            build_openai_app(
                LLMServingArgs(
                    llm_configs=[llm_config],
                    ingress_cls_config=ingress_cls_config,
                )
            )

    def test_rejects_multiple_models_without_body_forwarding(
        self, llm_config, disable_placement_bundles, monkeypatch
    ):
        monkeypatch.setattr(
            "ray.llm._internal.serve.core.ingress.builder."
            "RAY_SERVE_INGRESS_REQUEST_ROUTER_FORWARD_BODY",
            False,
        )
        with pytest.raises(
            ValueError,
            match="RAY_SERVE_INGRESS_REQUEST_ROUTER_FORWARD_BODY=1",
        ):
            build_openai_app(
                LLMServingArgs(llm_configs=[llm_config, self._second_config()])
            )

    def test_single_model_does_not_need_body_forwarding(
        self, llm_config, disable_placement_bundles, monkeypatch
    ):
        """One model needs no `model` field, so it needs no body."""
        monkeypatch.setattr(
            "ray.llm._internal.serve.core.ingress.builder."
            "RAY_SERVE_INGRESS_REQUEST_ROUTER_FORWARD_BODY",
            False,
        )
        app = build_openai_app(LLMServingArgs(llm_configs=[llm_config]))
        assert app._bound_deployment.name == "DirectStreamingIngress"

    def test_rejects_multiple_models_when_any_is_kv_aware(
        self, llm_config, disable_placement_bundles
    ):
        other = self._second_config(
            deployment_config={
                "request_router_config": RequestRouterConfig(
                    request_router_class=KVAwareRouter,
                )
            }
        )
        with pytest.raises(ValueError, match="KV-aware routing supports one model"):
            build_openai_app(LLMServingArgs(llm_configs=[llm_config, other]))

    def test_rejects_multiple_models_when_any_uses_lora(
        self, llm_config, disable_placement_bundles
    ):
        llm_config.lora_config = LoraConfig(
            dynamic_lora_loading_path="s3://fake-bucket/lora"
        )
        with pytest.raises(ValueError, match="LoRA supports one model"):
            build_openai_app(
                LLMServingArgs(llm_configs=[llm_config, self._second_config()])
            )

    def test_single_model_lora_discovery_is_still_supported(
        self, llm_config, disable_placement_bundles
    ):
        llm_config.lora_config = LoraConfig(
            dynamic_lora_loading_path="s3://fake-bucket/lora"
        )
        app = build_openai_app(LLMServingArgs(llm_configs=[llm_config]))
        assert app._bound_deployment.init_kwargs["lora_paths"] == {
            llm_config.model_id: "s3://fake-bucket/lora"
        }


class TestDirectStreamingDP:
    """Direct-streaming wiring tests for the data-parallel builder.

    Mirrors the ``test_direct_streaming_*`` tests on ``TestBuildOpenaiApp``
    but exercises ``build_dp_openai_app`` so that regressions in the DP
    wiring (deployment class, default request router) are caught at CPU
    unit-test speed instead of in GPU integration / release tests.
    """

    @pytest.fixture
    def llm_config(self):
        return LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id="test-model", model_source="test-source"
            )
        )

    def _enable_direct_streaming(self, monkeypatch):
        monkeypatch.setattr(
            "ray.llm._internal.serve.serving_patterns.data_parallel.builder."
            "RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING",
            True,
        )

    def test_dp_builds_dpserver_ingress_with_router_attached(
        self, llm_config, disable_placement_bundles, monkeypatch
    ):
        self._enable_direct_streaming(monkeypatch)

        app = build_dp_openai_app({"llm_config": llm_config})
        ingress_request_router = app._ingress_request_router

        assert app._bound_deployment.name == "DPServer:test-model"
        assert issubclass(app._bound_deployment.func_or_class, ASGIAppReplicaWrapper)
        assert issubclass(app._bound_deployment.func_or_class, DPServer)
        assert ingress_request_router is not None
        assert ingress_request_router._bound_deployment.name == "LLMRouter"
        # The DP topology keeps the server deployment as the app ingress, so the
        # router gets one server and no separate control ingress to route to.
        assert ingress_request_router._bound_deployment.init_kwargs["servers"] == {
            llm_config.model_id: app
        }
        assert ingress_request_router._bound_deployment.init_kwargs["ingress"] is None
        assert app._bound_deployment._direct_http is False

        request_router_config = (
            app._bound_deployment._deployment_config.request_router_config
        )
        assert request_router_config.request_router_class == (
            f"{RoundRobinRouter.__module__}.{RoundRobinRouter.__name__}"
        )

    def test_dp_user_request_router_config_wins(
        self, llm_config, disable_placement_bundles, monkeypatch
    ):
        """A user-supplied ``request_router_config`` on ``LLMConfig`` must
        survive DP direct-streaming wiring rather than being overwritten with
        the default ``RoundRobinRouter``.
        """
        self._enable_direct_streaming(monkeypatch)
        llm_config.deployment_config["request_router_config"] = RequestRouterConfig(
            request_router_class=ConsistentHashRouter,
        )

        app = build_dp_openai_app({"llm_config": llm_config})
        request_router_config = (
            app._bound_deployment._deployment_config.request_router_config
        )
        assert request_router_config.request_router_class == (
            f"{ConsistentHashRouter.__module__}.{ConsistentHashRouter.__name__}"
        )


class TestDirectStreamingPD:
    """Direct-streaming wiring tests for the prefill/decode builder.

    Covers the decode-class selection (``PDDecodeServer`` vs
    ``DPPDDecodeServer`` based on ``decode_dp_size``), the prefill binding
    into decode's init kwargs, and the ``LLMRouter`` ingress-request-router
    hookup.
    """

    @pytest.fixture
    def pd_configs(self):
        """Prefill and decode configs with required kv_transfer_config."""
        base_config = {
            "model_loading_config": {
                "model_id": "test-model",
                "model_source": "test-source",
            },
            "engine_kwargs": {
                "kv_transfer_config": {
                    "kv_connector": "NixlConnector",
                    "kv_role": "kv_both",
                },
            },
        }
        prefill = LLMConfig.model_validate(base_config)
        decode = LLMConfig.model_validate(base_config)
        return prefill, decode

    def _enable_direct_streaming(self, monkeypatch):
        monkeypatch.setattr(
            "ray.llm._internal.serve.serving_patterns.prefill_decode.builder."
            "RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING",
            True,
        )

    @staticmethod
    def _set_dp_size(llm_config, size):
        llm_config.engine_kwargs["data_parallel_size"] = size

    @pytest.mark.parametrize(
        ("prefill_dp", "decode_dp", "expected_prefill_cls", "expected_decode_cls"),
        [
            (1, 1, PDPrefillServer, PDDecodeServer),
            (1, 4, PDPrefillServer, DPPDDecodeServer),
            (4, 1, DPPDPrefillServer, PDDecodeServer),
            (4, 4, DPPDPrefillServer, DPPDDecodeServer),
        ],
    )
    def test_pd_decode_class_selection(
        self,
        pd_configs,
        disable_placement_bundles,
        monkeypatch,
        prefill_dp,
        decode_dp,
        expected_prefill_cls,
        expected_decode_cls,
    ):
        """Verify the DP-vs-non-DP variants are picked based on
        ``data_parallel_size`` for both prefill and decode legs.
        """
        self._enable_direct_streaming(monkeypatch)
        prefill, decode = pd_configs
        self._set_dp_size(prefill, prefill_dp)
        self._set_dp_size(decode, decode_dp)

        app = build_pd_openai_app({"prefill_config": prefill, "decode_config": decode})

        decode_deployment = app._bound_deployment
        assert issubclass(decode_deployment.func_or_class, ASGIAppReplicaWrapper)
        assert issubclass(decode_deployment.func_or_class, expected_decode_cls)

        prefill_app = decode_deployment.init_kwargs["prefill_server"]
        prefill_deployment = prefill_app._bound_deployment
        assert prefill_deployment.func_or_class is expected_prefill_cls

    def test_pd_ingress_request_router_is_llmrouter(
        self, pd_configs, disable_placement_bundles, monkeypatch
    ):
        self._enable_direct_streaming(monkeypatch)
        prefill, decode = pd_configs

        app = build_pd_openai_app({"prefill_config": prefill, "decode_config": decode})
        ingress_request_router = app._ingress_request_router

        assert ingress_request_router is not None
        assert ingress_request_router._bound_deployment.name == "LLMRouter"
        # As for DP: decode is the app ingress, so no control ingress is bound.
        assert ingress_request_router._bound_deployment.init_kwargs["servers"] == {
            decode.model_id: app
        }
        assert ingress_request_router._bound_deployment.init_kwargs["ingress"] is None
        assert app._bound_deployment._direct_http is False

        request_router_config = (
            app._bound_deployment._deployment_config.request_router_config
        )
        assert request_router_config.request_router_class == (
            f"{RoundRobinRouter.__module__}.{RoundRobinRouter.__name__}"
        )

    def test_pd_user_request_router_config_wins(
        self, pd_configs, disable_placement_bundles, monkeypatch
    ):
        """A user-supplied ``request_router_config`` on the decode
        ``LLMConfig`` must survive PD direct-streaming wiring rather than
        being overwritten with the default ``RoundRobinRouter``.
        """
        self._enable_direct_streaming(monkeypatch)
        prefill, decode = pd_configs
        decode.deployment_config["request_router_config"] = RequestRouterConfig(
            request_router_class=ConsistentHashRouter,
        )

        app = build_pd_openai_app({"prefill_config": prefill, "decode_config": decode})
        request_router_config = (
            app._bound_deployment._deployment_config.request_router_config
        )
        assert request_router_config.request_router_class == (
            f"{ConsistentHashRouter.__module__}.{ConsistentHashRouter.__name__}"
        )


class TestIngressScaleToZero:
    """Tests for ingress scale-to-zero behavior when all models have min_replicas=0."""

    def test_all_models_scale_to_zero(self, disable_placement_bundles):
        """When all models have min_replicas=0, ingress should also have min_replicas=0."""
        llm_cfg_dict_autoscaling = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="model_a"),
            accelerator_type="L4",
            deployment_config={
                "autoscaling_config": {
                    "min_replicas": 0,
                    "max_replicas": 2,
                }
            },
        )
        llm_cfg_obj_autoscaling = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="model_b"),
            accelerator_type="L4",
            deployment_config={
                "autoscaling_config": AutoscalingConfig(
                    min_replicas=0,
                    max_replicas=4,
                )
            },
        )

        app = build_openai_app(
            LLMServingArgs(
                llm_configs=[llm_cfg_dict_autoscaling, llm_cfg_obj_autoscaling],
            )
        )
        autoscaling_config = app._bound_deployment._deployment_config.autoscaling_config
        assert autoscaling_config.min_replicas == 0

    def test_mixed_min_replicas_keeps_default(self, disable_placement_bundles):
        """When some models have min_replicas>0, ingress should keep default min_replicas."""
        llm_cfg_zero = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="model_a"),
            accelerator_type="L4",
            deployment_config={
                "autoscaling_config": {
                    "min_replicas": 0,
                    "max_replicas": 2,
                }
            },
        )
        llm_cfg_nonzero = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="model_b"),
            accelerator_type="L4",
            deployment_config={
                "autoscaling_config": AutoscalingConfig(
                    min_replicas=1,
                    max_replicas=4,
                )
            },
        )

        app = build_openai_app(
            LLMServingArgs(
                llm_configs=[llm_cfg_zero, llm_cfg_nonzero],
            )
        )
        autoscaling_config = app._bound_deployment._deployment_config.autoscaling_config
        # Default min_replicas from AutoscalingConfig is 1
        assert autoscaling_config.min_replicas == 1

    def test_no_autoscaling_config_keeps_default(self, disable_placement_bundles):
        """When models don't have autoscaling_config, ingress should keep default."""
        llm_cfg = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="model_a"),
            accelerator_type="L4",
        )

        app = build_openai_app(
            LLMServingArgs(llm_configs=[llm_cfg]),
        )
        autoscaling_config = app._bound_deployment._deployment_config.autoscaling_config
        assert autoscaling_config.min_replicas == 1

    def test_user_override_takes_precedence(self, disable_placement_bundles):
        """User-specified ingress min_replicas should override scale-to-zero logic."""
        llm_cfg = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="model_a"),
            accelerator_type="L4",
            deployment_config={
                "autoscaling_config": {
                    "min_replicas": 0,
                    "max_replicas": 2,
                }
            },
        )

        app = build_openai_app(
            LLMServingArgs(
                llm_configs=[llm_cfg],
                ingress_deployment_config={
                    "autoscaling_config": {
                        "min_replicas": 3,
                        "max_replicas": 5,
                    }
                },
            )
        )
        autoscaling_config = app._bound_deployment._deployment_config.autoscaling_config
        assert autoscaling_config.min_replicas == 3


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
