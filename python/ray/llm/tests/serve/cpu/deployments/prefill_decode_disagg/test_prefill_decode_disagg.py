import sys

import pytest

from ray.llm._internal.serve.core.configs.llm_config import ModelLoadingConfig
from ray.llm._internal.serve.core.ingress.builder import (
    IngressClsConfig,
)
from ray.llm._internal.serve.core.ingress.ingress import OpenAiIngress
from ray.llm._internal.serve.serving_patterns.prefill_decode.builder import (
    PDServingArgs,
    ProxyClsConfig,
    build_pd_openai_app,
)
from ray.llm._internal.serve.serving_patterns.prefill_decode.pd_server import (
    PDProxyServer,
)
from ray.serve.llm import LLMConfig


class TestPDServingArgs:
    """Test suite for PDServingArgs data model."""

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

    def test_basic_creation_and_defaults(self, pd_configs):
        """Test creation with minimal config and verify defaults."""
        prefill, decode = pd_configs
        args = PDServingArgs(prefill_config=prefill, decode_config=decode)

        # Verify configs
        assert isinstance(args.prefill_config, LLMConfig)
        assert isinstance(args.decode_config, LLMConfig)

        # Verify defaults
        assert isinstance(args.proxy_cls_config, ProxyClsConfig)
        assert args.proxy_cls_config.proxy_cls == PDProxyServer
        assert isinstance(args.ingress_cls_config, IngressClsConfig)
        assert args.ingress_cls_config.ingress_cls == OpenAiIngress
        assert args.proxy_deployment_config == {}
        assert args.ingress_deployment_config == {}

    def test_flexible_input_types(self):
        """Test accepts dicts for prefill and decode configs."""
        config_dict = {
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
        args = PDServingArgs(prefill_config=config_dict, decode_config=config_dict)
        assert isinstance(args.prefill_config, LLMConfig)
        assert isinstance(args.decode_config, LLMConfig)

    def test_proxy_config_flexibility(self, pd_configs):
        """Test proxy_cls_config: defaults, dict input, object input, and class loading."""
        prefill, decode = pd_configs

        # Test defaults
        args_default = PDServingArgs(prefill_config=prefill, decode_config=decode)
        assert isinstance(args_default.proxy_cls_config, ProxyClsConfig)
        assert args_default.proxy_cls_config.proxy_cls == PDProxyServer
        assert args_default.proxy_cls_config.proxy_extra_kwargs == {}

        # Test as dict with custom kwargs
        args_dict = PDServingArgs(
            prefill_config=prefill,
            decode_config=decode,
            proxy_cls_config={"proxy_extra_kwargs": {"key": "value"}},
        )
        assert isinstance(args_dict.proxy_cls_config, ProxyClsConfig)
        assert args_dict.proxy_cls_config.proxy_extra_kwargs == {"key": "value"}

        # Test as object
        args_obj = PDServingArgs(
            prefill_config=prefill,
            decode_config=decode,
            proxy_cls_config=ProxyClsConfig(proxy_extra_kwargs={"key": "value"}),
        )
        assert isinstance(args_obj.proxy_cls_config, ProxyClsConfig)
        assert args_obj.proxy_cls_config.proxy_extra_kwargs == {"key": "value"}

        # Test class loading from string
        args_str = PDServingArgs(
            prefill_config=prefill,
            decode_config=decode,
            proxy_cls_config={
                "proxy_cls": "ray.llm._internal.serve.serving_patterns.prefill_decode.pd_server:PDProxyServer"
            },
        )
        assert args_str.proxy_cls_config.proxy_cls == PDProxyServer

    def test_ingress_config_flexibility(self, pd_configs):
        """Test ingress_cls_config: defaults, dict input, object input, and class loading."""
        prefill, decode = pd_configs

        # Test defaults
        args_default = PDServingArgs(prefill_config=prefill, decode_config=decode)
        assert isinstance(args_default.ingress_cls_config, IngressClsConfig)
        assert args_default.ingress_cls_config.ingress_cls == OpenAiIngress
        assert args_default.ingress_cls_config.ingress_extra_kwargs == {}

        # Test as dict with custom kwargs
        args_dict = PDServingArgs(
            prefill_config=prefill,
            decode_config=decode,
            ingress_cls_config={"ingress_extra_kwargs": {"key": "value"}},
        )
        assert isinstance(args_dict.ingress_cls_config, IngressClsConfig)
        assert args_dict.ingress_cls_config.ingress_extra_kwargs == {"key": "value"}

        # Test as object
        args_obj = PDServingArgs(
            prefill_config=prefill,
            decode_config=decode,
            ingress_cls_config=IngressClsConfig(ingress_extra_kwargs={"key": "value"}),
        )
        assert isinstance(args_obj.ingress_cls_config, IngressClsConfig)
        assert args_obj.ingress_cls_config.ingress_extra_kwargs == {"key": "value"}

        # Test class loading from string
        args_str = PDServingArgs(
            prefill_config=prefill,
            decode_config=decode,
            ingress_cls_config={
                "ingress_cls": "ray.llm._internal.serve.core.ingress.ingress:OpenAiIngress"
            },
        )
        assert args_str.ingress_cls_config.ingress_cls == OpenAiIngress

    def test_validation_rules(self):
        """Test validation: matching model IDs and required kv_transfer_config."""
        # Mismatched model IDs
        prefill = LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id="model-1", model_source="source"
            ),
            engine_kwargs={"kv_transfer_config": {"kv_connector": "NixlConnector"}},
        )
        decode = LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id="model-2", model_source="source"
            ),
            engine_kwargs={"kv_transfer_config": {"kv_connector": "NixlConnector"}},
        )
        with pytest.raises(ValueError, match="P/D model id mismatch"):
            PDServingArgs(prefill_config=prefill, decode_config=decode)

        # Missing kv_transfer_config
        prefill_no_kv = LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id="test-model", model_source="test-source"
            )
        )
        decode_no_kv = LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id="test-model", model_source="test-source"
            )
        )
        with pytest.raises(ValueError, match="kv_transfer_config is required"):
            PDServingArgs(prefill_config=prefill_no_kv, decode_config=decode_no_kv)


class TestServingArgsParsing:
    @pytest.mark.parametrize("kv_connector", ["NixlConnector", "LMCacheConnectorV1"])
    def test_parse_dict(self, kv_connector: str):
        prefill_config = LLMConfig(
            model_loading_config=dict(
                model_id="qwen-0.5b",
                model_source="Qwen/Qwen2.5-0.5B-Instruct",
            ),
            deployment_config=dict(
                autoscaling_config=dict(
                    min_replicas=2,
                    max_replicas=2,
                )
            ),
            engine_kwargs=dict(
                tensor_parallel_size=1,
                kv_transfer_config=dict(
                    kv_connector=kv_connector,
                    kv_role="kv_both",
                ),
            ),
        )

        decode_config = LLMConfig(
            model_loading_config=dict(
                model_id="qwen-0.5b",
                model_source="Qwen/Qwen2.5-0.5B-Instruct",
            ),
            deployment_config=dict(
                autoscaling_config=dict(
                    min_replicas=1,
                    max_replicas=1,
                )
            ),
            engine_kwargs=dict(
                tensor_parallel_size=1,
                kv_transfer_config=dict(
                    kv_connector=kv_connector,
                    kv_role="kv_both",
                ),
            ),
        )

        pd_config = {"prefill_config": prefill_config, "decode_config": decode_config}

        app = build_pd_openai_app(pd_config)
        assert app is not None


class TestBuildPDOpenaiApp:
    """Test suite for build_pd_openai_app function."""

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

    def test_deployment_config_merging(self, pd_configs):
        """Test that deployment configs are properly merged with default options.

        This test ensures that deep_merge_dicts return value is properly assigned
        for both proxy and ingress deployments, and that nested dictionaries are
        properly deep-merged without losing default values.
        """
        prefill, decode = pd_configs

        # Build app with custom configs for both proxy and ingress including nested options
        app = build_pd_openai_app(
            {
                "prefill_config": prefill,
                "decode_config": decode,
                "proxy_deployment_config": {
                    "num_replicas": 2,
                    "ray_actor_options": {
                        "num_cpus": 4,
                        "memory": 2048,
                    },
                    "max_ongoing_requests": 150,  # Override default
                },
                "ingress_deployment_config": {
                    "num_replicas": 5,
                    "ray_actor_options": {
                        "num_cpus": 8,
                        "memory": 4096,
                    },
                    "max_ongoing_requests": 300,  # Override default
                },
            }
        )

        # The app should have an ingress deployment bound to a proxy deployment
        # The proxy is passed as an Application via llm_deployments in init_kwargs
        ingress_deployment = app._bound_deployment
        proxy_app = ingress_deployment.init_kwargs["llm_deployments"][0]
        proxy_deployment = proxy_app._bound_deployment

        # Verify proxy config was applied with deep merge
        assert proxy_deployment._deployment_config.num_replicas == 2
        assert proxy_deployment.ray_actor_options["num_cpus"] == 4
        assert proxy_deployment.ray_actor_options["memory"] == 2048
        assert proxy_deployment._deployment_config.max_ongoing_requests == 150

        # Verify ingress config was applied with deep merge
        assert ingress_deployment._deployment_config.num_replicas == 5
        assert ingress_deployment.ray_actor_options["num_cpus"] == 8
        assert ingress_deployment.ray_actor_options["memory"] == 4096
        assert ingress_deployment._deployment_config.max_ongoing_requests == 300


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
