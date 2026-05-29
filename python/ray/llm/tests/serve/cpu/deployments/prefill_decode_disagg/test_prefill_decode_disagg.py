import sys
import warnings
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from ray.llm._internal.serve.core.configs.llm_config import (
    LLMConfig,
    ModelLoadingConfig,
)
from ray.llm._internal.serve.core.configs.openai_api_models import (
    ChatCompletionRequest,
)
from ray.llm._internal.serve.core.ingress.builder import (
    IngressClsConfig,
)
from ray.llm._internal.serve.core.ingress.ingress import OpenAiIngress
from ray.llm._internal.serve.core.server.llm_server import LLMServer
from ray.llm._internal.serve.serving_patterns.prefill_decode.builder import (
    PDServingArgs,
    build_pd_openai_app,
)
from ray.llm._internal.serve.serving_patterns.prefill_decode.pd_server import (
    PDDecodeServer,
    PDOrchestratorMixin,
    PDPrefillServer,
)
from ray.serve._private.http_util import SERVE_SESSION_ID


async def _aiter(items):
    for item in items:
        yield item


class _FakePrefillHandle:
    """Fake prefill DeploymentHandle. Records each .chat/.completions remote
    call with the session_id from any preceding ``.options(session_id=...)``,
    and yields one chunk with kv_transfer_params back to the orchestrator."""

    def __init__(self, calls=None, session_id=None):
        self.calls = calls if calls is not None else []
        self.session_id = session_id

    def options(self, **kwargs):
        return _FakePrefillHandle(
            calls=self.calls,
            session_id=kwargs.get("session_id", self.session_id),
        )

    def _method(self, name):
        def remote(request, raw_request_info):
            self.calls.append(
                {"method": name, "request": request, "session_id": self.session_id}
            )
            return _aiter(
                [SimpleNamespace(kv_transfer_params={"remote_engine_id": "prefill-1"})]
            )

        return SimpleNamespace(remote=remote)

    @property
    def chat(self):
        return self._method("chat")

    @property
    def completions(self):
        return self._method("completions")


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

        # TODO(Kourosh): Deprecated, remove in Ray 2.58.
        assert args.proxy_cls_config is None
        assert args.proxy_deployment_config is None
        assert isinstance(args.ingress_cls_config, IngressClsConfig)
        assert args.ingress_cls_config.ingress_cls == OpenAiIngress
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

    # TODO(Kourosh): Deprecated, remove in Ray 2.58.
    def test_proxy_config_deprecated(self, pd_configs):
        """Test proxy_cls_config and proxy_deployment_config emit deprecation warnings."""
        prefill, decode = pd_configs

        # proxy_cls_config as dict should warn
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            PDServingArgs(
                prefill_config=prefill,
                decode_config=decode,
                proxy_cls_config={"proxy_extra_kwargs": {"key": "value"}},
            )
            deprecation_msgs = [
                str(warning.message)
                for warning in w
                if issubclass(warning.category, DeprecationWarning)
            ]
            assert any(
                "proxy_cls_config is deprecated" in msg for msg in deprecation_msgs
            )

        # proxy_deployment_config should warn
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            PDServingArgs(
                prefill_config=prefill,
                decode_config=decode,
                proxy_deployment_config={"num_replicas": 2},
            )
            deprecation_msgs = [
                str(warning.message)
                for warning in w
                if issubclass(warning.category, DeprecationWarning)
            ]
            assert any(
                "proxy_deployment_config is deprecated" in msg
                for msg in deprecation_msgs
            )

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


class TestPDOrchestratorMixin:
    def test_prepare_prefill_request_limits_chat_to_one_token(self):
        request = ChatCompletionRequest(
            model="test-model",
            messages=[{"role": "user", "content": "hello"}],
            max_completion_tokens=32,
            stream=True,
            stream_options={"include_usage": True},
        )

        prefill_request = PDOrchestratorMixin._prepare_prefill_request(request)

        assert prefill_request.max_tokens == 1
        assert prefill_request.max_completion_tokens == 1
        assert prefill_request.stream is False
        assert prefill_request.stream_options is None
        assert request.max_completion_tokens == 32
        assert request.stream is True

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "method,path,body",
        [
            (
                "chat",
                "/v1/chat/completions",
                {"messages": [{"role": "user", "content": "hi"}]},
            ),
            ("completions", "/v1/completions", {"prompt": "hi"}),
        ],
    )
    async def test_direct_streaming_http_runs_pd_orchestration(
        self, method, path, body
    ):
        """HTTP traffic to PDDecodeServer's direct-streaming ASGI app must
        flow through PD orchestration (remote prefill, then local decode),
        propagate the session-id header to the prefill handle, and pass
        the prefill's kv_transfer_params to the local decode call.
        Regression for https://github.com/ray-project/ray/pull/63679.
        """
        from fastapi.testclient import TestClient

        from ray.llm.tests.serve.mocks.mock_vllm_engine import MockVLLMEngine

        server = PDDecodeServer.__new__(PDDecodeServer)
        server._prefill_handle = _FakePrefillHandle()
        server._llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test-model")
        )
        # The direct-streaming app starts from the engine-native ASGI app, so
        # the decode server needs a (mock) engine. PD only re-points the
        # chat/completions routes at the orchestrator, patched below.
        server.engine = MockVLLMEngine(server._llm_config)
        await server.engine.start()

        decode_calls = []

        async def _fake_decode(self, req, raw_info):
            decode_calls.append(req)
            return _aiter([['data: {"ok":true}\n\n']])

        app = await server.__serve_build_asgi_app__()
        with patch.object(LLMServer, method, _fake_decode):
            with TestClient(app) as client:
                resp = client.post(
                    path,
                    json={"model": "test-model", "stream": True, **body},
                    headers={SERVE_SESSION_ID: "session-a"},
                )

        assert resp.status_code == 200, resp.text
        assert server._prefill_handle.calls[0]["method"] == method
        assert server._prefill_handle.calls[0]["session_id"] == "session-a"
        assert decode_calls[0].kv_transfer_params == {"remote_engine_id": "prefill-1"}


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

    def test_3_tier_graph_structure(self, pd_configs):
        """Test that build_pd_openai_app creates a 3-tier graph:
        ingress -> PDDecodeServer -> PDPrefillServer.
        """
        prefill, decode = pd_configs
        app = build_pd_openai_app({"prefill_config": prefill, "decode_config": decode})

        # The app should have an ingress deployment bound to the decode deployment
        ingress_deployment = app._bound_deployment
        llm_deployments = ingress_deployment.init_kwargs["llm_deployments"]
        # Single model id -> single decode app (P/D shares the same model_id).
        assert len(llm_deployments) == 1
        decode_app = next(iter(llm_deployments.values()))
        decode_deployment = decode_app._bound_deployment

        assert decode_deployment.func_or_class is PDDecodeServer

        # Decode should have a prefill_server in its bind kwargs
        assert "prefill_server" in decode_deployment.init_kwargs

        # The prefill_server should be a PDPrefillServer Application
        prefill_app = decode_deployment.init_kwargs["prefill_server"]
        prefill_deployment = prefill_app._bound_deployment
        assert prefill_deployment.func_or_class is PDPrefillServer

    def test_ingress_deployment_config(self, pd_configs):
        """Test that ingress deployment configs are properly applied."""
        prefill, decode = pd_configs
        app = build_pd_openai_app(
            {
                "prefill_config": prefill,
                "decode_config": decode,
                "ingress_deployment_config": {
                    "num_replicas": 5,
                    "ray_actor_options": {
                        "num_cpus": 8,
                        "memory": 4096,
                    },
                    "max_ongoing_requests": 300,
                },
            }
        )

        ingress_deployment = app._bound_deployment
        assert ingress_deployment._deployment_config.num_replicas == 5
        assert ingress_deployment.ray_actor_options["num_cpus"] == 8
        assert ingress_deployment.ray_actor_options["memory"] == 4096
        assert ingress_deployment._deployment_config.max_ongoing_requests == 300

    # TODO(Kourosh): Deprecated, remove in Ray 2.58.
    def test_deprecated_proxy_config_ignored(self, pd_configs):
        """Test that deprecated proxy configs are accepted but ignored."""
        prefill, decode = pd_configs

        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            app = build_pd_openai_app(
                {
                    "prefill_config": prefill,
                    "decode_config": decode,
                    "proxy_deployment_config": {
                        "num_replicas": 99,
                    },
                }
            )
            # App should still be valid — proxy config is just ignored
            assert app is not None


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
