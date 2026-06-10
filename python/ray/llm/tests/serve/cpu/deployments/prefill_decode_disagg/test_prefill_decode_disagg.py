import asyncio
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
    CompletionRequest,
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
        from ray.llm._internal.serve.engines.vllm.kv_transfer.base import (
            DefaultConnectorBackend,
        )

        request = ChatCompletionRequest(
            model="test-model",
            messages=[{"role": "user", "content": "hello"}],
            max_completion_tokens=32,
            stream=True,
            stream_options={"include_usage": True},
        )

        be = DefaultConnectorBackend(llm_config=None)
        prefill_request = be.prepare_prefill_request(request=request, peer=None)

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
        # Engine init stores the backend on the config; the orchestrator reads it.
        from ray.llm._internal.serve.engines.vllm.kv_transfer.base import (
            DefaultConnectorBackend,
        )

        server._llm_config._kv_connector_backend = DefaultConnectorBackend(
            server._llm_config
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


class _ChooseReplicaPrefillHandle:
    """Fake prefill DeploymentHandle exercising the choose_replica/dispatch path.

    Mirrors the connector-protocol opt-in flow: the orchestrator opens a
    ``choose_replica`` async context manager, reads ``selection.replica_metadata``,
    then calls ``dispatch(selection, request, raw_info)``.
    """

    def __init__(self, calls=None, replica_metadata=None):
        self.calls = calls if calls is not None else []
        self._replica_metadata = (
            replica_metadata if replica_metadata is not None else {"peer": "prefill-7"}
        )

    def options(self, **kwargs):
        return self

    def _method(self, name):
        handle = self

        class _Selection:
            replica_metadata = handle._replica_metadata

        class _Ctx:
            async def __aenter__(self_inner):
                return _Selection()

            async def __aexit__(self_inner, *exc):
                return False

        def choose_replica(request):
            handle.calls.append({"phase": "choose_replica", "method": name})
            return _Ctx()

        def dispatch(selection, request, raw_request_info):
            handle.calls.append(
                {"phase": "dispatch", "method": name, "request": request}
            )
            return _aiter(
                [SimpleNamespace(kv_transfer_params={"remote_engine_id": "prefill-7"})]
            )

        return SimpleNamespace(choose_replica=choose_replica, dispatch=dispatch)

    @property
    def chat(self):
        return self._method("chat")

    @property
    def completions(self):
        return self._method("completions")


class TestConnectorProtocolHook:
    """The orchestrator delegates request shaping + handoff to the backend."""

    def test_base_connector_backend_is_abstract(self):
        """``BaseConnectorBackend`` is abstract and cannot be instantiated:
        ``prepare_prefill_request`` / ``prepare_decode_request`` are abstract."""
        from ray.llm._internal.serve.engines.vllm.kv_transfer.base import (
            BaseConnectorBackend,
        )

        with pytest.raises(TypeError):
            BaseConnectorBackend(llm_config=None)

    def test_default_protocol_mixin_shaping(self):
        """The ``DefaultPDProtocolMixin`` policy: prefill stamps the standard
        kv_transfer_params + clamps to one non-streaming token; decode forwards
        the prefill response's kv_transfer_params, and tolerates a None prefill
        response (concurrent-handoff mode) without crashing."""
        from ray.llm._internal.serve.engines.vllm.kv_transfer.base import (
            DefaultConnectorBackend,
        )

        request = ChatCompletionRequest(
            model="test-model",
            messages=[{"role": "user", "content": "hello"}],
            max_completion_tokens=16,
            stream=True,
            stream_options={"include_usage": True},
        )

        be = DefaultConnectorBackend(llm_config=None)

        prefill = be.prepare_prefill_request(
            request=request.model_copy(deep=True), peer=None
        )
        assert prefill.kv_transfer_params["do_remote_decode"] is True
        assert prefill.kv_transfer_params["do_remote_prefill"] is False
        assert prefill.max_tokens == 1
        assert prefill.max_completion_tokens == 1
        assert prefill.stream is False
        assert prefill.stream_options is None
        # Original untouched.
        assert request.max_completion_tokens == 16
        assert request.stream is True

        chunk = SimpleNamespace(kv_transfer_params={"remote_engine_id": "p1"})
        decode = be.prepare_decode_request(
            request=request.model_copy(deep=True), peer=None, prefill_response=chunk
        )
        assert decode.kv_transfer_params == {"remote_engine_id": "p1"}

        # None prefill_response (concurrent mode) must not crash and leaves
        # kv_transfer_params unset.
        decode_none = be.prepare_decode_request(
            request=request.model_copy(deep=True), peer=None, prefill_response=None
        )
        assert getattr(decode_none, "kv_transfer_params", None) is None

    def test_get_connector_backend_returns_stored_backend(self):
        """``_get_connector_backend`` returns the backend that engine init stored
        on the LLMConfig (and caches it); asserts if none was stored."""
        from ray.llm._internal.serve.engines.vllm.kv_transfer.base import (
            BaseConnectorBackend,
        )
        from ray.llm._internal.serve.engines.vllm.kv_transfer.nixl import (
            NixlConnectorBackend,
        )

        server = PDDecodeServer.__new__(PDDecodeServer)
        server._llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test-model"),
            engine_kwargs={
                "kv_transfer_config": {
                    "kv_connector": "NixlConnector",
                    "kv_role": "kv_both",
                }
            },
        )

        # No backend stored (engine init / setup_engine_backend didn't run) -> assert.
        with pytest.raises(AssertionError):
            server._get_connector_backend()

        # The backend setup_engine_backend would store is returned.
        stored = NixlConnectorBackend(llm_config=server._llm_config)
        assert isinstance(stored, BaseConnectorBackend)
        server._llm_config._kv_connector_backend = stored
        assert server._get_connector_backend() is stored

        # Cached on first access: a later config change isn't re-read.
        server._llm_config._kv_connector_backend = None
        assert server._get_connector_backend() is stored

    @pytest.mark.asyncio
    async def test_peer_binding_concurrent_handoff_takes_choose_replica_path(self):
        """A backend opting into requires_peer_binding + concurrent_handoff must
        drive the orchestrator down the choose_replica/dispatch + concurrent
        local-decode path, calling the backend's prepare_* with the peer."""
        from ray.llm._internal.serve.engines.vllm.kv_transfer.base import (
            BaseConnectorBackend,
        )

        seen = {}

        class _DummyBackend(BaseConnectorBackend):
            requires_peer_binding = True
            concurrent_handoff = True

            def prepare_prefill_request(self, *, request, peer):
                seen["prefill_peer"] = peer
                out = request.model_copy(deep=True)
                out.kv_transfer_params = {"role": "prefill", "peer": peer}
                return out

            def prepare_decode_request(self, *, request, peer, prefill_response):
                seen["decode_peer"] = peer
                seen["prefill_response"] = prefill_response
                out = request.model_copy(deep=True)
                out.kv_transfer_params = {"role": "decode", "peer": peer}
                return out

        server = PDDecodeServer.__new__(PDDecodeServer)
        server._llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test-model")
        )
        dummy_backend = _DummyBackend(server._llm_config)
        server._llm_config._kv_connector_backend = dummy_backend
        prefill = _ChooseReplicaPrefillHandle(replica_metadata={"peer": "prefill-7"})
        server._prefill_handle = prefill

        decode_calls = []

        async def _fake_super_completions(self, req, raw_info):
            decode_calls.append(req)
            return _aiter(["decode-chunk"])

        request = CompletionRequest(model="test-model", prompt="hi")

        # Patch the super() local-decode target (LLMServer.completions in the MRO).
        with patch.object(LLMServer, "completions", _fake_super_completions):
            chunks = [c async for c in server._pd_handle_request(request, None)]

        # choose_replica + dispatch were used (not .remote()).
        phases = [c["phase"] for c in prefill.calls]
        assert phases == ["choose_replica", "dispatch"], phases
        # Backend saw the peer metadata from the selection on both prepares.
        assert seen["prefill_peer"] == {"peer": "prefill-7"}
        assert seen["decode_peer"] == {"peer": "prefill-7"}
        # Concurrent handoff -> no prefill chunk captured before decode.
        assert seen["prefill_response"] is None
        # Local decode ran with the backend-shaped decode request.
        assert decode_calls[0].kv_transfer_params == {
            "role": "decode",
            "peer": {"peer": "prefill-7"},
        }
        assert chunks == ["decode-chunk"]

    @pytest.mark.asyncio
    async def test_default_nixl_backend_shapes_prefill_and_forwards_decode(self):
        """End-to-end (mock handle) default path through a resolved NIXL backend:
        prefill is shaped (max_tokens=1, do_remote_decode), and decode forwards
        the prefill chunk's kv_transfer_params."""
        server = PDDecodeServer.__new__(PDDecodeServer)
        server._llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test-model"),
            engine_kwargs={
                "kv_transfer_config": {
                    "kv_connector": "NixlConnector",
                    "kv_role": "kv_both",
                }
            },
        )
        # Engine init (setup_engine_backend) stores the backend on the config;
        # the orchestrator reads it from there.
        from ray.llm._internal.serve.engines.vllm.kv_transfer.nixl import (
            NixlConnectorBackend,
        )

        server._llm_config._kv_connector_backend = NixlConnectorBackend(
            server._llm_config
        )
        prefill = _FakePrefillHandle()
        server._prefill_handle = prefill

        decode_calls = []

        async def _fake_super_completions(self, req, raw_info):
            decode_calls.append(req)
            return _aiter(["decode-chunk"])

        request = CompletionRequest(model="test-model", prompt="hi")

        with patch.object(LLMServer, "completions", _fake_super_completions):
            chunks = [c async for c in server._pd_handle_request(request, None)]

        # Standard (non choose_replica) path: .remote() was used.
        sent_prefill = prefill.calls[0]["request"]
        assert sent_prefill.max_tokens == 1
        assert sent_prefill.kv_transfer_params["do_remote_decode"] is True
        # Decode forwarded prefill's kv_transfer_params.
        assert decode_calls[0].kv_transfer_params == {"remote_engine_id": "prefill-1"}
        assert chunks == ["decode-chunk"]

    @pytest.mark.asyncio
    async def test_concurrent_handoff_cancels_prefill_on_decode_failure(self):
        """In concurrent-handoff mode, if local decode raises, the background
        prefill task must be cancelled (no leak)."""
        from ray.llm._internal.serve.engines.vllm.kv_transfer.base import (
            BaseConnectorBackend,
        )

        prefill_started = asyncio.Event()
        prefill_cancelled = {"value": False}

        class _SlowPrefillHandle:
            def __init__(self):
                self.calls = []

            def options(self, **kwargs):
                return self

            def _method(self, name):
                async def _gen():
                    prefill_started.set()
                    try:
                        await asyncio.sleep(100)
                        yield SimpleNamespace(kv_transfer_params={})
                    except asyncio.CancelledError:
                        prefill_cancelled["value"] = True
                        raise

                def remote(request, raw_request_info):
                    self.calls.append({"method": name})
                    return _gen()

                return SimpleNamespace(remote=remote)

            @property
            def completions(self):
                return self._method("completions")

        class _ConcurrentBackend(BaseConnectorBackend):
            requires_peer_binding = False
            concurrent_handoff = True

            def prepare_prefill_request(self, *, request, peer):
                out = request.model_copy(deep=True)
                out.kv_transfer_params = {"do_remote_decode": True}
                return out

            def prepare_decode_request(self, *, request, peer, prefill_response):
                return request.model_copy(deep=True)

        server = PDDecodeServer.__new__(PDDecodeServer)
        server._llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test-model")
        )
        server._llm_config._kv_connector_backend = _ConcurrentBackend(
            server._llm_config
        )
        server._prefill_handle = _SlowPrefillHandle()

        async def _failing_super_completions(self, req, raw_info):
            await prefill_started.wait()

            async def _gen():
                raise RuntimeError("decode boom")
                yield  # pragma: no cover

            return _gen()

        request = CompletionRequest(model="test-model", prompt="hi")

        with patch.object(LLMServer, "completions", _failing_super_completions):
            with pytest.raises(RuntimeError, match="decode boom"):
                async for _ in server._pd_handle_request(request, None):
                    pass

        assert prefill_cancelled["value"] is True


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
