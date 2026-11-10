import asyncio
import copy
import json
import sys
import threading
from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from starlette.datastructures import Headers

from ray.exceptions import RayActorError, RaySystemError
from ray.llm._internal.serve.constants import get_llm_serve_runtime_env
from ray.llm._internal.serve.core.configs.llm_config import (
    LLMConfig,
    ModelLoadingConfig,
)
from ray.llm._internal.serve.core.ingress import (
    router_application as router_application_module,
)
from ray.llm._internal.serve.core.ingress.router_application import RouterApplication
from ray.llm._internal.serve.routing_policies.kv_aware import kv_token_tracker
from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    KV_TOKEN_KEY_HEADER,
    KV_TOKEN_METADATA_KEY,
    REQUEST_TOKEN_IDS_KWARG,
)
from ray.llm._internal.serve.routing_policies.kv_aware.router_state import KVRouterState
from ray.llm._internal.serve.routing_policies.kv_aware.token_channel import (
    decode_prompt_token_ids,
)
from ray.serve._private.common import DeploymentID
from ray.serve._private.constants import (
    RAY_SERVE_INGRESS_REQUEST_ROUTER_OPT_HEADERS_FIELD,
    SERVE_INGRESS_ROUTER_REQUEST_PATH_HEADER,
    SERVE_ROUTER_APPLICATION_DIRECT_RESPONSE_HEADER,
    SERVE_ROUTER_APPLICATION_ORIGINAL_METHOD_HEADER,
    SERVE_SESSION_ID,
)
from ray.serve.exceptions import DeploymentUnavailableError, RayServeException
from ray.serve.llm.request_router import KVAwareRouter

MODEL_APPS = {
    "model-a": "llm-model-a",
    "org/model-b": "llm-model-b",
}


class _FakeRequest:
    def __init__(
        self,
        body: bytes = b"",
        headers: Optional[dict] = None,
        form: Optional[dict] = None,
    ):
        self._body = body
        self.headers = Headers(headers or {})
        self._form = form or {}

    async def body(self) -> bytes:
        return self._body

    async def form(self) -> dict:
        return self._form


class FakeHandle:
    """An application handle whose request router picks `replica_id`."""

    def __init__(self, app_name: str):
        self.app_name = app_name
        self.deployment_id = DeploymentID("Ingress", app_name=app_name)
        self.replica_id = f"SERVE_REPLICA::{app_name}#Ingress#r1"
        self.initialized = False
        self.session_id = None
        self.calls: List[Dict] = []
        self.error: Optional[Exception] = None
        # Raised by the pick-only (`_reserve=False`) path only.
        self.pick_only_error: Optional[Exception] = None
        self.delay_s = 0.0
        self.routing_stats = {}

    def _init(self):
        self.initialized = True

    @property
    def is_initialized(self):
        return self.initialized

    def options(self, *, session_id):
        configured = FakeHandle(self.app_name)
        configured.__dict__.update(self.__dict__)
        configured.session_id = session_id
        return configured

    @asynccontextmanager
    async def choose_replica(self, *args, **kwargs):
        self.calls.append(
            {"args": args, "kwargs": kwargs, "session_id": self.session_id}
        )
        if self.delay_s:
            await asyncio.sleep(self.delay_s)
        if kwargs.get("_reserve") is False and self.pick_only_error is not None:
            raise self.pick_only_error
        if self.error is not None:
            raise self.error
        replica_id = MagicMock()
        replica_id.to_full_id_str.return_value = self.replica_id
        yield MagicMock(
            _replica=SimpleNamespace(
                replica_id=replica_id, routing_stats=self.routing_stats
            )
        )


@pytest.fixture(autouse=True)
def _no_real_app_handles():
    """Background lookups may outlive a test's own patch; never reach Serve."""
    with patch.object(router_application_module.serve, "get_app_handle", FakeHandle):
        yield


def _new(cls, *args):
    """Run a `serve.ingress` class's user constructor."""
    obj = cls.__new__(cls)
    cls.__bases__[0].__init__(obj, *args)
    return obj


def _llm_configs(model_apps=MODEL_APPS, api_keys=None):
    api_keys = api_keys or {}
    return [
        LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id=model_id),
            engine_kwargs=(
                {"api_key": api_keys[model_id]} if model_id in api_keys else {}
            ),
        )
        for model_id in model_apps
    ]


def _new_router(
    model_apps=MODEL_APPS, missing_apps=None, api_keys=None, llm_configs=None
):
    handles = {}
    missing_apps = set(missing_apps or [])

    def get_app_handle(app_name):
        if app_name in missing_apps:
            raise RayServeException(f"Application '{app_name}' does not exist.")
        handles.setdefault(app_name, FakeHandle(app_name))
        return handles[app_name]

    patcher = patch.object(
        router_application_module.serve, "get_app_handle", get_app_handle
    )
    patcher.start()
    router = _new(
        RouterApplication, model_apps, llm_configs or _llm_configs(model_apps, api_keys)
    )
    return router, handles, patcher


def _cancel_handle_tasks(router):
    for task in router._handles.values():
        task.cancel()


def _body(response) -> Dict:
    return json.loads(response.body)


def test_null_runtime_env_vars(monkeypatch):
    monkeypatch.delenv("VLLM_API_KEY", raising=False)
    config = LLMConfig(
        model_loading_config=ModelLoadingConfig(model_id="model-a"),
        runtime_env={"env_vars": None},
    )

    assert router_application_module._api_key_hashes(config) == ()


def _assert_direct(response):
    assert response.headers[SERVE_ROUTER_APPLICATION_DIRECT_RESPONSE_HEADER] == "1"


async def _route(router, method, path, body=b"", headers=None, form=None):
    headers = {
        **(headers or {}),
        SERVE_INGRESS_ROUTER_REQUEST_PATH_HEADER: path,
        SERVE_ROUTER_APPLICATION_ORIGINAL_METHOD_HEADER: method,
    }
    return await router.route(_FakeRequest(body, headers, form))


async def _chat(router, body, headers=None):
    raw = body if isinstance(body, bytes) else json.dumps(body).encode()
    return await _route(router, "POST", "/v1/chat/completions", raw, headers)


class TestModels:
    @pytest.mark.asyncio
    async def test_cors_preflight_is_direct_response(self):
        router, handles, patcher = _new_router()
        try:
            response = await _route(
                router,
                "OPTIONS",
                "/v1/chat/completions",
                headers={
                    "origin": "https://example.com",
                    "access-control-request-method": "POST",
                    "access-control-request-headers": "authorization,content-type",
                },
            )

            assert response.status_code == 200
            _assert_direct(response)
            assert response.headers["access-control-allow-origin"] == (
                "https://example.com"
            )
            assert response.headers["access-control-allow-credentials"] == "true"
            assert "POST" in response.headers["access-control-allow-methods"]
            assert response.headers["access-control-allow-headers"] == (
                "authorization,content-type"
            )
            assert handles == {}
        finally:
            patcher.stop()

    @pytest.mark.asyncio
    async def test_models_are_static_direct_response(self):
        router, handles, patcher = _new_router()
        try:
            response = await _route(router, "GET", "/v1/models")

            assert response.status_code == 200
            _assert_direct(response)
            assert _body(response) == {
                "object": "list",
                "data": [
                    {
                        "id": model_id,
                        "object": "model",
                        "owned_by": "organization-owner",
                        "permission": [],
                        "metadata": {"model_id": model_id},
                    }
                    for model_id in MODEL_APPS
                ],
            }
            assert handles == {}
        finally:
            patcher.stop()

    @pytest.mark.asyncio
    async def test_models_only_include_authorized_models(self):
        router, handles, patcher = _new_router(api_keys={"model-a": ["key-a"]})
        try:
            response = await _route(router, "GET", "/v1/models")
            assert [model["id"] for model in _body(response)["data"]] == ["org/model-b"]

            response = await _route(
                router,
                "GET",
                "/v1/models",
                headers={"authorization": "Bearer key-a"},
            )
            assert [model["id"] for model in _body(response)["data"]] == list(
                MODEL_APPS
            )
            assert handles == {}
        finally:
            patcher.stop()

    @pytest.mark.asyncio
    async def test_private_model_data_requires_api_key(self):
        router, handles, patcher = _new_router(api_keys={"model-a": ["key-a"]})
        try:
            response = await _route(router, "GET", "/v1/models/model-a")
            assert response.status_code == 401
            assert _body(response) == {"error": "Unauthorized"}

            response = await _route(
                router,
                "GET",
                "/v1/models/model-a",
                headers={"authorization": "Bearer key-a"},
            )
            assert response.status_code == 200
            assert _body(response)["id"] == "model-a"
            assert handles == {}
        finally:
            patcher.stop()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("path", ["org/model-b", "org--model-b"])
    async def test_model_data(self, path):
        router, handles, patcher = _new_router()
        try:
            response = await _route(router, "GET", f"/v1/models/{path}")

            assert response.status_code == 200
            _assert_direct(response)
            assert _body(response)["id"] == "org/model-b"
            assert handles == {}
        finally:
            patcher.stop()

    @pytest.mark.asyncio
    async def test_model_data_unknown(self):
        router, _, patcher = _new_router()
        try:
            response = await _route(router, "GET", "/v1/models/nope")

            assert response.status_code == 404
            _assert_direct(response)
            assert _body(response)["error"]["type"] == "InvalidModel"
        finally:
            patcher.stop()


class TestRoutingDecision:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "path",
        [
            "/v1/chat/completions",
            "/v1/completions",
            "/v1/embeddings",
            "/v1/score",
            "/tokenize",
            "/a/future/vllm/route",
        ],
    )
    async def test_public_routes_are_owned_by_selected_application(self, path):
        router, handles, patcher = _new_router()
        try:
            body = json.dumps({"model": "model-a"}).encode()
            decision = await _route(router, "POST", path, body)

            assert decision["application"] == "llm-model-a"
            assert decision["replica_id"] == handles["llm-model-a"].replica_id
        finally:
            patcher.stop()

    @pytest.mark.asyncio
    async def test_routes_multipart_request_by_model(self):
        router, handles, patcher = _new_router()
        try:
            decision = await _route(
                router,
                "POST",
                "/v1/audio/transcriptions",
                headers={"content-type": "multipart/form-data; boundary=test"},
                form={"model": "org/model-b"},
            )

            assert decision["application"] == "llm-model-b"
            assert decision["replica_id"] == handles["llm-model-b"].replica_id
        finally:
            patcher.stop()

    @pytest.mark.asyncio
    async def test_rejects_truncated_multipart_without_model(self):
        router, handles, patcher = _new_router()
        try:
            response = await _route(
                router,
                "POST",
                "/v1/audio/transcriptions",
                headers={
                    "content-type": "multipart/form-data; boundary=test",
                    "x-body-truncated": "8388608/10000000",
                },
            )

            assert response.status_code == 413
            assert _body(response)["error"]["type"] == "RequestTooLarge"
            assert handles == {}
        finally:
            patcher.stop()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("model_id", MODEL_APPS)
    async def test_returns_application_and_full_replica_id(self, model_id):
        router, handles, patcher = _new_router()
        try:
            body = {"model": model_id, "messages": [{"role": "user"}]}
            decision = await _chat(router, body)

            app_name = MODEL_APPS[model_id]
            handle = handles[app_name]
            assert decision == {
                "application": app_name,
                "replica_id": handle.replica_id,
            }
            assert handle.initialized
            assert router._kv_states == {}
            (call,) = handle.calls
            (payload,) = call["args"]
            assert payload.model == model_id
            assert payload.messages == body["messages"]
            assert call["kwargs"] == {"_reserve": False}
        finally:
            patcher.stop()

    @pytest.mark.asyncio
    async def test_kv_aware_selection_forwards_prompt_tokens(self):
        router, handles, patcher = _new_router()
        try:
            tokenizer = MagicMock()
            tokenizer.tokenize = AsyncMock(return_value=[1, 2, 3])
            state = KVRouterState.__new__(KVRouterState)
            state.tokenizer_task = asyncio.create_task(
                asyncio.sleep(0, result=tokenizer)
            )
            state.token_sender = MagicMock()
            state.token_sender.push.return_value = True
            state._warned_no_token_endpoint = False
            await asyncio.gather(*router._handles.values())
            router._kv_states["model-a"] = state
            handle = handles["llm-model-a"]
            handle.routing_stats = {
                KV_TOKEN_METADATA_KEY: {"endpoint": "tcp://127.0.0.1:1234"}
            }

            decision = await _chat(
                router,
                {
                    "model": "model-a",
                    "messages": [{"role": "user", "content": "hello"}],
                },
            )

            assert handle.calls[-1]["kwargs"] == {
                "_reserve": False,
                REQUEST_TOKEN_IDS_KWARG: [1, 2, 3],
            }
            token_key = decision[RAY_SERVE_INGRESS_REQUEST_ROUTER_OPT_HEADERS_FIELD][
                KV_TOKEN_KEY_HEADER
            ]
            state.token_sender.push.assert_called_once()
            assert state.token_sender.push.call_args.args[:2] == (
                "tcp://127.0.0.1:1234",
                token_key,
            )
        finally:
            patcher.stop()

    @pytest.mark.asyncio
    async def test_kv_models_have_independent_state(self, monkeypatch):
        from ray.llm._internal.serve.routing_policies.kv_aware import token_channel
        from ray.llm._internal.serve.routing_policies.kv_aware.vllm import tokenizer

        model_apps = {"model-a": "app-a", "model-b": "app-b", "model-c": "app-c"}
        configs = _llm_configs(model_apps)
        for config in configs[:2]:
            config.deployment_config = {
                "request_router_config": {"request_router_class": KVAwareRouter}
            }
        token_ids = {"model-a": [1, 2], "model-b": [3, 4]}
        trackers = {}

        def build_tracker(**kwargs):
            deployment_id = kwargs["serve_deployment_id"]
            tracker = MagicMock(_serve_deployment_id=deployment_id)
            tracker.on_lifecycle_events = AsyncMock()
            tracker.on_replica_updates = AsyncMock()
            trackers[deployment_id] = tracker
            return tracker

        def initialize_handle(handle):
            if handle.app_name != "app-c":
                assert (
                    kv_token_tracker.get_kv_token_tracker(handle.deployment_id)
                    is trackers[handle.deployment_id]
                )
            handle.initialized = True

        monkeypatch.setattr(kv_token_tracker, "KVTokenTracker", build_tracker)
        monkeypatch.setattr(FakeHandle, "_init", initialize_handle)
        monkeypatch.setattr(
            tokenizer,
            "Tokenizer",
            lambda config: MagicMock(
                tokenize=AsyncMock(return_value=token_ids[config.model_id])
            ),
        )
        monkeypatch.setattr(token_channel, "TokenSender", MagicMock)
        monkeypatch.setattr(
            router_application_module.serve,
            "get_replica_context",
            lambda: SimpleNamespace(deployment="RouterApplication", app_name="router"),
        )
        monkeypatch.setattr(
            router_application_module.serve, "get_deployment_handle", MagicMock()
        )
        kv_token_tracker.set_kv_token_tracker(None)
        router, handles, patcher = _new_router(model_apps, llm_configs=configs)
        try:
            await asyncio.gather(*router._handles.values())
            assert set(router._kv_states) == {"model-a", "model-b"}
            for model_id, ids in token_ids.items():
                handle = handles[model_apps[model_id]]
                handle.routing_stats = {
                    KV_TOKEN_METADATA_KEY: {"endpoint": f"tcp://{model_id}:1234"}
                }
                decision = await _chat(
                    router,
                    {
                        "model": model_id,
                        "messages": [{"role": "user", "content": "hello"}],
                    },
                )
                assert handle.calls[-1]["kwargs"][REQUEST_TOKEN_IDS_KWARG] == ids
                state = router._kv_states[model_id]
                endpoint, key, payload = state.token_sender.push.call_args.args
                assert endpoint == f"tcp://{model_id}:1234"
                assert decode_prompt_token_ids(payload) == ids
                assert (
                    decision[RAY_SERVE_INGRESS_REQUEST_ROUTER_OPT_HEADERS_FIELD][
                        KV_TOKEN_KEY_HEADER
                    ]
                    == key
                )
                state.tracker.start_replica_broadcast.assert_called_once_with(
                    router._self_handle, model_id=model_id
                )
                state.tracker.subscribe_token_endpoints.assert_called_once_with(
                    state.token_sender.connect
                )

            await router.on_model_lifecycle_events("model-a", ["completion"])
            await router.on_model_replica_updates("model-b", 7, ["reservation"])
            first, second = (
                router._kv_states[model_id].tracker for model_id in token_ids
            )
            first.on_lifecycle_events.assert_awaited_once_with(["completion"])
            second.on_lifecycle_events.assert_not_awaited()
            first.on_replica_updates.assert_not_awaited()
            second.on_replica_updates.assert_awaited_once_with(7, ["reservation"])

            await _chat(router, {"model": "model-c"})
            assert handles["app-c"].calls[-1]["kwargs"] == {"_reserve": False}
            await router.on_model_lifecycle_events("model-c", [])
        finally:
            patcher.stop()
            kv_token_tracker.set_kv_token_tracker(None)

    @pytest.mark.asyncio
    async def test_resolves_handles_at_startup_and_reuses_them(self):
        router, handles, patcher = _new_router()
        try:
            await asyncio.gather(*router._handles.values())
            assert set(handles) == set(MODEL_APPS.values())
            assert all(handle.initialized for handle in handles.values())

            first_handle = handles["llm-model-a"]
            await _chat(router, {"model": "model-a"})
            await _chat(router, {"model": "model-a"})

            assert handles["llm-model-a"] is first_handle
            assert len(first_handle.calls) == 2
        finally:
            patcher.stop()

    @pytest.mark.asyncio
    async def test_early_requests_share_startup_lookup(self):
        lookups: Dict[str, int] = {}

        def get_app_handle(app_name):
            lookups[app_name] = lookups.get(app_name, 0) + 1
            return FakeHandle(app_name)

        async def to_thread(func, *args):
            await asyncio.sleep(0.05)
            return func(*args)

        with patch.object(
            router_application_module.asyncio, "to_thread", to_thread
        ), patch.object(
            router_application_module.serve, "get_app_handle", get_app_handle
        ):
            router = _new(RouterApplication, MODEL_APPS, _llm_configs())
            decisions = await asyncio.gather(
                _chat(router, {"model": "model-a"}),
                _chat(router, {"model": "model-a"}),
            )

        assert lookups == dict.fromkeys(MODEL_APPS.values(), 1)
        assert decisions[0]["replica_id"] == decisions[1]["replica_id"]

    @pytest.mark.asyncio
    async def test_handle_lookup_does_not_block_control_requests(self):
        lookup_started = threading.Event()
        release_lookup = threading.Event()

        def get_app_handle(app_name):
            lookup_started.set()
            assert release_lookup.wait(timeout=5)
            return FakeHandle(app_name)

        with patch.object(
            router_application_module.serve, "get_app_handle", get_app_handle
        ):
            router = _new(RouterApplication, MODEL_APPS, _llm_configs())
            chat_task = asyncio.create_task(_chat(router, {"model": "model-a"}))
            assert await asyncio.to_thread(lookup_started.wait, 1)

            response = await asyncio.wait_for(
                _route(router, "GET", "/v1/models"), timeout=0.1
            )
            assert response.status_code == 200

            release_lookup.set()
            await chat_task

    @pytest.mark.asyncio
    async def test_handle_lookup_is_included_in_decision_timeout(self, monkeypatch):
        monkeypatch.setattr(router_application_module, "CHOOSE_REPLICA_TIMEOUT_S", 0.05)

        async def blocked_to_thread(func, *args):
            await asyncio.sleep(10)

        with patch.object(
            router_application_module.asyncio, "to_thread", blocked_to_thread
        ):
            router = _new(RouterApplication, MODEL_APPS, _llm_configs())
            try:
                response = await _chat(router, {"model": "model-a"})
                assert response.status_code == 503
            finally:
                _cancel_handle_tasks(router)

    @pytest.mark.asyncio
    async def test_body_without_routing_field_load_balances(self):
        router, handles, patcher = _new_router()
        try:
            await _chat(router, {"model": "model-a"})
            assert handles["llm-model-a"].calls[0]["args"] == ()
        finally:
            patcher.stop()

    @pytest.mark.asyncio
    async def test_applies_session_id(self):
        router, handles, patcher = _new_router()
        try:
            await _chat(
                router,
                {"model": "model-a"},
                headers={SERVE_SESSION_ID: "s-1"},
            )
            assert handles["llm-model-a"].calls[0]["session_id"] == "s-1"
        finally:
            patcher.stop()

    @pytest.mark.asyncio
    async def test_single_model_defaults_missing_model(self):
        router, _, patcher = _new_router({"model-a": "llm-model-a"})
        try:
            decision = await _chat(router, {"messages": []})
            assert decision["application"] == "llm-model-a"
        finally:
            patcher.stop()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "body, headers, status_code, error_type",
        [
            (b"{not json", None, 400, "BadRequestError"),
            (b"", None, 400, "BadRequestError"),
            (
                b'{"model": "model-a", "messa',
                {"x-body-truncated": "27/400000"},
                413,
                "RequestTooLarge",
            ),
            (b"[1, 2]", None, 400, "BadRequestError"),
            ({"messages": []}, None, 400, "BadRequestError"),
            ({"model": 1}, None, 400, "BadRequestError"),
            ({"model": ["model-a"]}, None, 400, "BadRequestError"),
            ({"model": "model-c"}, None, 404, "NotFoundError"),
        ],
    )
    async def test_errors(self, body, headers, status_code, error_type):
        router, handles, patcher = _new_router()
        try:
            response = await _chat(router, body, headers=headers)

            assert response.status_code == status_code
            _assert_direct(response)
            error = _body(response)["error"]
            assert error["type"] == error_type
            assert error["code"] == status_code
            assert handles == {}
        finally:
            patcher.stop()

    @pytest.mark.asyncio
    async def test_missing_application(self, monkeypatch):
        monkeypatch.setattr(router_application_module, "CHOOSE_REPLICA_TIMEOUT_S", 0.1)
        monkeypatch.setattr(router_application_module, "HANDLE_RETRY_INITIAL_S", 0.01)
        router, _, patcher = _new_router(missing_apps={"llm-model-a"})
        try:
            response = await _chat(router, {"model": "model-a"})
            assert response.status_code == 503
            assert response.headers["retry-after"] == "1"
            error = _body(response)["error"]
            assert error["type"] == "ServiceUnavailableError"
            assert "'model-a' (application 'llm-model-a')" in error["message"]
            # The lookup keeps retrying after the request times out.
            assert not router._handles["model-a"].done()
            # Other models are unaffected.
            assert "replica_id" in await _chat(router, {"model": "org/model-b"})
        finally:
            _cancel_handle_tasks(router)
            patcher.stop()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "error",
        [
            RayServeException("Application does not exist."),
            RayActorError(),
            RaySystemError("controller unavailable"),
        ],
    )
    async def test_application_deployed_after_router(self, monkeypatch, error):
        monkeypatch.setattr(router_application_module, "HANDLE_RETRY_INITIAL_S", 0.01)
        deployed = False

        def get_app_handle(app_name):
            if not deployed:
                raise error
            return FakeHandle(app_name)

        with patch.object(
            router_application_module.serve, "get_app_handle", get_app_handle
        ):
            router = _new(RouterApplication, MODEL_APPS, _llm_configs())
            try:
                await asyncio.sleep(0.05)
                deployed = True
                decision = await _chat(router, {"model": "model-a"})
                assert decision["application"] == "llm-model-a"
            finally:
                _cancel_handle_tasks(router)

    @pytest.mark.asyncio
    async def test_unexpected_lookup_error_is_not_retried(self, monkeypatch):
        monkeypatch.setattr(router_application_module, "HANDLE_RETRY_INITIAL_S", 0.01)
        lookups = 0

        def get_app_handle(app_name):
            nonlocal lookups
            lookups += 1
            raise TypeError("bug")

        with patch.object(
            router_application_module.serve, "get_app_handle", get_app_handle
        ):
            model_apps = {"model-a": "llm-model-a"}
            router = _new(RouterApplication, model_apps, _llm_configs(model_apps))
            with pytest.raises(TypeError, match="bug"):
                await _chat(router, {"model": "model-a"})
            await asyncio.sleep(0.05)
            assert lookups == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "error", [DeploymentUnavailableError(MagicMock()), RuntimeError("down")]
    )
    async def test_unavailable_model(self, error):
        router, handles, patcher = _new_router()
        try:
            handle = FakeHandle("llm-model-a")
            handle.error = error
            handles["llm-model-a"] = handle

            response = await _chat(router, {"model": "model-a"})
            assert response.status_code == 503
            assert response.headers["retry-after"] == "1"
            assert _body(response)["error"]["type"] == "ServiceUnavailableError"
        finally:
            patcher.stop()

    @pytest.mark.asyncio
    async def test_invalid_routing_payload(self):
        router, handles, patcher = _new_router()
        try:
            handle = FakeHandle("llm-model-a")
            handle.error = ValueError("messages must not be empty")
            handles["llm-model-a"] = handle

            response = await _chat(router, {"model": "model-a", "messages": []})

            assert response.status_code == 400
            error = _body(response)["error"]
            assert error["message"] == "messages must not be empty"
            assert error["type"] == "BadRequestError"
        finally:
            patcher.stop()

    @pytest.mark.asyncio
    async def test_pick_only_error_is_not_retried(self):
        router, handles, patcher = _new_router()
        try:
            handle = FakeHandle("llm-model-a")
            handle.pick_only_error = RuntimeError("down")
            handles["llm-model-a"] = handle

            response = await _chat(router, {"model": "model-a"})
            assert response.status_code == 503
            assert len(handle.calls) == 1
        finally:
            patcher.stop()

    @pytest.mark.asyncio
    async def test_no_ready_replica_times_out(self, monkeypatch):
        monkeypatch.setattr(router_application_module, "CHOOSE_REPLICA_TIMEOUT_S", 0.1)
        router, handles, patcher = _new_router()
        try:
            handle = FakeHandle("llm-model-a")
            handle.delay_s = 10
            handles["llm-model-a"] = handle

            response = await _chat(router, {"model": "model-a"})
            assert response.status_code == 503
        finally:
            patcher.stop()


def test_deployment_options_keep_router_available():
    expected_options = copy.deepcopy(router_application_module.DEFAULT_INGRESS_OPTIONS)
    expected_options["ray_actor_options"]["runtime_env"] = get_llm_serve_runtime_env()

    assert RouterApplication.get_deployment_options() == expected_options


@pytest.fixture
def kv_aware_configs():
    configs = _llm_configs()
    for config in configs:
        config.deployment_config = {
            "request_router_config": {"request_router_class": KVAwareRouter}
        }
    return configs


@pytest.mark.parametrize("runtime_env", [None, {}, {"env_vars": None}])
def test_deployment_options_allow_null_runtime_env_vars(kv_aware_configs, runtime_env):
    config = kv_aware_configs[0]
    config.runtime_env = runtime_env

    options = RouterApplication.get_deployment_options([config])

    assert options["ray_actor_options"]["runtime_env"] == get_llm_serve_runtime_env()


@pytest.mark.parametrize(
    "model_env_vars, expected_env_vars",
    [
        ([{"CACHE_A": "a"}, {"CACHE_B": "b"}], {"CACHE_A": "a", "CACHE_B": "b"}),
        ([{"SHARED": "same"}, {"SHARED": "same"}], {"SHARED": "same"}),
        (
            [
                {"VLLM_API_KEY": "model-a-key", "CACHE_A": "a"},
                {"VLLM_API_KEY": "model-b-key", "CACHE_B": "b"},
            ],
            {"CACHE_A": "a", "CACHE_B": "b"},
        ),
    ],
)
def test_deployment_options_merge_kv_runtime_env_vars(
    kv_aware_configs, model_env_vars, expected_env_vars
):
    configs = kv_aware_configs
    for config, env_vars in zip(configs, model_env_vars):
        config.runtime_env = {"env_vars": env_vars}
    original_runtime_envs = copy.deepcopy([config.runtime_env for config in configs])

    options = RouterApplication.get_deployment_options(configs)

    assert options["ray_actor_options"]["runtime_env"] == get_llm_serve_runtime_env(
        {"env_vars": expected_env_vars}
    )
    assert [config.runtime_env for config in configs] == original_runtime_envs


def test_deployment_options_reject_conflicting_kv_runtime_env_vars(kv_aware_configs):
    configs = kv_aware_configs
    configs[0].runtime_env = {"env_vars": {"SHARED": "a"}}
    configs[1].runtime_env = {"env_vars": {"SHARED": "b"}}

    with pytest.raises(ValueError, match="must agree on runtime env var SHARED"):
        RouterApplication.get_deployment_options(configs)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
