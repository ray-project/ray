import asyncio
import json
import sys
import threading
from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest
from starlette.datastructures import Headers

from ray.llm._internal.serve.constants import get_llm_serve_runtime_env
from ray.llm._internal.serve.core.ingress import applications as applications_module
from ray.llm._internal.serve.core.ingress.applications import RouterApplication
from ray.serve._private.constants import (
    SERVE_ROUTER_APPLICATION_DIRECT_RESPONSE_HEADER,
    SERVE_SESSION_ID,
)
from ray.serve.exceptions import DeploymentUnavailableError, RayServeException

MODEL_APPS = {
    "model-a": "llm-model-a",
    "org/model-b": "llm-model-b",
}


class _FakeRequest:
    def __init__(self, body: bytes = b"", headers: Optional[dict] = None):
        self._body = body
        self.headers = Headers(headers or {})

    async def body(self) -> bytes:
        return self._body


class FakeHandle:
    """An application handle whose request router picks `replica_id`."""

    def __init__(self, app_name: str):
        self.app_name = app_name
        self.replica_id = f"SERVE_REPLICA::{app_name}#Ingress#r1"
        self.initialized = False
        self.session_id = None
        self.calls: List[Dict] = []
        self.error: Optional[Exception] = None
        # Raised by the pick-only (`_reserve=False`) path only.
        self.pick_only_error: Optional[Exception] = None
        self.delay_s = 0.0
        self.pick_only_delay_s = 0.0

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
        if kwargs.get("_reserve") is False and self.pick_only_delay_s:
            await asyncio.sleep(self.pick_only_delay_s)
        if kwargs.get("_reserve") is False and self.pick_only_error is not None:
            raise self.pick_only_error
        if self.error is not None:
            raise self.error
        replica_id = MagicMock()
        replica_id.to_full_id_str.return_value = self.replica_id
        yield MagicMock(_replica=SimpleNamespace(replica_id=replica_id))


def _new(cls, *args):
    """Run a `serve.ingress` class's user constructor."""
    obj = cls.__new__(cls)
    cls.__bases__[0].__init__(obj, *args)
    return obj


def _new_router(model_apps=MODEL_APPS, missing_apps=None):
    handles = {}
    missing_apps = set(missing_apps or [])

    def get_app_handle(app_name):
        if app_name in missing_apps:
            raise RayServeException(f"Application '{app_name}' does not exist.")
        handles.setdefault(app_name, FakeHandle(app_name))
        return handles[app_name]

    patcher = patch.object(applications_module.serve, "get_app_handle", get_app_handle)
    patcher.start()
    router = _new(RouterApplication, model_apps)
    return router, handles, patcher


def _body(response) -> Dict:
    return json.loads(response.body)


def _assert_direct(response):
    assert response.headers[SERVE_ROUTER_APPLICATION_DIRECT_RESPONSE_HEADER] == "1"


async def _chat(router, body, headers=None):
    raw = body if isinstance(body, bytes) else json.dumps(body).encode()
    return await router.chat(_FakeRequest(raw, headers))


class TestModels:
    @pytest.mark.asyncio
    async def test_models_are_static_direct_response(self):
        router, handles, patcher = _new_router()
        try:
            response = await router.models()

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
    @pytest.mark.parametrize("path", ["org/model-b", "org--model-b"])
    async def test_model_data(self, path):
        router, handles, patcher = _new_router()
        try:
            response = await router.model_data(path)

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
            response = await router.model_data("nope")

            assert response.status_code == 404
            _assert_direct(response)
            assert _body(response)["error"]["type"] == "InvalidModel"
        finally:
            patcher.stop()


class TestChatDecision:
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
            (call,) = handle.calls
            (payload,) = call["args"]
            assert payload.model == model_id
            assert payload.messages == body["messages"]
            assert call["kwargs"] == {"_reserve": False}
        finally:
            patcher.stop()

    @pytest.mark.asyncio
    async def test_lazily_resolves_and_caches_application_handle(self):
        router, handles, patcher = _new_router()
        try:
            assert handles == {}
            await _chat(router, {"model": "model-a"})
            first_handle = handles["llm-model-a"]
            await _chat(router, {"model": "model-a"})

            assert handles == {"llm-model-a": first_handle}
            assert len(first_handle.calls) == 2
        finally:
            patcher.stop()

    @pytest.mark.asyncio
    async def test_concurrent_requests_share_application_handle_lookup(self):
        lookup_count = 0

        def get_app_handle(app_name):
            nonlocal lookup_count
            lookup_count += 1
            return FakeHandle(app_name)

        async def to_thread(func, *args):
            await asyncio.sleep(0.05)
            return func(*args)

        with patch.object(
            applications_module.asyncio, "to_thread", to_thread
        ), patch.object(applications_module.serve, "get_app_handle", get_app_handle):
            router = _new(RouterApplication, MODEL_APPS)
            decisions = await asyncio.gather(
                _chat(router, {"model": "model-a"}),
                _chat(router, {"model": "model-a"}),
            )

        assert lookup_count == 1
        assert decisions[0]["replica_id"] == decisions[1]["replica_id"]

    @pytest.mark.asyncio
    async def test_handle_lookup_does_not_block_control_requests(self):
        lookup_started = threading.Event()
        release_lookup = threading.Event()

        def get_app_handle(app_name):
            lookup_started.set()
            assert release_lookup.wait(timeout=5)
            return FakeHandle(app_name)

        with patch.object(applications_module.serve, "get_app_handle", get_app_handle):
            router = _new(RouterApplication, MODEL_APPS)
            chat_task = asyncio.create_task(_chat(router, {"model": "model-a"}))
            assert await asyncio.to_thread(lookup_started.wait, 1)

            response = await asyncio.wait_for(router.models(), timeout=0.1)
            assert response.status_code == 200

            release_lookup.set()
            await chat_task

    @pytest.mark.asyncio
    async def test_handle_lookup_is_included_in_decision_timeout(self, monkeypatch):
        monkeypatch.setattr(applications_module, "CHOOSE_REPLICA_TIMEOUT_S", 0.05)

        async def blocked_to_thread(func, *args):
            await asyncio.sleep(10)

        with patch.object(applications_module.asyncio, "to_thread", blocked_to_thread):
            router = _new(RouterApplication, MODEL_APPS)
            response = await _chat(router, {"model": "model-a"})
            assert response.status_code == 503

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
    async def test_missing_application(self):
        router, _, patcher = _new_router(missing_apps={"llm-model-a"})
        try:
            response = await _chat(router, {"model": "model-a"})
            assert response.status_code == 503
            assert _body(response)["error"]["type"] == "ServiceUnavailableError"
        finally:
            patcher.stop()

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
            assert _body(response)["error"]["type"] == "ServiceUnavailableError"
        finally:
            patcher.stop()

    @pytest.mark.asyncio
    async def test_falls_back_to_reserving_path(self, monkeypatch):
        monkeypatch.setattr(applications_module, "PICK_ONLY_TIMEOUT_S", 0.05)
        router, handles, patcher = _new_router()
        try:
            handle = FakeHandle("llm-model-a")
            handle.pick_only_delay_s = 10
            handles["llm-model-a"] = handle

            decision = await _chat(router, {"model": "model-a"})
            assert decision["replica_id"] == handle.replica_id
            assert [c["kwargs"] for c in handle.calls] == [{"_reserve": False}, {}]
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
        monkeypatch.setattr(applications_module, "CHOOSE_REPLICA_TIMEOUT_S", 0.1)
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
    options = RouterApplication.get_deployment_options()
    assert options == {
        "max_ongoing_requests": applications_module.DEFAULT_MAX_ONGOING_REQUESTS,
        "ray_actor_options": {
            "num_cpus": 1,
            "runtime_env": get_llm_serve_runtime_env(),
        },
        "autoscaling_config": {
            "min_replicas": 1,
            "initial_replicas": 2,
            "max_replicas": 10,
            "target_ongoing_requests": 100,
        },
    }


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
