import asyncio
import json
import sys
from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest
from starlette.datastructures import Headers

from ray.llm._internal.serve.core.ingress import applications as applications_module
from ray.llm._internal.serve.core.ingress.applications import (
    ApplicationDescriptor,
    ControlApplication,
    ModelApplication,
    RouterApplication,
)
from ray.serve._private.constants import SERVE_SESSION_ID
from ray.serve.exceptions import DeploymentUnavailableError

MODEL_APPS = [
    ModelApplication("llm-model-model-a-1", "LLMServer:model-a", "model-a"),
    ModelApplication(
        "llm-model-org--model-b-2", "LLMServer:org--model-b", "org/model-b"
    ),
]
CONTROL_APP = ApplicationDescriptor("llm-control", "ControlApplication")
MODEL_CARDS = {
    m.model_id: {"id": m.model_id, "object": "model", "owned_by": "org"}
    for m in MODEL_APPS
}


class _FakeRequest:
    def __init__(self, body: bytes = b"", headers: Optional[dict] = None):
        self._body = body
        self.headers = Headers(headers or {})

    async def body(self) -> bytes:
        return self._body


class FakeHandle:
    """An ingress handle whose request router picks `replica_id`."""

    def __init__(self, deployment_name: str, app_name: str):
        self.deployment_name = deployment_name
        self.app_name = app_name
        self.replica_id = f"SERVE_REPLICA::{app_name}#{deployment_name}#r1"
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

    def options(self, *, session_id):
        configured = FakeHandle(self.deployment_name, self.app_name)
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


def _new_router(model_apps=MODEL_APPS):
    handles = {}

    def get_deployment_handle(name, app_name, _check_exists):
        # Siblings may not be registered yet, so existence is not checked.
        assert _check_exists is False
        handles[app_name] = FakeHandle(name, app_name)
        return handles[app_name]

    with patch.object(
        applications_module.serve, "get_deployment_handle", get_deployment_handle
    ):
        router = _new(RouterApplication, model_apps, CONTROL_APP)
    return router, handles


def _body(response) -> Dict:
    return json.loads(response.body)


async def _chat(router, body, headers=None):
    raw = body if isinstance(body, bytes) else json.dumps(body).encode()
    return await router.chat(_FakeRequest(raw, headers))


class TestControlApplication:
    @pytest.mark.asyncio
    async def test_models(self):
        control = _new(ControlApplication, MODEL_CARDS)

        assert await control.models() == {
            "object": "list",
            "data": list(MODEL_CARDS.values()),
        }

    @pytest.mark.asyncio
    @pytest.mark.parametrize("path", ["org/model-b", "org--model-b"])
    async def test_model_data(self, path):
        control = _new(ControlApplication, MODEL_CARDS)

        assert await control.model_data(path) == MODEL_CARDS["org/model-b"]

    @pytest.mark.asyncio
    async def test_model_data_unknown(self):
        control = _new(ControlApplication, MODEL_CARDS)

        response = await control.model_data("nope")

        assert response.status_code == 404
        assert _body(response)["error"]["type"] == "InvalidModel"


class TestRouterInit:
    def test_resolves_one_initialized_handle_per_application(self):
        _, handles = _new_router()

        assert {
            app: (h.deployment_name, h.initialized) for app, h in handles.items()
        } == {
            "llm-model-model-a-1": ("LLMServer:model-a", True),
            "llm-model-org--model-b-2": ("LLMServer:org--model-b", True),
            "llm-control": ("ControlApplication", True),
        }


class TestChatDecision:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("model_app", MODEL_APPS, ids=lambda m: m.model_id)
    async def test_returns_application_and_full_replica_id(self, model_app):
        router, handles = _new_router()
        handle = handles[model_app.application_name]

        body = {"model": model_app.model_id, "messages": [{"role": "user"}]}
        decision = await _chat(router, body)

        assert decision == {
            "application": model_app.application_name,
            "replica_id": handle.replica_id,
        }
        (call,) = handle.calls
        # The parsed body reaches the request router like the OpenAI ingress's.
        (payload,) = call["args"]
        assert payload.model == model_app.model_id
        assert payload.messages == body["messages"]
        # HAProxy sends the request, so no replica slot is reserved.
        assert call["kwargs"] == {"_reserve": False}
        assert not handles["llm-control"].calls

    @pytest.mark.asyncio
    async def test_body_without_routing_field_load_balances(self):
        router, handles = _new_router()

        await _chat(router, {"model": "model-a"})

        assert handles["llm-model-model-a-1"].calls[0]["args"] == ()

    @pytest.mark.asyncio
    async def test_applies_session_id(self):
        router, handles = _new_router()

        await _chat(router, {"model": "model-a"}, headers={SERVE_SESSION_ID: "s-1"})

        assert handles["llm-model-model-a-1"].calls[0]["session_id"] == "s-1"

    @pytest.mark.asyncio
    async def test_single_model_defaults_missing_model(self):
        router, _ = _new_router(model_apps=MODEL_APPS[:1])

        decision = await _chat(router, {"messages": []})

        assert decision["application"] == "llm-model-model-a-1"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "body, headers, status, error_type",
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
    async def test_errors(self, body, headers, status, error_type):
        router, handles = _new_router()

        response = await _chat(router, body, headers=headers)

        assert response.status_code == status
        error = _body(response)["error"]
        assert error["type"] == error_type
        assert error["code"] == status
        assert all(not h.calls for h in handles.values())

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "error", [DeploymentUnavailableError(MagicMock()), RuntimeError("down")]
    )
    async def test_unavailable_model(self, error):
        router, handles = _new_router()
        handles["llm-model-model-a-1"].error = error

        response = await _chat(router, {"model": "model-a"})

        assert response.status_code == 503
        assert _body(response)["error"]["type"] == "ServiceUnavailableError"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("pick_only", ["error", "slow"])
    async def test_falls_back_to_reserving_path(self, monkeypatch, pick_only):
        """Without a quick pick-only result, reserve, which registers demand."""
        monkeypatch.setattr(applications_module, "PICK_ONLY_TIMEOUT_S", 0.05)
        router, handles = _new_router()
        handle = handles["llm-model-model-a-1"]
        if pick_only == "error":
            handle.pick_only_error = RuntimeError("no replicas")
        else:
            handle.pick_only_delay_s = 10

        decision = await _chat(router, {"model": "model-a"})

        assert decision["replica_id"] == handle.replica_id
        assert [c["kwargs"] for c in handle.calls] == [{"_reserve": False}, {}]

    @pytest.mark.asyncio
    async def test_no_ready_replica_times_out(self, monkeypatch):
        monkeypatch.setattr(applications_module, "CHOOSE_REPLICA_TIMEOUT_S", 0.1)
        router, handles = _new_router()
        handles["llm-model-model-a-1"].delay_s = 10

        response = await _chat(router, {"model": "model-a"})

        assert response.status_code == 503


class TestModelsDecision:
    @pytest.mark.asyncio
    async def test_selects_control_replica(self):
        router, handles = _new_router()

        decision = await router.models(_FakeRequest(headers={SERVE_SESSION_ID: "s"}))

        control = handles["llm-control"]
        assert decision == {
            "application": "llm-control",
            "replica_id": control.replica_id,
        }
        assert control.calls == [
            {"args": (), "kwargs": {"_reserve": False}, "session_id": "s"}
        ]

    @pytest.mark.asyncio
    async def test_unavailable_control(self):
        router, handles = _new_router()
        handles["llm-control"].error = DeploymentUnavailableError(MagicMock())

        response = await router.models(_FakeRequest())

        assert response.status_code == 503


@pytest.mark.parametrize("cls", [RouterApplication, ControlApplication])
@pytest.mark.parametrize("scale_to_zero", [False, True])
def test_deployment_options(cls, scale_to_zero):
    options = cls.get_deployment_options(scale_to_zero=scale_to_zero)
    assert ("min_replicas" in options["autoscaling_config"]) == scale_to_zero


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
