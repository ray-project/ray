"""End-to-end tests for `build_openai_applications` over HAProxy, with mock engines."""
import csv
import glob
import io
import json
import os
import sys
from typing import Dict, List, Optional
from unittest.mock import patch

import httpx
import pytest

import ray
from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.llm._internal.serve.core.configs.llm_config import (
    LLMConfig,
    ModelLoadingConfig,
)
from ray.llm._internal.serve.core.ingress.builder import build_openai_applications
from ray.llm._internal.serve.engines.vllm.vllm_models import VLLMEngineConfig
from ray.llm.tests.serve.cpu.deployments.utils.direct_streaming_utils import (
    CONTENT_HASH_ROUTER,
    consistent_hash_deployment_config,
    requires_direct_streaming,
)
from ray.serve._private.constants import (
    RAY_SERVE_HAPROXY_STATS_PORT,
    SERVE_SESSION_ID,
)
from ray.serve._private.haproxy import HAProxyManager
from ray.serve.config import RequestRouterConfig
from ray.serve.schema import ApplicationStatus

BASE_URL = "http://localhost:8000"
MOCK_ENGINE = "ray.llm.tests.serve.mocks.mock_vllm_engine.MockVLLMEngine"

ROUND_ROBIN_MODEL = "rr-model"
SESSION_MODEL = "org/session-model"
BODY_MODEL = "body-model"
MODELS = [ROUND_ROBIN_MODEL, SESSION_MODEL, BODY_MODEL]

# Starting a dozen engine-importing replicas takes minutes on small CI machines.
pytestmark = pytest.mark.timeout(900)


def _llm_config(model_id: str, deployment_config: dict) -> LLMConfig:
    return LLMConfig(
        model_loading_config=ModelLoadingConfig(model_id=model_id),
        runtime_env={"env_vars": {"RAYLLM_VLLM_ENGINE_CLS": MOCK_ENGINE}},
        deployment_config=deployment_config,
        log_engine_metrics=False,
    )


def _deployment_config(num_replicas: int, router: Optional[str] = None) -> dict:
    config = {"num_replicas": num_replicas, "ray_actor_options": {"num_cpus": 0.1}}
    if router is not None:
        config["request_router_config"] = RequestRouterConfig(
            request_router_class=router
        )
    return config


@pytest.fixture(scope="module")
def targets():
    """Deploy three model applications plus router and control once."""
    serve.shutdown()
    if ray.is_initialized():
        ray.shutdown()

    llm_configs = [
        _llm_config(ROUND_ROBIN_MODEL, _deployment_config(2)),
        _llm_config(
            SESSION_MODEL, {**consistent_hash_deployment_config(), "num_replicas": 2}
        ),
        _llm_config(BODY_MODEL, _deployment_config(2, CONTENT_HASH_ROUTER)),
    ]
    with patch.object(
        VLLMEngineConfig,
        "placement_bundles",
        new_callable=lambda: property(lambda self: []),
    ):
        targets = build_openai_applications({"llm_configs": llm_configs})
    serve.run_many(targets)
    wait_for_condition(_all_running, timeout=600)
    # Decisions fail closed until HAProxy knows every replica the router sees.
    for model in MODELS:
        wait_for_condition(
            lambda: _chat(model, raise_for_status=False).status_code == 200,
            timeout=60,
        )
    yield {t.name: t for t in targets}

    serve.shutdown()
    ray.shutdown()


@pytest.fixture
def app_names(targets) -> Dict[str, str]:
    """Model ID -> model application name."""
    by_route = {t.route_prefix: t.name for t in targets.values()}
    return {m: by_route[f"/v1/{m.replace('/', '--')}"] for m in MODELS}


def _all_running() -> bool:
    applications = serve.status().applications
    return bool(applications) and all(
        app.status == ApplicationStatus.RUNNING for app in applications.values()
    )


def _chat(
    model,
    content: str = "hi",
    *,
    url: str = f"{BASE_URL}/v1/chat/completions",
    headers: Optional[dict] = None,
    raise_for_status: bool = True,
    **body,
) -> httpx.Response:
    payload = {"messages": [{"role": "user", "content": content}], "max_tokens": 2}
    if model is not None:
        payload["model"] = model
    payload.update(body)
    resp = httpx.post(url, json=payload, headers=headers, timeout=30)
    if raise_for_status:
        assert resp.status_code == 200, resp.text
    return resp


def _replica(resp: httpx.Response) -> str:
    return resp.headers["x-replica-id"]


def _router_route_calls() -> int:
    """Requests logged by the model applications' LLMRouters (`/internal/route` only)."""
    log_dir = os.path.join(
        ray._private.worker._global_node.get_logs_dir_path(), "serve"
    )
    total = 0
    for path in glob.glob(os.path.join(log_dir, "replica_*LLMRouter*.log")):
        with open(path, errors="replace") as f:
            total += sum(" -- POST " in line for line in f)
    return total


def _backend_bytes_out() -> Dict[str, int]:
    """Response bytes HAProxy has sent from each backend."""
    resp = httpx.get(f"http://localhost:{RAY_SERVE_HAPROXY_STATS_PORT}/stats;csv")
    rows = csv.DictReader(io.StringIO(resp.text.lstrip("# ")))
    return {
        row["pxname"]: int(row["bout"] or 0)
        for row in rows
        if row["svname"] == "BACKEND"
    }


def _backend(app_name: str) -> str:
    return HAProxyManager.get_safe_name(f"http-{app_name}")


@requires_direct_streaming
class TestRouterApplication:
    def test_routes_each_model_to_its_application(self, targets):
        for model in MODELS:
            resp = _chat(model)
            body = resp.json()
            # The mock engine 404s other models; the client never sees the decision.
            assert body["object"] == "chat.completion"
            assert "replica_id" not in body and "application" not in body

    def test_round_robin(self, targets):
        replicas = [_replica(_chat(ROUND_ROBIN_MODEL)) for _ in range(6)]
        assert len(set(replicas)) == 2

    def test_session_affinity(self, targets):
        def replica_for(session_id: str) -> str:
            resp = _chat(SESSION_MODEL, headers={SERVE_SESSION_ID: session_id})
            assert resp.headers["x-serve-session-id"] == session_id
            return _replica(resp)

        assert len({replica_for("session-1") for _ in range(10)}) == 1
        assert len({replica_for(f"session-{i}") for i in range(10)}) > 1

    def test_body_aware_routing(self, targets):
        """The parsed body reaches the model's request router."""
        assert len({_replica(_chat(BODY_MODEL, "same prompt")) for _ in range(8)}) == 1
        assert len({_replica(_chat(BODY_MODEL, f"prompt {i}")) for i in range(12)}) > 1

    def test_streams_directly_from_the_model_replica(self, targets, app_names):
        """Response bytes come from the model's backend, none from the router's."""
        before = _backend_bytes_out()
        resp = _chat(ROUND_ROBIN_MODEL, stream=True, max_tokens=200)
        assert resp.headers["content-type"].startswith("text/event-stream")
        chunks = [line for line in resp.text.splitlines() if line.startswith("data:")]
        assert len(chunks) > 100

        def counted():
            after = _backend_bytes_out()
            model = _backend(app_names[ROUND_ROBIN_MODEL])
            return after[model] - before[model] >= len(resp.content)

        wait_for_condition(counted, timeout=10)
        router = _backend("llm")
        assert _backend_bytes_out()[router] == before[router]

    def test_bypasses_model_llm_routers(self, targets):
        before = _router_route_calls()
        for model in MODELS:
            for _ in range(3):
                _chat(model)
        # Direct model routes still use their LLMRouter; once logged, the count is final.
        for model in MODELS:
            _chat(
                model,
                url=f"{BASE_URL}/v1/{model.replace('/', '--')}/v1/chat/completions",
            )
        wait_for_condition(
            lambda: _router_route_calls() >= before + len(MODELS), timeout=20
        )
        assert _router_route_calls() == before + len(MODELS)


@requires_direct_streaming
class TestControlRoutes:
    def test_list_models_routes_to_control(self, targets):
        before = _backend_bytes_out()
        resp = httpx.get(f"{BASE_URL}/v1/models")
        assert resp.status_code == 200
        assert [m["id"] for m in resp.json()["data"]] == MODELS

        control, router = _backend("llm-control"), _backend("llm")
        wait_for_condition(
            lambda: _backend_bytes_out()[control] - before[control]
            >= len(resp.content),
            timeout=10,
        )
        assert _backend_bytes_out()[router] == before[router]

    def test_control_routes_are_directly_accessible(self, targets):
        resp = httpx.get(f"{BASE_URL}/v1/control/v1/models")
        assert resp.status_code == 200
        assert [m["id"] for m in resp.json()["data"]] == MODELS

        resp = httpx.get(f"{BASE_URL}/v1/control/v1/models/org--session-model")
        assert resp.json()["id"] == SESSION_MODEL

    @pytest.mark.parametrize(
        "body, status",
        [
            ({"messages": []}, 400),
            ({"model": 7, "messages": []}, 400),
            ({"model": "nope", "messages": []}, 404),
        ],
    )
    def test_chat_errors(self, targets, body, status):
        resp = httpx.post(f"{BASE_URL}/v1/chat/completions", json=body, timeout=30)
        assert resp.status_code == status
        error = resp.json()["error"]
        assert error["code"] == status
        assert error["message"]

    def test_malformed_json(self, targets):
        resp = httpx.post(
            f"{BASE_URL}/v1/chat/completions",
            content=b"{not json",
            headers={"content-type": "application/json"},
            timeout=30,
        )
        assert resp.status_code == 400
        assert resp.json()["error"]["type"] == "BadRequestError"


@requires_direct_streaming
def test_unavailable_model_application(targets, app_names):
    """Runs last in this module: it deletes a model application."""
    serve.delete(app_names[BODY_MODEL])

    def unavailable():
        resp = _chat(BODY_MODEL, raise_for_status=False)
        return resp.status_code == 503 and resp.json()["error"]["code"] == 503

    wait_for_condition(unavailable, timeout=60)
    # Other models are unaffected.
    _chat(ROUND_ROBIN_MODEL)


@requires_direct_streaming
def test_scale_from_zero(targets):
    """With everything at zero replicas, requests 503 until replicas start."""
    # Start from a fresh cluster rather than the module's applications.
    serve.shutdown()
    ray.shutdown()
    autoscaling = {
        "autoscaling_config": {
            "min_replicas": 0,
            "initial_replicas": 0,
            "max_replicas": 1,
            "upscale_delay_s": 0,
            "downscale_delay_s": 600,
            "metrics_interval_s": 0.1,
            "look_back_period_s": 0.2,
        },
        "ray_actor_options": {"num_cpus": 0.1},
    }
    with patch.object(
        VLLMEngineConfig,
        "placement_bundles",
        new_callable=lambda: property(lambda self: []),
    ):
        scaled_targets = build_openai_applications(
            {"llm_configs": [_llm_config("zero-model", autoscaling)]}
        )
    serve.run_many(scaled_targets, wait_for_applications_running=False)
    # Before HAProxy has the new routes, requests 404.
    wait_for_condition(
        lambda: "/v1/zero-model" in httpx.get(f"{BASE_URL}/-/routes").text,
        timeout=60,
    )

    statuses: List[int] = []

    def served():
        resp = _chat("zero-model", raise_for_status=False)
        statuses.append(resp.status_code)
        return resp.status_code == 200

    wait_for_condition(served, timeout=180, retry_interval_ms=2000)
    assert set(statuses) <= {200, 503}, statuses
    assert json.loads(_chat("zero-model").text)["object"] == "chat.completion"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
