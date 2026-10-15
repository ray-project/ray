"""End-to-end tests for `build_openai_router_app` over HAProxy."""

import json
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
from ray.llm._internal.serve.core.ingress.builder import (
    build_openai_app,
    build_openai_router_app,
)
from ray.llm._internal.serve.engines.vllm.vllm_models import VLLMEngineConfig
from ray.llm.tests.serve.cpu.deployments.utils.direct_streaming_utils import (
    CONTENT_HASH_ROUTER,
    consistent_hash_deployment_config,
    requires_direct_streaming,
)
from ray.serve._private.constants import SERVE_SESSION_ID
from ray.serve.config import RequestRouterConfig
from ray.serve.schema import ApplicationStatus

BASE_URL = "http://localhost:8000"
MOCK_ENGINE = "ray.llm.tests.serve.mocks.mock_vllm_engine.MockVLLMEngine"

ROUND_ROBIN_MODEL = "rr-model"
SESSION_MODEL = "org/session-model"
BODY_MODEL = "body-model"
MODELS = [ROUND_ROBIN_MODEL, SESSION_MODEL, BODY_MODEL]
MODEL_APPLICATIONS = {
    ROUND_ROBIN_MODEL: "llm-model-rr",
    SESSION_MODEL: "llm-model-session",
    BODY_MODEL: "llm-model-body",
}

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


def _all_running() -> bool:
    applications = serve.status().applications
    return bool(applications) and all(
        app.status == ApplicationStatus.RUNNING for app in applications.values()
    )


def _deploy(model_configs: Dict[str, LLMConfig]) -> None:
    with patch.object(
        VLLMEngineConfig,
        "placement_bundles",
        new_callable=lambda: property(lambda self: []),
    ):
        for model_id, config in model_configs.items():
            serve.run(
                build_openai_app({"llm_configs": [config]}),
                name=MODEL_APPLICATIONS[model_id],
                route_prefix=f"/models/{model_id.replace('/', '--')}",
            )
    serve.run(
        build_openai_router_app({"model_applications": MODEL_APPLICATIONS}),
        name="llm",
        route_prefix="/",
    )
    wait_for_condition(_all_running, timeout=600)


@pytest.fixture(scope="module")
def applications():
    serve.shutdown()
    if ray.is_initialized():
        ray.shutdown()

    _deploy(
        {
            ROUND_ROBIN_MODEL: _llm_config(ROUND_ROBIN_MODEL, _deployment_config(2)),
            SESSION_MODEL: _llm_config(
                SESSION_MODEL,
                {**consistent_hash_deployment_config(), "num_replicas": 2},
            ),
            BODY_MODEL: _llm_config(
                BODY_MODEL, _deployment_config(2, CONTENT_HASH_ROUTER)
            ),
        }
    )
    for model in MODELS:
        wait_for_condition(
            lambda model=model: _chat(model, raise_for_status=False).status_code == 200,
            timeout=60,
        )
    yield

    serve.shutdown()
    ray.shutdown()


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


def _replica(response: httpx.Response) -> str:
    return response.headers["x-replica-id"]


@requires_direct_streaming
class TestRouterApplication:
    def test_routes_each_model_to_its_application(self, applications):
        for model in MODELS:
            response = _chat(model)
            assert response.json()["object"] == "chat.completion"
            assert "replica_id" not in response.json()
            assert "application" not in response.json()

    def test_round_robin(self, applications):
        replicas = [_replica(_chat(ROUND_ROBIN_MODEL)) for _ in range(6)]
        assert len(set(replicas)) == 2

    def test_session_affinity(self, applications):
        def replica_for(session_id: str) -> str:
            response = _chat(SESSION_MODEL, headers={SERVE_SESSION_ID: session_id})
            assert response.headers["x-serve-session-id"] == session_id
            return _replica(response)

        assert len({replica_for("session-1") for _ in range(10)}) == 1
        assert len({replica_for(f"session-{i}") for i in range(10)}) > 1

    def test_body_aware_routing(self, applications):
        assert len({_replica(_chat(BODY_MODEL, "same prompt")) for _ in range(8)}) == 1
        assert len({_replica(_chat(BODY_MODEL, f"prompt {i}")) for i in range(12)}) > 1

    def test_direct_model_routes_remain_available(self, applications):
        for model in MODELS:
            response = _chat(
                model,
                url=f"{BASE_URL}/models/{model.replace('/', '--')}/v1/chat/completions",
            )
            assert response.json()["object"] == "chat.completion"


@requires_direct_streaming
class TestControlRoutes:
    def test_list_models(self, applications):
        response = httpx.get(f"{BASE_URL}/v1/models")
        assert response.status_code == 200
        assert [model["id"] for model in response.json()["data"]] == MODELS
        assert "x-serve-router-direct-response" not in response.headers

    def test_retrieve_model(self, applications):
        response = httpx.get(f"{BASE_URL}/v1/models/org--session-model")
        assert response.status_code == 200
        assert response.json()["id"] == SESSION_MODEL

        response = httpx.get(f"{BASE_URL}/v1/models/nope")
        assert response.status_code == 404
        assert response.json()["error"]["type"] == "InvalidModel"

    @pytest.mark.parametrize(
        "body, status_code",
        [
            ({"messages": []}, 400),
            ({"model": 7, "messages": []}, 400),
            ({"model": "nope", "messages": []}, 404),
        ],
    )
    def test_chat_errors(self, applications, body, status_code):
        response = httpx.post(f"{BASE_URL}/v1/chat/completions", json=body, timeout=30)
        assert response.status_code == status_code
        assert response.json()["error"]["code"] == status_code


@requires_direct_streaming
def test_unavailable_model_application(applications):
    serve.delete(MODEL_APPLICATIONS[BODY_MODEL])

    def unavailable():
        response = _chat(BODY_MODEL, raise_for_status=False)
        return (
            response.status_code == 503
            and response.json()["error"]["code"] == 503
            and response.headers.get("retry-after") == "1"
        )

    wait_for_condition(unavailable, timeout=60)
    _chat(ROUND_ROBIN_MODEL)


@requires_direct_streaming
def test_scale_from_zero(applications):
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
    model_id = "zero-model"
    model_apps = {model_id: "llm-model-zero"}
    with patch.object(
        VLLMEngineConfig,
        "placement_bundles",
        new_callable=lambda: property(lambda self: []),
    ):
        serve.run(
            build_openai_app({"llm_configs": [_llm_config(model_id, autoscaling)]}),
            name=model_apps[model_id],
            route_prefix="/models/zero-model",
        )
    serve.run(
        build_openai_router_app({"model_applications": model_apps}),
        name="llm",
        route_prefix="/",
    )

    # Listing models is static and must not be coupled to model availability.
    assert httpx.get(f"{BASE_URL}/v1/models").json()["data"][0]["id"] == model_id

    statuses: List[int] = []

    def served():
        response = _chat(model_id, raise_for_status=False)
        statuses.append(response.status_code)
        return response.status_code == 200

    wait_for_condition(served, timeout=180, retry_interval_ms=2000)
    assert set(statuses) <= {200, 503}, statuses
    assert json.loads(_chat(model_id).text)["object"] == "chat.completion"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
