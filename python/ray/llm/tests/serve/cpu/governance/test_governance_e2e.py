"""HTTP end-to-end tests for GovernanceIngress.

These tests go through ``build_openai_app`` + ``serve.run`` and the OpenAI
HTTP API. MockVLLMEngine stands in for a real model. Ingress-level tests that
call ``_process_llm_request`` live in ``test_governance_ingress.py``.
"""

import sys
from typing import Dict, Optional

import httpx
import pytest

from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.llm._internal.serve.core.governance.reference_middleware import (
    BudgetMiddleware,
    PIIMiddleware,
)
from ray.serve.llm import LLMConfig, ModelLoadingConfig, build_openai_app
from ray.serve.llm.governance import GovernanceIngress

MODEL_ID = "test-model"
BASE_URL = "http://localhost:8000"
CHAT_URL = f"{BASE_URL}/v1/chat/completions"
MOCK_ENGINE = "ray.llm.tests.serve.mocks.mock_vllm_engine.MockVLLMEngine"
PII_ENGINE = "ray.llm.tests.serve.mocks.mock_vllm_engine.PiiChatMockEngine"


def _llm_config(engine_cls: str = MOCK_ENGINE) -> LLMConfig:
    return LLMConfig(
        model_loading_config=ModelLoadingConfig(model_id=MODEL_ID),
        runtime_env={"env_vars": {"RAYLLM_VLLM_ENGINE_CLS": engine_cls}},
        log_engine_metrics=False,
        deployment_config={"num_replicas": 1},
    )


def _deploy_governance_app(middlewares, *, engine_cls: str = MOCK_ENGINE) -> None:
    app = build_openai_app(
        {
            "llm_configs": [_llm_config(engine_cls)],
            "ingress_cls_config": {
                "ingress_cls": GovernanceIngress,
                "ingress_extra_kwargs": {"middlewares": middlewares},
            },
            "ingress_deployment_config": {"num_replicas": 1},
        }
    )
    serve.run(app)
    wait_for_condition(_models_ready, timeout=60)


def _models_ready() -> bool:
    response = httpx.get(f"{BASE_URL}/v1/models", timeout=5.0)
    if response.status_code != 200:
        return False
    model_ids = [item.get("id") for item in response.json().get("data", [])]
    return MODEL_ID in model_ids


def _chat(
    content: str,
    *,
    stream: bool = False,
    user: Optional[str] = None,
    max_tokens: int = 2,
    headers: Optional[Dict[str, str]] = None,
) -> httpx.Response:
    body = {
        "model": MODEL_ID,
        "messages": [{"role": "user", "content": content}],
        "stream": stream,
        "max_tokens": max_tokens,
    }
    if user is not None:
        body["user"] = user
    return httpx.post(CHAT_URL, json=body, headers=headers, timeout=60.0)


def test_governance_e2e_blocks_pii_before_llm(
    shutdown_ray_and_serve, disable_placement_bundles
):
    _deploy_governance_app([PIIMiddleware()])

    response = _chat("Email me at secret@example.com")

    assert response.status_code == 400, response.text
    payload = response.json()
    assert payload["error"]["code"] == "PII_DETECTED", payload


def test_governance_e2e_blocks_pii_streaming_request(
    shutdown_ray_and_serve, disable_placement_bundles
):
    _deploy_governance_app([PIIMiddleware()])

    response = _chat("Email me at secret@example.com", stream=True)

    assert response.status_code == 400, response.text
    assert response.json()["error"]["code"] == "PII_DETECTED", response.json()


def test_governance_e2e_budget_allows_then_blocks(
    shutdown_ray_and_serve, disable_placement_bundles
):
    # Any recorded usage exhausts a budget of 1, so the second request is blocked.
    _deploy_governance_app([BudgetMiddleware(token_budget=1)])

    first = _chat("Hello", user="budget-user", max_tokens=2)
    assert first.status_code == 200, first.text
    payload = first.json()
    assert payload["choices"][0]["message"]["content"]
    assert payload["usage"]["total_tokens"] >= 1, payload

    second = _chat("Hello", user="budget-user", max_tokens=2)
    assert second.status_code == 402, second.text
    assert second.json()["error"]["code"] == "BUDGET_EXCEEDED"


def test_governance_e2e_blocks_pii_in_model_response(
    shutdown_ray_and_serve, disable_placement_bundles
):
    _deploy_governance_app(
        [PIIMiddleware(scan_requests=False, scan_responses=True)],
        engine_cls=PII_ENGINE,
    )

    response = _chat("Say hello")

    assert response.status_code == 400, response.text
    assert response.json()["error"]["code"] == "PII_DETECTED", response.json()


def test_governance_e2e_pii_and_budget_chain(
    shutdown_ray_and_serve, disable_placement_bundles
):
    _deploy_governance_app(
        [BudgetMiddleware(token_budget=100), PIIMiddleware()],
    )

    response = _chat("Summarize this doc")

    assert response.status_code == 200, response.text
    assert response.json()["choices"][0]["message"]["content"]


def test_governance_e2e_streaming_success(
    shutdown_ray_and_serve, disable_placement_bundles
):
    # Token accounting after a stream is covered by
    # test_governance_e2e_budget_allows_then_blocks. This test only checks
    # that a streaming request still returns SSE through the HTTP ingress.
    _deploy_governance_app([BudgetMiddleware(token_budget=100)])

    response = _chat("Hello", stream=True, user="stream-user", max_tokens=4)

    assert response.status_code == 200, response.text
    assert "text/event-stream" in response.headers["content-type"]
    assert "data:" in response.text


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
