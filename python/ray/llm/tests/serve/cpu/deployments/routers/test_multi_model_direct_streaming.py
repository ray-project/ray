"""Multi-model direct streaming end to end, through the real HAProxy path.

Two `MockVLLMEngine`-backed models are served by one application:

    DirectStreamingIngress      GET /v1/models, GET /v1/models/{model:path}
    |- LLMServer:model-a        _direct_http=True
    |- LLMServer:model-b        _direct_http=True
    `- LLMRouter                ingress request router

Every request reaches HAProxy, which asks `LLMRouter` where to send it. These
tests assert the two halves of that decision: a discovery request reaches the
control ingress, and an inference request reaches the one model deployment its
`model` field names -- never the other one, and never a fallback.

The mock engine tags each response with `x-deployment-name`, which is what makes
"reached the right backend" directly observable rather than inferred from a 200.
"""

import sys
from typing import Optional
from unittest.mock import patch

import httpx
import pytest

import ray
from ray import serve
from ray.llm._internal.serve.core.configs.llm_config import (
    LLMConfig,
    ModelLoadingConfig,
)
from ray.llm._internal.serve.core.ingress.builder import (
    LLMServingArgs,
    build_openai_app,
)
from ray.llm._internal.serve.engines.vllm.vllm_models import VLLMEngineConfig
from ray.llm.tests.serve.cpu.deployments.utils.direct_streaming_utils import (
    requires_direct_streaming,
    run_app_through_haproxy,
)
from ray.serve._private.constants import (
    RAY_SERVE_INGRESS_REQUEST_ROUTER_FORWARD_BODY,
)

MODEL_A = "model-a"
# A `/` in the id exercises the `{model:path}` discovery route and the
# `/`-to-`--` deployment-name rewrite.
MODEL_B = "meta-llama/model-b"

DEPLOYMENT_NAMES = {
    MODEL_A: "LLMServer:model-a",
    MODEL_B: "LLMServer:meta-llama--model-b",
}

requires_body_forwarding = pytest.mark.skipif(
    not RAY_SERVE_INGRESS_REQUEST_ROUTER_FORWARD_BODY,
    reason="Multi-model routing selects on the request body, which HAProxy only "
    "forwards with RAY_SERVE_INGRESS_REQUEST_ROUTER_FORWARD_BODY=1.",
)


@pytest.fixture(scope="class", name="cpu_ray_cluster")
def _cpu_ray_cluster():
    """Class-scoped stand-in for the function-scoped conftest fixtures.

    Bringing up two model deployments behind HAProxy costs tens of seconds, so
    every test in a class reads one running application instead of redeploying
    it. Patches placement bundles away so the mock engine schedules on CPU.
    """
    serve.shutdown()
    if ray.is_initialized():
        ray.shutdown()
    with patch.object(
        VLLMEngineConfig,
        "placement_bundles",
        new_callable=lambda: property(lambda self: []),
    ):
        yield
    serve.shutdown()
    if ray.is_initialized():
        ray.shutdown()


def _mock_engine_config(model_id: str) -> LLMConfig:
    return LLMConfig(
        model_loading_config=ModelLoadingConfig(model_id=model_id),
        runtime_env={
            "env_vars": {
                "RAYLLM_VLLM_ENGINE_CLS": (
                    "ray.llm.tests.serve.mocks.mock_vllm_engine.MockVLLMEngine"
                )
            }
        },
        deployment_config={"num_replicas": 1, "ray_actor_options": {"num_cpus": 0.1}},
        accelerator_type=None,
        log_engine_metrics=False,
    )


def _chat(
    base_url: str,
    model: Optional[str] = None,
    *,
    stream: bool = False,
    **kwargs,
) -> httpx.Response:
    body = {"messages": [{"role": "user", "content": "hi"}], "max_tokens": 2, **kwargs}
    if model is not None:
        body["model"] = model
    if stream:
        body["stream"] = True
    return httpx.post(f"{base_url}/v1/chat/completions", json=body, timeout=30)


@requires_direct_streaming
@requires_body_forwarding
class TestMultiModelDirectStreaming:
    @pytest.fixture(scope="class", name="base_url")
    def _base_url(self, cpu_ray_cluster):
        app = build_openai_app(
            LLMServingArgs(
                llm_configs=[_mock_engine_config(MODEL_A), _mock_engine_config(MODEL_B)]
            )
        )
        yield run_app_through_haproxy(app)

    # ---- Discovery reaches the control ingress -----------------------------

    def test_models_lists_both_models(self, base_url: str):
        resp = httpx.get(f"{base_url}/v1/models", timeout=30)
        assert resp.status_code == 200, resp.text
        assert {card["id"] for card in resp.json()["data"]} == {MODEL_A, MODEL_B}
        # No model deployment tagged this response, so the control ingress
        # served it -- the mock engine also serves `/v1/models`, and reaching
        # that one would list a single model.
        assert "x-deployment-name" not in resp.headers

    @pytest.mark.parametrize("model", [MODEL_A, MODEL_B])
    def test_model_detail_reaches_the_control_ingress(self, base_url: str, model: str):
        resp = httpx.get(f"{base_url}/v1/models/{model}", timeout=30)
        assert resp.status_code == 200, resp.text
        assert resp.json()["id"] == model
        assert "x-deployment-name" not in resp.headers

    # ---- Inference reaches the named model ---------------------------------

    @pytest.mark.parametrize("model", [MODEL_A, MODEL_B])
    def test_chat_reaches_the_named_model(self, base_url: str, model: str):
        resp = _chat(base_url, model)
        assert resp.status_code == 200, resp.text
        assert resp.headers["x-deployment-name"] == DEPLOYMENT_NAMES[model]

    @pytest.mark.parametrize("model", [MODEL_A, MODEL_B])
    def test_streaming_succeeds_through_both_models(self, base_url: str, model: str):
        with httpx.stream(
            "POST",
            f"{base_url}/v1/chat/completions",
            json={
                "model": model,
                "messages": [{"role": "user", "content": "hi"}],
                "max_tokens": 4,
                "stream": True,
            },
            timeout=30,
        ) as resp:
            assert resp.status_code == 200, resp.read()
            assert resp.headers["x-deployment-name"] == DEPLOYMENT_NAMES[model]
            chunks = [line for line in resp.iter_lines() if line.strip()]
        assert chunks, "expected at least one streamed chunk"

    # ---- Fail-closed selection ---------------------------------------------

    @pytest.mark.parametrize(
        "model", [None, "not-a-configured-model"], ids=["missing", "unknown"]
    )
    def test_an_unresolvable_model_fails_closed(self, base_url: str, model):
        """With several models there is no safe default, so nothing may serve.

        The router answers 4xx; which status HAProxy surfaces to the client is
        its business (today any router non-200 becomes a 503).
        """
        resp = _chat(base_url, model)
        assert resp.status_code >= 400, resp.text
        assert "x-deployment-name" not in resp.headers


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
