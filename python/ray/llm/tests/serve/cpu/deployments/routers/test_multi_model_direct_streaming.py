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

    def test_discovery_wins_over_a_model_field_in_the_body(self, base_url: str):
        """Route ownership is decided before the body is read at all.

        A `GET /v1/models` carrying a `model` field must still be the ingress's,
        or a client could steer a control request into a model deployment.
        """
        resp = httpx.request(
            "GET",
            f"{base_url}/v1/models",
            json={"model": MODEL_B},
            timeout=30,
        )
        assert resp.status_code == 200, resp.text
        assert {card["id"] for card in resp.json()["data"]} == {MODEL_A, MODEL_B}
        assert "x-deployment-name" not in resp.headers

    def test_unknown_model_detail_is_404_not_a_model_deployment(self, base_url: str):
        resp = httpx.get(f"{base_url}/v1/models/not-a-model", timeout=30)
        assert resp.status_code == 404, resp.text
        assert "x-deployment-name" not in resp.headers

    # ---- Inference reaches the named model ---------------------------------

    @pytest.mark.parametrize("model", [MODEL_A, MODEL_B])
    def test_chat_reaches_the_named_model(self, base_url: str, model: str):
        resp = _chat(base_url, model)
        assert resp.status_code == 200, resp.text
        assert resp.headers["x-deployment-name"] == DEPLOYMENT_NAMES[model]

    def test_chat_completions_is_not_claimed_by_the_control_ingress(
        self, base_url: str
    ):
        """It is not an ingress route, so it goes through model-field routing.

        Both models are reachable on the one path, which is only true because
        the control ingress does not declare it.
        """
        served = {
            _chat(base_url, model).headers["x-deployment-name"]
            for model in (MODEL_A, MODEL_B)
        }
        assert served == set(DEPLOYMENT_NAMES.values())

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

    def test_a_request_is_never_redispatched_into_the_other_backend(
        self, base_url: str
    ):
        """One request must not be able to end up on the wrong model's replicas.

        HAProxy gives each direct deployment its own backend, so a retry or
        redispatch stays inside it. The mock engine 404s a request whose `model`
        is not its own, so a crossed request would be visible here as a 404
        tagged with the wrong deployment; repeat enough times that a
        load-balanced fallback would show up.
        """
        for _ in range(20):
            for model in (MODEL_A, MODEL_B):
                resp = _chat(base_url, model)
                assert resp.status_code == 200, resp.text
                assert resp.headers["x-deployment-name"] == DEPLOYMENT_NAMES[model]

    # ---- Fail-closed selection ---------------------------------------------

    def test_a_request_without_a_model_fails_closed(self, base_url: str):
        """With several models there is no safe default, so this must not serve.

        The router answers 400; HAProxy currently reports any non-200 from the
        router to the client as 503 `router_non_200`.
        """
        resp = _chat(base_url)
        assert resp.status_code in (400, 503), resp.text
        assert "x-deployment-name" not in resp.headers

    def test_an_unknown_model_fails_closed(self, base_url: str):
        """The router answers 404; HAProxy currently surfaces it as 503."""
        resp = _chat(base_url, "not-a-configured-model")
        assert resp.status_code in (404, 503), resp.text
        assert "x-deployment-name" not in resp.headers


@requires_direct_streaming
class TestSingleModelDirectStreaming:
    """One model gets the same topology, and needs no `model` field."""

    @pytest.fixture(scope="class", name="base_url")
    def _base_url(self, cpu_ray_cluster):
        app = build_openai_app(
            LLMServingArgs(llm_configs=[_mock_engine_config(MODEL_A)])
        )
        yield run_app_through_haproxy(app)

    def test_chat_without_a_model_succeeds(self, base_url: str):
        resp = _chat(base_url)
        assert resp.status_code == 200, resp.text
        assert resp.headers["x-deployment-name"] == DEPLOYMENT_NAMES[MODEL_A]

    def test_models_is_served_by_the_control_ingress(self, base_url: str):
        resp = httpx.get(f"{base_url}/v1/models", timeout=30)
        assert resp.status_code == 200, resp.text
        assert [card["id"] for card in resp.json()["data"]] == [MODEL_A]
        assert "x-deployment-name" not in resp.headers


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
