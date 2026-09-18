"""Real NIXL transfers through two prefill, decode, and ingress replicas."""

import asyncio
import json
import os
from unittest.mock import patch

import httpx
import pytest

import ray
from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.serving_patterns.prefill_decode.builder import (
    build_pd_openai_app,
)
from ray.llm.tests.serve.cpu.deployments.utils.direct_streaming_utils import (
    requires_direct_streaming,
)


@requires_direct_streaming
@pytest.mark.timeout(600)
def test_pd_kv_nixl_streaming():
    pytest.importorskip("dynamo.llm")
    model = os.environ.get("PD_KV_TEST_MODEL", "Qwen/Qwen3-0.6B")
    config = LLMConfig(
        model_loading_config={"model_id": "qwen", "model_source": model},
        accelerator_type=None,
        deployment_config={
            "num_replicas": 2,
            "ray_actor_options": {"num_cpus": 0.5},
            "request_router_config": {
                "request_router_class": "ray.serve.llm.request_router.KVAwareRouter",
            },
        },
        engine_kwargs={
            "max_model_len": 4096,
            "enforce_eager": True,
            "gpu_memory_utilization": 0.4,
            "enable_prefix_caching": True,
            "enable_prompt_tokens_details": True,
            "kv_transfer_config": {
                "kv_connector": "NixlConnector",
                "kv_role": "kv_both",
            },
        },
    )
    with patch.dict(
        os.environ,
        {
            "RAY_SERVE_INGRESS_ROUTER_REPLICAS_PER_NODE": "2",
            "RAY_SERVE_DIRECT_INGRESS_MIN_DRAINING_PERIOD_S": "0",
        },
    ):
        ray.init(num_cpus=12, object_store_memory=1024**3, include_dashboard=False)
        try:
            if ray.cluster_resources().get("GPU", 0) < 4:
                pytest.skip("Requires four GPUs")
            serve.run(
                build_pd_openai_app(
                    {
                        "prefill_config": config.model_copy(deep=True),
                        "decode_config": config.model_copy(deep=True),
                    }
                )
            )
            url = "http://127.0.0.1:8000"
            payload = {
                "model": "qwen",
                "prompt": "The capital of France is",
                "max_tokens": 16,
                "temperature": 0,
                "ignore_eos": True,
            }
            wait_for_condition(
                lambda: httpx.post(
                    url + "/v1/completions", json=payload, timeout=60
                ).status_code
                == 200,
                timeout=120,
            )
            routers = serve.get_deployment_handle("LLMRouter", app_name="default")
            routers._init(_run_router_in_separate_loop=True)
            assert len(routers.broadcast("health").results(timeout_s=10)) == 2
            asyncio.run(_check_generation(url, payload))
        finally:
            serve.shutdown()
            ray.shutdown()


async def _check_generation(url, payload):
    cases = [("France", "Paris"), ("Japan", "Tokyo"), ("Italy", "Rome")] * 4
    async with httpx.AsyncClient(base_url=url, timeout=90) as client:
        responses = await asyncio.gather(
            *[
                client.post(
                    "/v1/completions",
                    json={**payload, "prompt": f"The capital of {country} is"},
                    headers={"x-request-id": "repeated-client-id"},
                )
                for country, _ in cases
            ]
        )
        for response in responses:
            assert response.status_code == 200, response.text
            usage = response.json()["usage"]
            assert usage["completion_tokens"] == payload["max_tokens"]
            assert usage["prompt_tokens_details"]["cached_tokens"] > 0
        texts = [r.json()["choices"][0]["text"] for r in responses]
        for text, (_, city) in zip(texts, cases):
            assert city in text, text
        assert len({r.json()["id"] for r in responses}) == len(responses)
        async with client.stream(
            "POST", "/v1/completions", json={**payload, "stream": True}
        ) as stream:
            assert stream.status_code == 200
            text = ""
            finished = False
            async for line in stream.aiter_lines():
                if line == "data: [DONE]":
                    finished = True
                elif line.startswith("data: "):
                    chunk = json.loads(line[6:])
                    text += "".join(
                        choice["text"] for choice in chunk.get("choices", [])
                    )
            assert finished
            assert "Paris" in text, text
        chat = await client.post(
            "/v1/chat/completions",
            json={
                "model": "qwen",
                "messages": [
                    {
                        "role": "user",
                        "content": "What is the capital of France? Answer with the city name only.",
                    }
                ],
                "max_tokens": 32,
                "temperature": 0,
                "chat_template_kwargs": {"enable_thinking": False},
            },
        )
        assert chat.status_code == 200, chat.text
        assert "Paris" in chat.json()["choices"][0]["message"]["content"]
