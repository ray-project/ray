import asyncio
import json
import os
from unittest.mock import patch

import httpx
import pytest

import ray
from ray import serve
from ray._common.network_utils import find_free_port
from ray._common.test_utils import wait_for_condition
from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.serving_patterns.prefill_decode.builder import (
    build_pd_openai_app,
)
from ray.llm.tests.serve.cpu.deployments.utils.direct_streaming_utils import (
    requires_direct_streaming,
    run_app_through_haproxy,
)
from ray.llm.tests.serve.mocks.mock_pd_kv_engine import MockLLMPDRouter


@requires_direct_streaming
class TestPDKVAwareDirectStreaming:
    @pytest.fixture(scope="class")
    def app(self):
        pytest.importorskip("dynamo.llm")
        from ray.llm._internal.serve.engines.vllm.vllm_models import VLLMEngineConfig

        config = LLMConfig(
            model_loading_config={"model_id": "test-model"},
            accelerator_type=None,
            deployment_config={
                "num_replicas": 2,
                "ray_actor_options": {"num_cpus": 0.1},
                "request_router_config": {
                    "request_router_class": "ray.serve.llm.request_router.KVAwareRouter",
                },
            },
            runtime_env={
                "env_vars": {
                    "RAYLLM_VLLM_ENGINE_CLS": "ray.llm.tests.serve.mocks.mock_pd_kv_engine.MockPDKVEngine"
                }
            },
            engine_kwargs={
                "kv_transfer_config": {
                    "kv_connector": "NixlConnector",
                    "kv_role": "kv_both",
                }
            },
        )
        with patch.dict(
            os.environ,
            {
                "RAY_SERVE_INGRESS_ROUTER_REPLICAS_PER_NODE": "2",
                "RAY_SERVE_DIRECT_INGRESS_MIN_DRAINING_PERIOD_S": "0",
            },
        ):
            ray.init(
                num_cpus=8, object_store_memory=512 * 1024**2, include_dashboard=False
            )
            try:
                serve.start(http_options={"port": find_free_port()})
                with patch.object(
                    VLLMEngineConfig, "placement_bundles", property(lambda self: [])
                ), patch(
                    "ray.llm._internal.serve.core.ingress.pd_router.LLMPDRouter",
                    MockLLMPDRouter,
                ):
                    application = build_pd_openai_app(
                        {
                            "prefill_config": config.model_copy(deep=True),
                            "decode_config": config.model_copy(deep=True),
                        }
                    )
                    url = run_app_through_haproxy(application, timeout_s=120)
                wait_for_condition(
                    lambda: httpx.post(
                        url + "/v1/completions",
                        json={
                            "model": "test-model",
                            "prompt": "ready",
                            "max_tokens": 1,
                        },
                        timeout=30,
                    ).status_code
                    == 200,
                    timeout=90,
                )
                routers = serve.get_deployment_handle("LLMRouter", app_name="default")
                routers._init(_run_router_in_separate_loop=True)
                assert len(routers.broadcast("health").results(timeout_s=10)) == 2
                yield url
            finally:
                serve.shutdown()
                ray.shutdown()

    @pytest.mark.asyncio
    async def test_sequential_prefill_and_direct_decode(self, app):
        async with httpx.AsyncClient(base_url=app, timeout=30) as client:
            results = await asyncio.gather(
                *[
                    client.post(
                        "/v1/completions",
                        headers={"x-request-id": "same-client-id"},
                        json={
                            "model": "test-model",
                            "prompt": "concurrent " * 32,
                            "max_tokens": 4,
                        },
                    )
                    for _ in range(12)
                ]
            )
        infos = []
        for response in results:
            assert response.status_code == 200, response.text
            info = json.loads(response.json()["choices"][0]["text"])
            assert info["prefill_done_at"] <= info["decode_started_at"]
            assert "Prefill:" in info["prefill"]
            assert "Decode:" in info["decode"]
            assert info["tokens"] == [ord(c) for c in "concurrent " * 32]
            assert response.headers["x-replica-id"] == info["decode"].rsplit("#", 1)[-1]
            infos.append(info)
        assert len({i["routing_id"] for i in infos}) == len(infos)
        assert len({i["prefill"] for i in infos}) == 2
        assert len({i["decode"] for i in infos}) == 2

    @pytest.mark.asyncio
    async def test_chat_stream(self, app):
        async with httpx.AsyncClient(base_url=app, timeout=30) as client:
            async with client.stream(
                "POST",
                "/v1/chat/completions",
                json={
                    "model": "test-model",
                    "messages": [{"role": "user", "content": "hello"}],
                    "max_tokens": 4,
                    "stream": True,
                },
            ) as response:
                assert response.status_code == 200
                lines = [line async for line in response.aiter_lines() if line]
                info = json.loads(lines[0].removeprefix("data: "))
                assert "Prefill:" in info["prefill"]
                assert "Decode:" in info["decode"]
                assert info["tokens"] == [ord(c) for c in "chat"]
                assert lines[-1] == "data: [DONE]"

    @pytest.mark.asyncio
    async def test_prefill_reuses_cached_prefix(self, app):
        prefix = "cache-affinity " * 64
        async with httpx.AsyncClient(base_url=app, timeout=30) as client:

            async def complete(suffix):
                response = await client.post(
                    "/v1/completions",
                    json={
                        "model": "test-model",
                        "prompt": prefix + suffix,
                        "max_tokens": 4,
                    },
                )
                assert response.status_code == 200, response.text
                return json.loads(response.json()["choices"][0]["text"])

            seed = await complete("seed")
            await asyncio.sleep(0.5)
            for suffix in ("one", "two", "three", "four", "five", "six"):
                result = await complete(suffix)
                assert result["prefill"] == seed["prefill"]

    @pytest.mark.asyncio
    async def test_prefill_failure_and_truncation(self, app):
        async with httpx.AsyncClient(base_url=app, timeout=30) as client:
            for prompt in ("fail prefill", "long " * 100000):
                response = await client.post(
                    "/v1/completions",
                    json={
                        "model": "test-model",
                        "prompt": prompt,
                        "max_tokens": 1,
                    },
                )
                assert response.status_code >= 400
            response = await client.post(
                "/v1/completions",
                json={
                    "model": "test-model",
                    "prompt": "still healthy",
                    "max_tokens": 1,
                },
            )
            assert response.status_code == 200, response.text

    @pytest.mark.asyncio
    async def test_decode_selection_uses_load_after_prefill(self, app):
        async with httpx.AsyncClient(base_url=app, timeout=30) as client:
            slow = asyncio.create_task(
                client.post(
                    "/v1/completions",
                    json={
                        "model": "test-model",
                        "prompt": "slow-prefill " * 32,
                        "max_tokens": 4,
                    },
                )
            )
            await asyncio.sleep(0.5)
            async with client.stream(
                "POST",
                "/v1/completions",
                json={
                    "model": "test-model",
                    "prompt": "busy decode " * 256,
                    "stream": True,
                    "max_tokens": 200,
                },
            ) as busy:
                assert busy.status_code == 200
                busy_lines = busy.aiter_lines()
                first = await anext(busy_lines)
                busy_info = json.loads(first.removeprefix("data: "))
                response = await slow
                assert response.status_code == 200, response.text
                info = json.loads(response.json()["choices"][0]["text"])
                assert busy_info["decode_started_at"] < info["prefill_done_at"]
                assert info["decode"] != busy_info["decode"]
            # Closing the stream cancels decode; subsequent requests remain routable.
            response = await client.post(
                "/v1/completions",
                json={
                    "model": "test-model",
                    "prompt": "after cancel",
                    "max_tokens": 1,
                },
            )
            assert response.status_code == 200, response.text

    @pytest.mark.asyncio
    async def test_transfer_metadata_survives_missing_tokens(self, app):
        prompt = "drop-tokens and tokenize again"
        async with httpx.AsyncClient(base_url=app, timeout=30) as client:
            response = await client.post(
                "/v1/completions",
                json={"model": "test-model", "prompt": prompt, "max_tokens": 4},
            )
        assert response.status_code == 200, response.text
        info = json.loads(response.json()["choices"][0]["text"])
        assert not info["staged_tokens"]
        assert info["tokens"] == [ord(c) for c in prompt]
        assert "Prefill:" in info["prefill"]
        assert info["prefill_done_at"] <= info["decode_started_at"]

    @pytest.mark.asyncio
    async def test_oversized_metadata_is_rejected(self, app):
        async with httpx.AsyncClient(base_url=app, timeout=30) as client:
            response = await client.post(
                "/v1/completions",
                json={
                    "model": "test-model",
                    "prompt": "oversized-metadata",
                    "max_tokens": 1,
                },
            )
        assert response.status_code == 503, response.text

    @pytest.mark.asyncio
    async def test_stream_survives_router_exit(self, app):
        async with httpx.AsyncClient(base_url=app, timeout=30) as client:
            async with client.stream(
                "POST",
                "/v1/completions",
                json={
                    "model": "test-model",
                    "prompt": "stream " * 32,
                    "stream": True,
                    "max_tokens": 40,
                },
            ) as response:
                assert response.status_code == 200
                lines = response.aiter_lines()
                first = await anext(lines)
                assert '"decode"' in first
                routers = serve.get_deployment_handle("LLMRouter", app_name="default")
                await routers.broadcast("exit").results_async(return_exceptions=True)
                rest = [line async for line in lines]
                assert "data: [DONE]" in rest
                assert sum('"token"' in line for line in rest) == 39
