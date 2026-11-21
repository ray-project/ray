import asyncio
from collections import defaultdict
import json
import sys

import pytest
import requests
from dynamo.llm import compute_block_hash_for_seq

import ray
from ray import serve
from ray._common.test_utils import async_wait_for_condition

from utils import (
    MODEL_ID,
    _TestKVAwareRouter,
    build_kv_app,
    build_kv_config,
    patch_ingress,
)

APP_NAME = "lifecycle_tracking_gpu_test"
# Token tracking reports Serve's canonical request id, matching the id used for
# routing, even if vLLM derives a separate engine-level id internally.
REQUEST_ID = "gpu-req-1"
BLOCK_SIZE = 16
# Long enough, with ignore_eos, to observe the request while it decodes.
MAX_TOKENS = 512
# A multi-block prompt so the engine caches retrievable KV blocks.
PROMPT_TEXT = (
    "Repeat the following instruction carefully and then answer it in full "
    "detail: describe the water cycle, including evaporation, condensation, "
    "precipitation, and collection, giving a concrete real-world example for "
    "each of the four stages and how they connect."
)


def num_prompt_blocks(token_ids):
    """Number of full KV blocks in a token sequence (matches the indexer)."""
    return len(compute_block_hash_for_seq(list(token_ids), BLOCK_SIZE))


def tokenize_chat(endpoint):
    """The engine's exact token ids for the chat-templated prompt."""
    host, port = endpoint
    resp = requests.post(
        f"http://{host}:{port}/tokenize",
        json={
            "model": MODEL_ID,
            "messages": [{"role": "user", "content": PROMPT_TEXT}],
            "add_generation_prompt": True,
        },
        timeout=60,
    )
    assert resp.status_code == 200, resp.text
    return resp.json()["tokens"]


class TestLifecycleTracking:
    """The engine publishes each request's lifecycle events to every LLMRouter
    replica, whose selection service applies them to its booking of the
    request. The test ``LLMRouter`` subclass exposes each router's booked load
    over the deployment handle so the test can assert exact accounting."""

    @pytest.fixture(scope="class")
    def deployed_handle(self):
        """Deploy a direct-streaming LLMServer with KV events on and the
        introspection ingress."""
        if not ray.is_initialized():
            ray.init(address="auto")
        serve.shutdown()

        # Built before patch_ingress; see build_kv_config.
        llm_config = build_kv_config(
            request_router_class=_TestKVAwareRouter,
            kv_events_port_base=21600,
        )
        # Swap the ingress for the introspection LLMRouter so each router's
        # booked load is reachable over the deployment handle.
        with patch_ingress():
            handle = serve.run(build_kv_app(llm_config), name=APP_NAME)
        yield handle
        serve.shutdown()

    async def _backend_endpoint(self, handle):
        """Poll for the replica's backend HTTP (host, port); it is reported
        asynchronously after the replica starts its direct-ingress server."""
        for _ in range(60):
            async with handle.choose_replica() as selection:
                endpoint = selection._replica.backend_http_endpoint
            if endpoint is not None:
                return endpoint
            await asyncio.sleep(1.0)
        raise AssertionError("replica backend HTTP endpoint never became available")

    async def _registered_worker(self, router, num_ingress_replicas):
        """Wait for every ingress replica to see the worker as schedulable and
        subscribe to its lifecycle events."""

        async def registered():
            per_replica = await router.broadcast(
                "get_registered_worker_ids"
            ).results_async()
            sources = await router.broadcast("get_lifecycle_sources").results_async()
            return (
                len(per_replica) == num_ingress_replicas
                and all(len(ids) == 1 for ids in per_replica)
                and all(len(endpoints) == 1 for endpoints in sources)
            )

        await async_wait_for_condition(registered, timeout=90, retry_interval_ms=1000)
        # Subscriptions connect in the background.
        await asyncio.sleep(1)
        return (await router.broadcast("get_registered_worker_ids").results_async())[0][
            0
        ]

    @pytest.mark.asyncio
    async def test_engine_lifecycle_reaches_every_router(self, deployed_handle):
        router = serve.get_deployment_handle("LLMRouter", app_name=APP_NAME)
        host, port = await self._backend_endpoint(deployed_handle)
        replica_ids = await router.broadcast("get_replica_id").results_async()
        # The replica's worker must register before a request can book against it.
        worker_id = await self._registered_worker(router, len(replica_ids))

        async def loads():
            return {
                load["replica_id"]: (
                    load["active_requests"],
                    load["potential_prefill_tokens"],
                )
                for load in await router.broadcast(
                    "get_load_snapshot", worker_id
                ).results_async()
            }

        assert all(active == 0 for active, _ in (await loads()).values())
        prompt_token_ids = tokenize_chat((host, port))
        await router.select_worker.remote(
            REQUEST_ID, prompt_token_ids, [worker_id], MAX_TOKENS
        )

        # One ingress selects; every ingress books its load through the mesh.
        async def booked():
            readings = await loads()
            return len(readings) == len(replica_ids) and all(
                active == 1 and prefill > 0 for active, prefill in readings.values()
            )

        await async_wait_for_condition(booked, timeout=30, retry_interval_ms=100)

        # X-Request-Id pins the engine request id; include_usage returns the
        # engine's own token counts as ground truth.
        url = f"http://{host}:{port}/v1/chat/completions"
        payload = {
            "model": MODEL_ID,
            "messages": [{"role": "user", "content": PROMPT_TEXT}],
            "stream": True,
            "stream_options": {"include_usage": True},
            "max_tokens": MAX_TOKENS,
            "ignore_eos": True,
            "temperature": 0.0,
        }

        # Sample every router's booked load after each streamed chunk: completion
        # frees the request, so its in-flight state is only observable while
        # streaming.
        usage = None
        in_flight = defaultdict(set)
        with requests.post(
            url,
            json=payload,
            headers={"X-Request-Id": REQUEST_ID},
            stream=True,
            timeout=120,
        ) as resp:
            assert resp.status_code == 200, resp.text
            for raw in resp.iter_lines():
                if not raw:
                    continue
                line = raw.decode("utf-8")
                if not line.startswith("data:"):
                    continue
                data = line[len("data:") :].strip()
                if data == "[DONE]":
                    break
                chunk = json.loads(data)
                if chunk.get("usage"):
                    usage = chunk["usage"]
                for replica_id, reading in (await loads()).items():
                    in_flight[replica_id].add(reading)

        assert usage is not None, "expected a final usage chunk"
        assert len(prompt_token_ids) == usage["prompt_tokens"]  # token ids booked
        # Every router applied the engine's prefill completion to its own
        # booking while the request decoded.
        assert set(in_flight) == set(replica_ids)
        assert all((1, 0) in readings for readings in in_flight.values()), in_flight

        # The engine's completion frees the request on every router: no leaked
        # active load to skew later scoring.
        async def freed():
            return all(reading == (0, 0) for reading in (await loads()).values())

        await async_wait_for_condition(freed, timeout=15, retry_interval_ms=200)

        # The engine indexed the prompt's KV blocks; they show as cache overlap
        # on the worker (the prefix the next overlapping request would reuse).
        prompt_blocks = num_prompt_blocks(prompt_token_ids)
        assert prompt_blocks >= 1

        async def overlap_indexed():
            per_replica = await router.broadcast(
                "get_kv_overlap_blocks", prompt_token_ids
            ).results_async()
            return all(
                overlaps.get(worker_id, 0) == prompt_blocks for overlaps in per_replica
            )

        await async_wait_for_condition(
            overlap_indexed, timeout=60, retry_interval_ms=500
        )

    @pytest.mark.asyncio
    async def test_booked_reservation_changes_scoring_load(self, deployed_handle):
        """A booked reservation shows up as active load on the worker (the value
        scoring consumes) on every router, and freeing it restores baseline."""
        router = serve.get_deployment_handle("LLMRouter", app_name=APP_NAME)
        num_ingress_replicas = len(
            await router.broadcast("get_replica_id").results_async()
        )
        worker_id = await self._registered_worker(router, num_ingress_replicas)

        async def worker_active_requests():
            return await router.broadcast(
                "get_worker_active_requests", worker_id
            ).results_async()

        assert all(active == 0 for active in await worker_active_requests())

        await router.select_worker.remote("probe", list(range(64)), [worker_id], 32)

        async def reservation_converged():
            active_requests = await worker_active_requests()
            return len(active_requests) == num_ingress_replicas and all(
                active == 1 for active in active_requests
            )

        await async_wait_for_condition(
            reservation_converged, timeout=30, retry_interval_ms=100
        )

        await router.broadcast("free_reservation", "probe").results_async()

        async def worker_load_cleared():
            return all(active == 0 for active in await worker_active_requests())

        await async_wait_for_condition(
            worker_load_cleared, timeout=15, retry_interval_ms=200
        )


if __name__ == "__main__":
    if not ray.is_initialized():
        ray.init(address="auto")
    sys.exit(pytest.main(["-v", "-s", __file__]))
