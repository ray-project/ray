"""Tests for token-load state across LLMRouter ingress replicas.

Each ingress owns a local KV selector and books load when it performs atomic
selection for a request. Engine lifecycle events are still broadcast, but a
non-routing ingress ignores events for reservations it does not own. Cached KV
blocks are tracked per replica: every ingress independently subscribes to each
engine worker's KV-event stream.
"""

import asyncio
import sys
import threading

import pytest
import requests
from dynamo.llm import compute_block_hash_for_seq

import ray
from ray import serve
from ray._common.test_utils import async_wait_for_condition
from ray.serve.llm.request_router import KVAwareRouter

from utils import MODEL_ID, build_kv_app, build_kv_config, patch_ingress

APP_NAME = "token_load_test"
KV_EVENTS_PORT_BASE = 21800
MAX_TOKENS = 16
BLOCK_SIZE = 16
# A multi-block prompt so the engine caches retrievable KV blocks.
PROMPT_TEXT = (
    "Repeat the following instruction carefully and then answer it in full "
    "detail: describe the water cycle, including evaporation, condensation, "
    "precipitation, and collection, giving a concrete real-world example for "
    "each of the four stages and how they connect."
)
MESSAGES = [{"role": "user", "content": PROMPT_TEXT}]


def _post_chat(endpoint, max_tokens=MAX_TOKENS, request_id=None):
    """Send one chat completion through the deployment's HTTP ingress."""
    host, port = endpoint
    headers = {"X-Request-Id": request_id} if request_id else None
    resp = requests.post(
        f"http://{host}:{port}/v1/chat/completions",
        json={
            "model": MODEL_ID,
            "messages": MESSAGES,
            "max_tokens": max_tokens,
            "temperature": 0.0,
            "ignore_eos": True,
        },
        headers=headers,
        timeout=300,
    )
    assert resp.status_code == 200, resp.text


def _tokenize_chat(endpoint):
    """The engine's exact token ids for the chat-templated prompt."""
    host, port = endpoint
    resp = requests.post(
        f"http://{host}:{port}/tokenize",
        json={"model": MODEL_ID, "messages": MESSAGES, "add_generation_prompt": True},
        timeout=60,
    )
    assert resp.status_code == 200, resp.text
    return resp.json()["tokens"]


def _num_prompt_blocks(token_ids):
    """Number of full KV blocks in a token sequence (matches the indexer)."""
    return len(compute_block_hash_for_seq(list(token_ids), BLOCK_SIZE))


def _restart_serve(ingress_replicas_per_node):
    """Start a fresh controller with the ingress replica count.

    The controller reads RAY_SERVE_INGRESS_ROUTER_REPLICAS_PER_NODE from its
    own environment at creation.
    """
    serve.shutdown()
    serve.start(
        controller_options={
            "runtime_env": {
                "env_vars": {
                    "RAY_SERVE_INGRESS_ROUTER_REPLICAS_PER_NODE": str(
                        ingress_replicas_per_node
                    )
                }
            }
        }
    )


async def _broadcast(router, method, *args):
    return await router.broadcast(method, *args).results_async()


async def _ingress_replica_ids(router, ingress_replicas_per_node):
    """Every ingress replica's id. The per-node setting applies to each proxy
    node, so the total depends on the cluster's shape; read it off the
    broadcast rather than assuming it equals the per-node setting."""
    replica_ids = await _broadcast(router, "get_replica_id")
    assert len(set(replica_ids)) == len(replica_ids)
    assert len(replica_ids) >= ingress_replicas_per_node
    return replica_ids


async def _registered_worker(router, num_replicas):
    """Wait until every ingress replica can schedule the engine's worker and
    return the worker id."""

    async def registered():
        per_replica = await _broadcast(router, "get_registered_worker_ids")
        return len(per_replica) == num_replicas and all(
            len(ids) == 1 for ids in per_replica
        )

    await async_wait_for_condition(registered, timeout=120, retry_interval_ms=1000)
    return (await _broadcast(router, "get_registered_worker_ids"))[0][0]


async def _backend_endpoint(handle):
    """Poll for the replica's backend HTTP (host, port); it is reported
    asynchronously after the replica starts its direct-ingress server."""
    for _ in range(60):
        async with handle.choose_replica() as selection:
            endpoint = selection._replica.backend_http_endpoint
        if endpoint is not None:
            return endpoint
        await asyncio.sleep(1.0)
    raise AssertionError("replica backend HTTP endpoint never became available")


@pytest.mark.parametrize("ingress_replicas_per_node", [1, 2], scope="class")
class TestIngressSynchronization:
    @pytest.fixture(scope="class")
    def deployed_handle(self, ingress_replicas_per_node):
        if not ray.is_initialized():
            ray.init(address="auto")
        _restart_serve(ingress_replicas_per_node=ingress_replicas_per_node)
        llm_config = build_kv_config(
            request_router_class=KVAwareRouter,
            kv_events_port_base=KV_EVENTS_PORT_BASE,
        )
        with patch_ingress():
            handle = serve.run(build_kv_app(llm_config), name=APP_NAME)
        yield handle
        serve.shutdown()

    @pytest.mark.asyncio
    async def test_booked_load(self, ingress_replicas_per_node, deployed_handle):
        """One ingress selects atomically, then every ingress converges through
        booking, prefill completion, and request completion."""
        router = serve.get_deployment_handle("LLMRouter", app_name=APP_NAME)
        replica_ids = await _ingress_replica_ids(router, ingress_replicas_per_node)
        num_ingress_replicas = len(replica_ids)
        worker_id = await _registered_worker(router, num_ingress_replicas)

        loads = await _broadcast(router, "get_worker_load", worker_id)
        assert all(load["active_requests"] == 0 for load in loads)

        token_ids = list(range(64))
        selection = await router.select_worker.remote(
            "probe", token_ids, [worker_id], 32
        )
        assert selection["worker_id"] == worker_id

        async def reservation_converged():
            states = await _broadcast(router, "get_request_lifecycle", "probe")
            loads = await _broadcast(router, "get_worker_load", worker_id)
            return (
                len(states) == num_ingress_replicas
                and all(state is not None for state in states)
                and all(load["active_requests"] == 1 for load in loads)
                and len({load["potential_prefill_tokens"] for load in loads}) == 1
                and loads[0]["potential_prefill_tokens"] > 0
            )

        await async_wait_for_condition(
            reservation_converged, timeout=30, retry_interval_ms=100
        )

        await _broadcast(
            router, "on_lifecycle_events", [("on_prefill_complete", ("probe",))]
        )

        async def prefill_converged():
            states = await _broadcast(router, "get_request_lifecycle", "probe")
            loads = await _broadcast(router, "get_worker_load", worker_id)
            return all(
                state is not None and state["prefill_completed"] for state in states
            ) and all(
                load["active_requests"] == 1 and load["potential_prefill_tokens"] == 0
                for load in loads
            )

        await async_wait_for_condition(
            prefill_converged, timeout=30, retry_interval_ms=100
        )

        await _broadcast(
            router, "on_lifecycle_events", [("on_request_completed", ("probe",))]
        )

        async def completion_converged():
            states = await _broadcast(router, "get_request_lifecycle", "probe")
            loads = await _broadcast(router, "get_worker_load", worker_id)
            return all(state is None for state in states) and all(
                load["active_requests"] == 0 for load in loads
            )

        await async_wait_for_condition(
            completion_converged, timeout=30, retry_interval_ms=100
        )

    @pytest.mark.asyncio
    async def test_real_request_load(self, ingress_replicas_per_node, deployed_handle):
        """A streamed request is eventually booked and freed on every ingress."""
        router = serve.get_deployment_handle("LLMRouter", app_name=APP_NAME)
        num_ingress_replicas = len(
            await _ingress_replica_ids(router, ingress_replicas_per_node)
        )
        worker_id = await _registered_worker(router, num_ingress_replicas)
        endpoint = await _backend_endpoint(deployed_handle)

        # A long generation keeps the request in flight while we observe it.
        request_errors = []

        def post_chat():
            try:
                _post_chat(
                    endpoint,
                    max_tokens=1024,
                    request_id="token-load-inflight-1",
                )
            except Exception as e:
                request_errors.append(e)

        request_thread = threading.Thread(
            target=post_chat,
        )
        request_thread.start()
        try:

            async def is_active():
                loads = await _broadcast(router, "get_worker_load", worker_id)
                return len(loads) == num_ingress_replicas and all(
                    load["active_requests"] >= 1 for load in loads
                )

            await async_wait_for_condition(is_active, timeout=60, retry_interval_ms=200)
        finally:
            request_thread.join(timeout=300)
        assert not request_thread.is_alive()
        if request_errors:
            raise request_errors[0]

        async def is_cleared():
            loads = await _broadcast(router, "get_worker_load", worker_id)
            return all(load["active_requests"] == 0 for load in loads)

        await async_wait_for_condition(is_cleared, timeout=30, retry_interval_ms=500)

    @pytest.mark.asyncio
    async def test_kv_events_indexed(self, ingress_replicas_per_node, deployed_handle):
        """Engine KV-cache events reach every ingress replica's indexer, so all
        replicas score the cached prefix alike."""
        router = serve.get_deployment_handle("LLMRouter", app_name=APP_NAME)
        num_ingress_replicas = len(
            await _ingress_replica_ids(router, ingress_replicas_per_node)
        )
        worker_id = await _registered_worker(router, num_ingress_replicas)

        # Warm the engine's cache; it emits stored-block events every ingress
        # indexer subscribes to.
        endpoint = await _backend_endpoint(deployed_handle)
        _post_chat(endpoint)
        prompt_token_ids = _tokenize_chat(endpoint)
        prompt_blocks = _num_prompt_blocks(prompt_token_ids)
        assert prompt_blocks >= 1

        async def is_indexed():
            per_replica = await _broadcast(
                router, "get_kv_overlap_blocks", prompt_token_ids
            )
            return len(per_replica) == num_ingress_replicas and all(
                overlaps.get(worker_id) == prompt_blocks for overlaps in per_replica
            )

        await async_wait_for_condition(is_indexed, timeout=60, retry_interval_ms=500)


if __name__ == "__main__":
    if not ray.is_initialized():
        ray.init(address="auto")
    sys.exit(pytest.main(["-v", "-s", __file__]))
