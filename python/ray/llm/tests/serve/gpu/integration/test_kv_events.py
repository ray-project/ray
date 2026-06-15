import asyncio
import sys

import pytest
import requests
from dynamo.llm import compute_block_hash_for_seq

import ray
from ray import serve
from ray._common.test_utils import async_wait_for_condition
from ray.llm._internal.serve.core.ingress.builder import (
    _build_direct_streaming_llm_deployment,
)
from ray.llm._internal.serve.routing_policies.kv_aware.kv_aware_actor import (
    KV_ROUTER_ACTOR_NAME,
    KVRouterActor,
)
from ray.llm._internal.serve.routing_policies.kv_aware.kv_events import (
    configure_kv_events_for_kv_routing,
)
from ray.serve._private.constants import (
    SERVE_DEPLOYMENT_ACTOR_PREFIX,
    SERVE_NAMESPACE,
)
from ray.serve.config import DeploymentActorConfig
from ray.serve.llm import LLMConfig, ModelLoadingConfig

MODEL_ID = "qwen3-0.6b"
MODEL_SOURCE = "Qwen/Qwen3-0.6B"
APP_NAME = "kv_events_gpu_test"
NUM_REPLICAS = 2
BLOCK_SIZE = 16
MAX_TOKENS = 50
# MESSAGES and FLUSH_MESSAGES share a long prefix (two full 16-token blocks) so
# the reset test asserts a *partial* overlap fallback: after one replica's prefix
# cache is cleared and re-warmed with FLUSH_MESSAGES, its overlap for MESSAGES
# drops to just the shared prefix blocks while the untouched replica keeps the
# full prompt.
_SHARED_PREFIX = (
    "Repeat the following sentence exactly five times in a row, word for word, "
    "without adding anything else at all: the quick brown fox jumps over the lazy dog"
)
MESSAGES = [
    {
        "role": "user",
        "content": (
            _SHARED_PREFIX
            + " near the calm river bank today under a wide clear evening sky "
            "over the hills."
        ),
    }
]
FLUSH_MESSAGES = [
    {"role": "user", "content": _SHARED_PREFIX + " beside the tall fence."}
]


def discover_deployment_actor(app_name, deployment_name, actor_name):
    """Resolve a deployment-scoped actor by its registered name."""
    prefix = f"{SERVE_DEPLOYMENT_ACTOR_PREFIX}{app_name}::{deployment_name}::"
    suffix = f"::{actor_name}"
    for entry in ray.util.list_named_actors(all_namespaces=True):
        name = entry.get("name") or ""
        if (
            entry.get("namespace") == SERVE_NAMESPACE
            and name.startswith(prefix)
            and name.endswith(suffix)
        ):
            return ray.get_actor(name, namespace=SERVE_NAMESPACE)
    return None


def post_chat(endpoint, messages=MESSAGES, max_tokens=MAX_TOKENS):
    host, port = endpoint
    response = requests.post(
        f"http://{host}:{port}/v1/chat/completions",
        json={
            "model": MODEL_ID,
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": 0.0,
            "ignore_eos": True,
        },
        timeout=120,
    )
    assert response.status_code == 200, response.text
    return response.json()


def tokenize_prompt(endpoint, messages=MESSAGES):
    """The engine's exact token ids for a chat-templated prompt."""
    host, port = endpoint
    response = requests.post(
        f"http://{host}:{port}/tokenize",
        json={"model": MODEL_ID, "messages": messages, "add_generation_prompt": True},
        timeout=60,
    )
    assert response.status_code == 200, response.text
    return response.json()["tokens"]


def prompt_block_hashes(token_ids):
    """Dynamo's content hashes for the full blocks of a token sequence."""
    return compute_block_hash_for_seq(list(token_ids), BLOCK_SIZE)


def stored_tokens_hashes(indexer_events, worker_id):
    """Dynamo per-block token hashes stored for a worker in the indexer dump."""
    return {
        block["tokens_hash"]
        for entry in indexer_events
        if entry["worker_id"] == worker_id
        for block in entry["event"]["data"]["stored"]["blocks"]
    }


class TestKvEvents:
    @pytest.fixture(scope="class")
    def deployed_handle(self):
        """Deploy two direct-streaming LLMServer replicas with KV events on."""
        if not ray.is_initialized():
            ray.init(address="auto")
        serve.shutdown()

        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id=MODEL_ID,
                model_source=MODEL_SOURCE,
            ),
            deployment_config=dict(
                autoscaling_config=dict(
                    min_replicas=NUM_REPLICAS, max_replicas=NUM_REPLICAS
                ),
                deployment_actors=[
                    DeploymentActorConfig(
                        name=KV_ROUTER_ACTOR_NAME,
                        actor_class=KVRouterActor,
                        actor_options={"num_cpus": 0},
                        init_kwargs={"block_size": BLOCK_SIZE},
                    )
                ],
            ),
            engine_kwargs=dict(
                max_model_len=2048,
                enforce_eager=True,
                gpu_memory_utilization=0.4,  # small model on a shared GPU
            ),
            experimental_configs={"KV_EVENTS_PORT_BASE": 21557},
            runtime_env=dict(
                env_vars={
                    "RAY_SERVE_ENABLE_DIRECT_INGRESS": "1",
                    "RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING": "1",
                    # /reset_prefix_cache is a vLLM dev-mode endpoint.
                    "VLLM_SERVER_DEV_MODE": "1",
                },
            ),
            log_engine_metrics=False,
        )
        configure_kv_events_for_kv_routing(llm_config)

        app = _build_direct_streaming_llm_deployment(llm_config)
        handle = serve.run(app, name=APP_NAME)
        yield handle
        serve.shutdown()

    async def _discover_replicas(self, handle):
        """Map each replica's full id to its backend HTTP endpoint."""
        endpoints = {}
        for _ in range(100):
            async with handle.choose_replica() as selection:
                replica = selection._replica
                if replica.backend_http_endpoint is not None:
                    replica_id = replica.replica_id.to_full_id_str()
                    endpoints[replica_id] = replica.backend_http_endpoint
            if len(endpoints) == NUM_REPLICAS:
                return endpoints
            await asyncio.sleep(0.5)
        raise AssertionError(
            f"Expected {NUM_REPLICAS} replicas with backend endpoints, "
            f"found {len(endpoints)}."
        )

    @pytest.mark.asyncio
    @pytest.mark.timeout(600)
    async def test_kv_events_reach_router_actor(self, deployed_handle):
        actor = discover_deployment_actor(
            APP_NAME, deployed_handle.deployment_name, KV_ROUTER_ACTOR_NAME
        )
        assert actor is not None, "KV router actor was not discoverable"

        replica_endpoints = await self._discover_replicas(deployed_handle)

        # Every replica registered its Dynamo worker identity on startup.
        replica_by_worker = await actor.get_kv_event_worker_replicas.remote()
        assert sorted(replica_by_worker.values()) == sorted(replica_endpoints)
        endpoints = {
            worker_id: replica_endpoints[replica_id]
            for worker_id, replica_id in replica_by_worker.items()
        }
        worker_ids = sorted(endpoints)
        # The Ray-supplied worker ids agree with LongPoll replica tracking.
        assert await actor.get_candidate_worker_ids.remote() == worker_ids

        # The same prompt on each replica caches the same content.
        usages = {}
        for worker_id in worker_ids:
            usages[worker_id] = post_chat(endpoints[worker_id])["usage"]

        prompt_token_ids = tokenize_prompt(endpoints[worker_ids[0]])
        expected_hashes = prompt_block_hashes(prompt_token_ids)
        num_prompt_blocks = len(expected_hashes)
        assert num_prompt_blocks >= 2

        # The engines' events reached the indexer: full prompt overlap
        # scored on both workers.
        async def both_workers_fully_overlap():
            overlaps = await actor.get_kv_overlap_blocks.remote(prompt_token_ids)
            return {
                worker_id: blocks
                for worker_id, blocks in overlaps.items()
                if blocks == num_prompt_blocks
            }.keys() == set(worker_ids)

        await async_wait_for_condition(both_workers_fully_overlap, timeout=30)
        events = await actor.get_kv_indexer_events.remote()
        assert sorted({e["worker_id"] for e in events}) == worker_ids

        # Each worker's chain holds the prompt's exact hashes plus decode
        # blocks. The block filling on the last step may not commit, and
        # decode events trail the response, so wait for total - 1.
        total_blocks = (len(prompt_token_ids) + MAX_TOKENS) // BLOCK_SIZE

        async def all_blocks_indexed():
            events = await actor.get_kv_indexer_events.remote()
            return all(
                len(stored_tokens_hashes(events, worker_id)) >= total_blocks - 1
                for worker_id in worker_ids
            )

        await async_wait_for_condition(all_blocks_indexed, timeout=30)
        events = await actor.get_kv_indexer_events.remote()
        for worker_id in worker_ids:
            usage = usages[worker_id]
            assert usage["prompt_tokens"] == len(prompt_token_ids)
            assert usage["completion_tokens"] == MAX_TOKENS

            hashes = stored_tokens_hashes(events, worker_id)
            assert set(expected_hashes) <= hashes
            assert len(hashes) in (total_blocks, total_blocks - 1)

        # /reset_prefix_cache clears only this worker's view; the engine
        # drains queued KV events on scheduler steps, so a small follow-up
        # request flushes the AllBlocksCleared event.
        reset_worker, untouched_worker = worker_ids
        host, port = endpoints[reset_worker]
        response = requests.post(f"http://{host}:{port}/reset_prefix_cache", timeout=60)
        assert response.status_code == 200, response.text
        flush_messages = FLUSH_MESSAGES
        post_chat(endpoints[reset_worker], messages=flush_messages, max_tokens=2)

        # The reset worker's overlap falls back to the chat-template prefix
        # the two prompts share.
        flush_token_ids = tokenize_prompt(endpoints[reset_worker], flush_messages)
        shared_prefix = next(
            (
                i
                for i, (a, b) in enumerate(zip(prompt_token_ids, flush_token_ids))
                if a != b
            ),
            min(len(prompt_token_ids), len(flush_token_ids)),
        )
        shared_blocks = shared_prefix // BLOCK_SIZE

        async def reset_worker_cleared():
            overlaps = await actor.get_kv_overlap_blocks.remote(prompt_token_ids)
            # A fully-cleared worker has no overlap, so it is absent from the dict.
            return overlaps.get(reset_worker, 0) == shared_blocks

        await async_wait_for_condition(reset_worker_cleared, timeout=30)
        overlaps = await actor.get_kv_overlap_blocks.remote(prompt_token_ids)
        assert overlaps.get(untouched_worker) == num_prompt_blocks


if __name__ == "__main__":
    if not ray.is_initialized():
        ray.init(address="auto")
    sys.exit(pytest.main(["-v", "-s", __file__]))
