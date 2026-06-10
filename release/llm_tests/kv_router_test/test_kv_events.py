import asyncio
import sys
from typing import Dict, List

import pytest
import requests
from dynamo.llm import compute_block_hash_for_seq
from transformers import AutoTokenizer

import ray
from ray import serve
from ray._common.test_utils import async_wait_for_condition
from ray.llm._internal.serve.core.ingress.builder import (
    _build_direct_streaming_llm_deployment,
)
from ray.llm._internal.serve.routing_policies.kv_aware.kv_aware_actor import (
    _MODEL_NAME,
    _TENANT_ID,
    KV_ROUTER_ACTOR_NAME,
    KVRouterActor,
)
from ray.llm._internal.serve.routing_policies.kv_aware.kv_events import (
    configure_kv_events_for_kv_routing,
)
from ray.serve.config import DeploymentActorConfig
from ray.serve.llm import LLMConfig, ModelLoadingConfig
from ray.serve.llm.request_router import KVAwareRouter

from utils import discover_deployment_actor

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


class _TestKVRouterActor(KVRouterActor):
    """KVRouterActor augmented with test-only introspection."""

    async def get_candidate_worker_ids(self) -> List[int]:
        """(Test only) The workers currently tracked from running replicas.

        Async so it runs on the actor's event loop, serialized with
        ``_on_deployment_targets`` which mutates the same map on that loop.
        """
        return sorted(self._replica_id_by_worker)

    def get_kv_event_worker_replicas(self) -> Dict[int, str]:
        """(Test only) The registered Dynamo worker id -> replica full id mapping."""
        return dict(self._replica_id_by_worker)

    def get_registered_worker_ids(self) -> List[int]:
        """(Test only) Worker ids the selection service can currently schedule."""
        if self._svc is None:
            return []
        workers = self._svc.list_workers(model_name=_MODEL_NAME, tenant_id=_TENANT_ID)
        return sorted(
            w["worker_id"] for w in workers if w["lifecycle"] == "schedulable"
        )

    async def get_kv_overlap_blocks(self, token_ids: List[int]) -> Dict[int, int]:
        """(Test only) Per-worker device-tier KV overlap blocks for a token sequence.

        Returns the number of leading blocks of ``token_ids`` each worker has cached.
        """
        if self._svc is None:
            return {}
        scores = await self._svc.overlap_scores(
            {
                "model_name": _MODEL_NAME,
                "tenant_id": _TENANT_ID,
                "token_ids": list(token_ids),
            }
        )
        return {w["worker_id"]: w["device_blocks"] for w in scores["workers"]}


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


def tokenize_text(endpoint, prompt):
    """Token ids for a raw prompt string via the completion /tokenize path.

    add_special_tokens is False because a chat-templated string already carries
    the template's special tokens as text.
    """
    host, port = endpoint
    response = requests.post(
        f"http://{host}:{port}/tokenize",
        json={"model": MODEL_ID, "prompt": prompt, "add_special_tokens": False},
        timeout=60,
    )
    assert response.status_code == 200, response.text
    return response.json()["tokens"]


def num_prompt_blocks(token_ids):
    """Number of full KV blocks in a token sequence."""
    return len(compute_block_hash_for_seq(list(token_ids), BLOCK_SIZE))


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
                        actor_class=ray.remote(_TestKVRouterActor),
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
    async def test_kv_events_reach_selection_service(self, deployed_handle):
        """Each replica's real engine KV events reach the selection service via
        its connect-out listener, a per-worker prefix-cache reset is observed as
        reduced overlap, and scoring routes the prompt to the higher-overlap
        worker."""
        actor = discover_deployment_actor(
            APP_NAME, deployed_handle.deployment_name, KV_ROUTER_ACTOR_NAME
        )
        assert actor is not None, "KV router actor was not discoverable"

        replica_endpoints = await self._discover_replicas(deployed_handle)

        # Each replica advertises its KV-events endpoint via record_routing_stats;
        # the controller propagates it on the LongPoll replica snapshot and the
        # actor registers the worker with the selection service. Wait for every
        # replica to be registered (the controller polls routing stats on an
        # interval, so this is not synchronous with replica startup).
        async def all_replicas_registered():
            replica_by_worker = await actor.get_kv_event_worker_replicas.remote()
            return sorted(replica_by_worker.values()) == sorted(replica_endpoints)

        await async_wait_for_condition(all_replicas_registered, timeout=90)

        replica_by_worker = await actor.get_kv_event_worker_replicas.remote()
        endpoints = {
            worker_id: replica_endpoints[replica_id]
            for worker_id, replica_id in replica_by_worker.items()
        }
        worker_ids = sorted(endpoints)
        assert await actor.get_candidate_worker_ids.remote() == worker_ids
        assert await actor.get_registered_worker_ids.remote() == worker_ids

        # The same prompt on each replica caches the same content.
        usages = {}
        for worker_id in worker_ids:
            usages[worker_id] = post_chat(endpoints[worker_id])["usage"]

        prompt_token_ids = tokenize_prompt(endpoints[worker_ids[0]])
        prompt_blocks = num_prompt_blocks(prompt_token_ids)
        assert prompt_blocks >= 2

        # The engines' KV events reached the indexer: full prompt overlap is
        # scored on both workers.
        async def both_workers_fully_overlap():
            overlaps = await actor.get_kv_overlap_blocks.remote(prompt_token_ids)
            return all(overlaps.get(w) == prompt_blocks for w in worker_ids)

        await async_wait_for_condition(both_workers_fully_overlap, timeout=60)

        for worker_id in worker_ids:
            usage = usages[worker_id]
            assert usage["prompt_tokens"] == len(prompt_token_ids)
            assert usage["completion_tokens"] == MAX_TOKENS

        # /reset_prefix_cache clears only this worker's view; the engine drains
        # queued KV events on scheduler steps, so a small follow-up request
        # flushes the AllBlocksCleared event to the listener.
        reset_worker, untouched_worker = worker_ids
        host, port = endpoints[reset_worker]
        response = requests.post(f"http://{host}:{port}/reset_prefix_cache", timeout=60)
        assert response.status_code == 200, response.text
        post_chat(endpoints[reset_worker], messages=FLUSH_MESSAGES, max_tokens=2)

        # The reset worker's overlap falls back to the chat-template prefix the
        # two prompts share; the untouched worker keeps the full prompt.
        flush_token_ids = tokenize_prompt(endpoints[reset_worker], FLUSH_MESSAGES)
        diverge = next(
            (
                i
                for i, (a, b) in enumerate(zip(prompt_token_ids, flush_token_ids))
                if a != b
            ),
            min(len(prompt_token_ids), len(flush_token_ids)),
        )
        shared_blocks = diverge // BLOCK_SIZE

        async def reset_worker_cleared():
            overlaps = await actor.get_kv_overlap_blocks.remote(prompt_token_ids)
            return overlaps.get(reset_worker, 0) == shared_blocks

        await async_wait_for_condition(reset_worker_cleared, timeout=60)
        overlaps = await actor.get_kv_overlap_blocks.remote(prompt_token_ids)
        assert overlaps.get(untouched_worker) == prompt_blocks

        # Scoring routes the prompt to the worker holding more cached overlap.
        selection = await actor.select_worker.remote(
            "score-req", prompt_token_ids, worker_ids
        )
        assert selection["worker_id"] == untouched_worker

    @pytest.mark.asyncio
    @pytest.mark.timeout(600)
    async def test_chat_tokens_match_prefill(self, deployed_handle):
        """Ensure chat template is applied: a chat request scores the same overlap as
        the prompt rendered with the model's chat template and tokenized as raw text."""
        actor = discover_deployment_actor(
            APP_NAME, deployed_handle.deployment_name, KV_ROUTER_ACTOR_NAME
        )
        assert actor is not None
        replica_endpoints = await self._discover_replicas(deployed_handle)

        async def all_registered():
            registered = await actor.get_kv_event_worker_replicas.remote()
            return sorted(registered.values()) == sorted(replica_endpoints)

        await async_wait_for_condition(all_registered, timeout=90)

        # Ground truth: render the chat template client-side and tokenize as text.
        worker_id, replica_id = next(
            iter((await actor.get_kv_event_worker_replicas.remote()).items())
        )
        endpoint = replica_endpoints[replica_id]
        manual_prompt = AutoTokenizer.from_pretrained(MODEL_SOURCE).apply_chat_template(
            MESSAGES, add_generation_prompt=True, tokenize=False
        )
        manual_token_ids = tokenize_text(endpoint, manual_prompt)
        prompt_blocks = num_prompt_blocks(manual_token_ids)
        assert prompt_blocks >= 2

        # Warm this worker's prefix cache with the chat request and wait until the
        # indexer reflects the manually-templated prompt's blocks.
        post_chat(endpoint)

        async def manual_fully_overlaps():
            overlaps = await actor.get_kv_overlap_blocks.remote(manual_token_ids)
            return overlaps.get(worker_id) == prompt_blocks

        await async_wait_for_condition(manual_fully_overlaps, timeout=60)

        # The chat /tokenize tokens hit the same cached blocks -> same score,
        # proving /tokenize applied the chat template.
        chat_token_ids = tokenize_prompt(endpoint, MESSAGES)
        chat_overlaps = await actor.get_kv_overlap_blocks.remote(chat_token_ids)
        assert chat_overlaps.get(worker_id) == prompt_blocks
        assert chat_token_ids == manual_token_ids


class TestKvScoring:
    """End-to-end KV-aware routing: a request routed through a deployed
    KVAwareRouter is scored by the selection service and lands on a live
    replica."""

    @pytest.fixture(scope="class")
    def kv_aware_handle(self):
        """Deploy with KVAwareRouter; the build auto-attaches the KVRouterActor
        and enables engine KV events."""
        if not ray.is_initialized():
            ray.init(address="auto")
        serve.shutdown()

        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id=MODEL_ID, model_source=MODEL_SOURCE
            ),
            deployment_config=dict(
                autoscaling_config=dict(
                    min_replicas=NUM_REPLICAS, max_replicas=NUM_REPLICAS
                ),
                request_router_config={"request_router_class": KVAwareRouter},
            ),
            engine_kwargs=dict(
                max_model_len=2048,
                enforce_eager=True,
                gpu_memory_utilization=0.4,
            ),
            experimental_configs={"KV_EVENTS_PORT_BASE": 21600},
            runtime_env=dict(
                env_vars={
                    "RAY_SERVE_ENABLE_DIRECT_INGRESS": "1",
                    "RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING": "1",
                }
            ),
            log_engine_metrics=False,
        )
        app = _build_direct_streaming_llm_deployment(llm_config)
        handle = serve.run(app, name="kv_scoring_gpu_test")
        yield handle
        serve.shutdown()

    @pytest.mark.asyncio
    @pytest.mark.timeout(600)
    async def test_routes_to_higher_overlap_replica(self, kv_aware_handle):
        """An overlapping prompt routes back to the replica that cached it,
        scored through the full KVAwareRouter path."""
        async with kv_aware_handle.choose_replica(
            request_body=b"",
            body_truncated=False,
            _reserve=False,
            request_token_ids=[1],  # KV-aware routing requires token ids
        ) as selection:
            cached_id = selection._replica.replica_id.to_full_id_str()
            cached_endpoint = selection._replica.backend_http_endpoint
        post_chat(cached_endpoint)
        prompt_token_ids = tokenize_prompt(cached_endpoint)
        assert num_prompt_blocks(prompt_token_ids) >= 2

        # Worker registration and KV-event indexing are asynchronous, so poll the
        # scoring path until it converges on the replica holding the cached blocks.
        async def routes_to_cached_replica():
            picks = set()
            for _ in range(3):
                async with kv_aware_handle.choose_replica(
                    request_body=b"",
                    body_truncated=False,
                    _reserve=False,
                    request_token_ids=prompt_token_ids,
                ) as selection:
                    picks.add(selection._replica.replica_id.to_full_id_str())
            return picks == {cached_id}

        await async_wait_for_condition(routes_to_cached_replica, timeout=120)


if __name__ == "__main__":
    if not ray.is_initialized():
        ray.init(address="auto")
    sys.exit(pytest.main(["-v", "-s", __file__]))
