import asyncio
import sys

import pytest
import requests
from dynamo.llm import compute_block_hash_for_seq
from transformers import AutoTokenizer

import ray
from ray import serve
from ray._common.test_utils import async_wait_for_condition
from ray.serve.config import RequestRouterConfig
from ray.serve.llm import LLMConfig, ModelLoadingConfig, build_openai_app
from ray.serve.llm.request_router import KVAwareRouter

from utils import (
    _TestKVAwareRouter,
    build_kv_app,
    discover_replica_endpoints,
    patch_ingress,
)

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
OFFLOAD_MESSAGES = [
    {
        "role": "user",
        "content": (
            "Remember this native KV offload target exactly. "
            + ("alpha beta gamma delta epsilon zeta eta theta iota kappa " "lambda mu ")
            * 20
        ),
    }
]


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
                # This test validates the KV-events plane (engine events ->
                # selection service indexer), not routing: requests are sent
                # directly to each replica's endpoint, so this subclass borrows
                # RoundRobinRouter's selection purely so replica discovery can
                # enumerate both replicas.
                request_router_config=RequestRouterConfig(
                    request_router_class=_TestKVAwareRouter
                ),
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
        # Swap the ingress for the introspection LLMRouter so the embedded
        # tracker's state is reachable over the deployment handle.
        with patch_ingress():
            app = build_kv_app(llm_config)
            handle = serve.run(app, name=APP_NAME)
        yield handle
        serve.shutdown()

    @pytest.mark.asyncio
    @pytest.mark.timeout(600)
    async def test_kv_events_reach_selection_service(self, deployed_handle):
        """Each replica's real engine KV events reach the selection service via
        its connect-out listener, a per-worker prefix-cache reset is observed as
        reduced overlap, and scoring routes the prompt to the higher-overlap
        worker."""
        router = serve.get_deployment_handle("LLMRouter", app_name=APP_NAME)

        replica_endpoints = await discover_replica_endpoints(
            deployed_handle, NUM_REPLICAS
        )

        # Each replica advertises its KV-events endpoint via record_routing_stats;
        # the controller propagates it on the LongPoll replica snapshot and the
        # tracker registers the worker with the selection service. Wait for every
        # replica to be registered (the controller polls routing stats on an
        # interval, so this is not synchronous with replica startup).
        async def all_replicas_registered():
            replica_by_worker = await router.get_kv_event_worker_replicas.remote()
            return sorted(replica_by_worker.values()) == sorted(replica_endpoints)

        await async_wait_for_condition(all_replicas_registered, timeout=90)

        replica_by_worker = await router.get_kv_event_worker_replicas.remote()
        endpoints = {
            worker_id: replica_endpoints[replica_id]
            for worker_id, replica_id in replica_by_worker.items()
        }
        worker_ids = sorted(endpoints)
        assert await router.get_candidate_worker_ids.remote() == worker_ids
        assert await router.get_registered_worker_ids.remote() == worker_ids

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
            overlaps = await router.get_kv_overlap_blocks.remote(prompt_token_ids)
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
            overlaps = await router.get_kv_overlap_blocks.remote(prompt_token_ids)
            return overlaps.get(reset_worker, 0) == shared_blocks

        await async_wait_for_condition(reset_worker_cleared, timeout=60)
        overlaps = await router.get_kv_overlap_blocks.remote(prompt_token_ids)
        assert overlaps.get(untouched_worker) == prompt_blocks

        # Scoring routes the prompt to the worker holding more cached overlap.
        selection = await router.select_worker.remote(
            "score-req", prompt_token_ids, worker_ids
        )
        assert selection["worker_id"] == untouched_worker

    @pytest.mark.asyncio
    @pytest.mark.timeout(600)
    async def test_chat_tokens_match_prefill(self, deployed_handle):
        """Ensure chat template is applied: a chat request scores the same overlap as
        the prompt rendered with the model's chat template and tokenized as raw text."""
        router = serve.get_deployment_handle("LLMRouter", app_name=APP_NAME)
        replica_endpoints = await discover_replica_endpoints(
            deployed_handle, NUM_REPLICAS
        )

        async def all_registered():
            registered = await router.get_kv_event_worker_replicas.remote()
            return sorted(registered.values()) == sorted(replica_endpoints)

        await async_wait_for_condition(all_registered, timeout=90)

        # Ground truth: render the chat template client-side and tokenize as text.
        worker_id, replica_id = next(
            iter((await router.get_kv_event_worker_replicas.remote()).items())
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
            overlaps = await router.get_kv_overlap_blocks.remote(manual_token_ids)
            return overlaps.get(worker_id) == prompt_blocks

        await async_wait_for_condition(manual_fully_overlaps, timeout=60)

        # The chat /tokenize tokens hit the same cached blocks -> same score,
        # proving /tokenize applied the chat template.
        chat_token_ids = tokenize_prompt(endpoint, MESSAGES)
        chat_overlaps = await router.get_kv_overlap_blocks.remote(chat_token_ids)
        assert chat_overlaps.get(worker_id) == prompt_blocks
        assert chat_token_ids == manual_token_ids


class TestKvOffload:
    """End-to-end native vLLM CPU offload with tier-aware routing."""

    @pytest.fixture(scope="class")
    def deployed_handle(self):
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
                request_router_config=RequestRouterConfig(
                    request_router_class=KVAwareRouter
                ),
            ),
            engine_kwargs=dict(
                enable_prefix_caching=True,
                enable_prompt_tokens_details=True,
                enable_force_include_usage=True,
                enforce_eager=True,
                gpu_memory_utilization=0.4,
                kv_offloading_backend="native",
                kv_offloading_size=1.0,
                max_model_len=512,
                num_gpu_blocks_override=32,
            ),
            experimental_configs={"KV_EVENTS_PORT_BASE": 21700},
            runtime_env=dict(
                env_vars={
                    "RAY_SERVE_ENABLE_DIRECT_INGRESS": "1",
                    "RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING": "1",
                }
            ),
            log_engine_metrics=False,
        )
        with patch_ingress():
            app = build_kv_app(llm_config)
            handle = serve.run(app, name="kv_offload_gpu_test")
        yield handle
        serve.shutdown()

    @pytest.mark.asyncio
    @pytest.mark.timeout(600)
    async def test_offload_routes_to_cpu_prefix_and_reloads(self, deployed_handle):
        """A GPU-evicted CPU prefix stays routable and reloads on its replica."""
        router = serve.get_deployment_handle(
            "LLMRouter", app_name="kv_offload_gpu_test"
        )
        replica_endpoints = await discover_replica_endpoints(
            deployed_handle, NUM_REPLICAS
        )

        async def all_registered():
            registered = await router.get_kv_event_worker_replicas.remote()
            return sorted(registered.values()) == sorted(replica_endpoints)

        await async_wait_for_condition(all_registered, timeout=90)
        replica_by_worker = await router.get_kv_event_worker_replicas.remote()
        endpoints = {
            worker_id: replica_endpoints[replica_id]
            for worker_id, replica_id in replica_by_worker.items()
        }
        cached_worker, miss_worker = sorted(endpoints)
        target_tokens = tokenize_prompt(endpoints[cached_worker], OFFLOAD_MESSAGES)
        target_blocks = num_prompt_blocks(target_tokens)
        assert 4 < target_blocks < 32

        post_chat(endpoints[cached_worker], OFFLOAD_MESSAGES, max_tokens=2)

        async def target_is_on_gpu():
            scores = await router.get_kv_overlap_scores.remote(target_tokens)
            return scores.get(cached_worker, {}).get("device_blocks") == target_blocks

        await async_wait_for_condition(target_is_on_gpu, timeout=60)

        # Each unique prompt displaces old GPU blocks. Native offload retains
        # the target in CPU memory and emits CPU-tier events as that happens.
        async def target_is_only_on_cpu():
            scores = await router.get_kv_overlap_scores.remote(target_tokens)
            cached_score = scores.get(cached_worker, {})
            return (
                cached_score.get("device_blocks") == 0
                and cached_score.get("host_pinned_blocks", 0) > 0
            )

        offloaded = False
        for i in range(12):
            filler = [
                {
                    "role": "user",
                    "content": (
                        f"Unique GPU eviction sequence {i}. " + f"filler-{i} " * 60
                    ),
                }
            ]
            post_chat(endpoints[cached_worker], filler, max_tokens=1)
            for _ in range(10):
                if await target_is_only_on_cpu():
                    offloaded = True
                    break
                await asyncio.sleep(0.5)
            if offloaded:
                break
        if not offloaded:
            raise AssertionError("Target prefix was not offloaded from GPU to CPU.")

        scores = await router.get_kv_overlap_scores.remote(target_tokens)
        cached_score = scores[cached_worker]
        miss_score = scores[miss_worker]
        assert cached_score["host_pinned_extension_blocks"] > 0
        assert (
            0
            < cached_score["router_credit_blocks"]
            < cached_score["host_pinned_blocks"]
        )
        assert miss_score["device_blocks"] == 0
        assert miss_score["host_pinned_blocks"] == 0

        # Exercise the production HAProxy -> LLMRouter -> KVAwareRouter path.
        # The response can only report a cache hit if it reached the replica
        # whose target prefix is still available in CPU memory.
        response = post_chat(("127.0.0.1", 8000), OFFLOAD_MESSAGES, max_tokens=2)
        assert response["usage"]["prompt_tokens_details"]["cached_tokens"] > 0

        # The request was served from CPU and the loaded blocks are visible on
        # GPU again on that replica, while the uncached replica remains empty.
        await async_wait_for_condition(target_is_on_gpu, timeout=60)
        scores = await router.get_kv_overlap_scores.remote(target_tokens)
        assert scores[miss_worker]["device_blocks"] == 0
        assert scores[miss_worker]["host_pinned_blocks"] == 0


class TestKvScoring:
    """End-to-end KV-aware routing: a request routed through a deployed
    KVAwareRouter is scored by the selection service and lands on a live
    replica."""

    @pytest.fixture(scope="class")
    def kv_aware_handle(self):
        """Deploy with KVAwareRouter; the ingress builds the KVTokenTracker
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
                request_router_config=RequestRouterConfig(
                    request_router_class=KVAwareRouter
                ),
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
        app = build_kv_app(llm_config)
        handle = serve.run(app, name="kv_scoring_gpu_test")
        yield handle
        serve.shutdown()

    @pytest.mark.asyncio
    @pytest.mark.timeout(600)
    async def test_routes_to_higher_overlap_replica(self, kv_aware_handle):
        """An overlapping prompt routes back to the replica that cached it,
        scored through the full KVAwareRouter path."""
        async with kv_aware_handle.choose_replica(
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
                    _reserve=False,
                    request_token_ids=prompt_token_ids,
                ) as selection:
                    picks.add(selection._replica.replica_id.to_full_id_str())
            return picks == {cached_id}

        await async_wait_for_condition(routes_to_cached_replica, timeout=120)


class TestFastokens:
    @pytest.fixture(scope="class")
    def fastokens_handle(self):
        if not ray.is_initialized():
            ray.init(address="auto")
        serve.shutdown()

        llm_config = LLMConfig(
            model_loading_config=dict(
                model_id="qwen3-0.6b",
                model_source="Qwen/Qwen3-0.6B",
            ),
            runtime_env=dict(env_vars={"VLLM_USE_FASTOKENS": "1"}),
            deployment_config=dict(
                autoscaling_config=dict(min_replicas=1, max_replicas=2),
                request_router_config=dict(request_router_class=KVAwareRouter),
            ),
        )
        serve.run(
            build_openai_app({"llm_configs": [llm_config]}),
            name="fastokens_test",
        )
        yield
        serve.shutdown()

    @pytest.mark.timeout(600)
    @pytest.mark.parametrize(
        "path,payload",
        [
            pytest.param(
                "/v1/chat/completions",
                {
                    "model": "qwen3-0.6b",
                    "messages": [{"role": "user", "content": "Say hello."}],
                    "max_tokens": 8,
                },
                id="chat",
            ),
            pytest.param(
                "/v1/completions",
                {
                    "model": "qwen3-0.6b",
                    "prompt": "Say hello.",
                    "max_tokens": 8,
                },
                id="completion",
            ),
        ],
    )
    def test_fastokens_with_pre_routing_tokenization(
        self, fastokens_handle, path, payload
    ):
        response = requests.post(
            f"http://localhost:8000{path}", json=payload, timeout=120
        )
        assert response.status_code == 200, response.text


if __name__ == "__main__":
    if not ray.is_initialized():
        ray.init(address="auto")
    sys.exit(pytest.main(["-v", "-s", __file__]))
