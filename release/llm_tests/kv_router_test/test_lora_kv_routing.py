import sys

import pytest
import requests
from dynamo.llm import compute_block_hash_for_seq

import ray
from ray import serve
from ray._common.test_utils import async_wait_for_condition
from ray.serve._private.constants import SERVE_MULTIPLEXED_MODEL_ID
from ray.serve._private.test_utils import get_application_url
from ray.serve.config import RequestRouterConfig
from ray.serve.llm import LLMConfig, LoraConfig, ModelLoadingConfig
from ray.serve.llm.request_router import KVAwareRouter

from utils import (
    build_kv_app,
    discover_replica_endpoints,
    patch_ingress,
)

APP_NAME = "lora_kv_routing_test"
NUM_REPLICAS = 2
BLOCK_SIZE = 16
MAX_TOKENS = 4

BASE_MODEL_ID = "llama-216m-dummy"
MODEL_SOURCE = "s3://anonymous@air-example-data/llama-3.2-216M-dummy/"
ADAPTER_SOURCE = "s3://anonymous@air-example-data/llama-3.2-216M-lora-dummy-multi"
MAX_LORA_RANK = 16

ADAPTER_A = f"{BASE_MODEL_ID}:adapter-a"
ADAPTER_B = f"{BASE_MODEL_ID}:adapter-b"

# Long enough to span several full KV blocks, so a prefix-cache hit on it is
# unambiguous. Both adapters send this same prompt: identical tokens under
# different adapter names is exactly the collision the LoRA salt must prevent.
PROMPT = (
    "Describe the water cycle in full detail, covering evaporation, "
    "condensation, precipitation and collection. Give a concrete real-world "
    "example of each of the four stages, say where on Earth it is easiest to "
    "observe, and explain how each stage feeds the next one."
)
# A prompt neither replica has seen, for the routing decision that cannot lean
# on KV overlap.
UNSEEN_PROMPT = (
    "Explain how a bicycle derailleur shifts the chain across the cassette, "
    "step by step, what tension the rear cage applies while it does, and why "
    "the chain does not fall off between the sprockets."
)


def post_completion(url, model, prompt=PROMPT, max_tokens=MAX_TOKENS):
    """Complete ``prompt`` against ``url``.

    A replica's direct-ingress URL pins the prefill to that replica; the
    application URL goes through routing. HAProxy sets the multiplexed model ID
    header itself, so sending it only matters on a direct URL.
    """
    response = requests.post(
        f"{url}/v1/completions",
        headers={SERVE_MULTIPLEXED_MODEL_ID: model} if model != BASE_MODEL_ID else {},
        json={
            "model": model,
            "prompt": prompt,
            "max_tokens": max_tokens,
            "temperature": 0.0,
        },
        timeout=120,
    )
    assert response.status_code == 200, response.text
    return response.json()


def cached_tokens(response):
    """Prompt tokens the engine served out of its prefix cache."""
    return response["usage"]["prompt_tokens_details"]["cached_tokens"]


def tokenize(url, prompt=PROMPT):
    """The engine's exact token IDs for ``prompt``.

    An adapter does not change tokenization, so the base model ID works even on
    a replica that has not loaded the adapter.
    """
    response = requests.post(
        f"{url}/tokenize",
        json={"model": BASE_MODEL_ID, "prompt": prompt},
        timeout=60,
    )
    assert response.status_code == 200, response.text
    return response.json()["tokens"]


def num_prompt_blocks(token_ids):
    """Number of full KV blocks in a token sequence (matches the indexer)."""
    return len(compute_block_hash_for_seq(list(token_ids), BLOCK_SIZE))


async def registered_endpoints(handle, router):
    """Map each registered worker ID to its replica's direct-ingress URL.

    Replicas advertise their KV-events endpoint through routing stats, so the
    tracker registers their workers some time after the replicas are up.
    """
    replica_endpoints = await discover_replica_endpoints(handle, NUM_REPLICAS)

    async def all_replicas_registered():
        replica_by_worker = await router.get_kv_event_worker_replicas.remote()
        return sorted(replica_by_worker.values()) == sorted(replica_endpoints)

    await async_wait_for_condition(all_replicas_registered, timeout=180)
    replica_by_worker = await router.get_kv_event_worker_replicas.remote()

    def url(replica_id):
        host, port = replica_endpoints[replica_id]
        return f"http://{host}:{port}"

    return {
        worker_id: url(replica_id)
        for worker_id, replica_id in replica_by_worker.items()
    }


class TestLoraKvRouting:
    @pytest.fixture(scope="class")
    def deployed_handle(self):
        """Two direct-streaming LoRA replicas behind a KVAwareRouter."""
        if not ray.is_initialized():
            ray.init(address="auto")
        serve.shutdown()

        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id=BASE_MODEL_ID,
                model_source=MODEL_SOURCE,
            ),
            lora_config=LoraConfig(dynamic_lora_loading_path=ADAPTER_SOURCE),
            deployment_config=dict(
                autoscaling_config=dict(
                    min_replicas=NUM_REPLICAS, max_replicas=NUM_REPLICAS
                ),
                request_router_config=RequestRouterConfig(
                    request_router_class=KVAwareRouter
                ),
            ),
            engine_kwargs=dict(
                enable_lora=True,
                max_lora_rank=MAX_LORA_RANK,
                max_loras=2,
                enable_prefix_caching=True,
                enable_prompt_tokens_details=True,
                enable_force_include_usage=True,
                enforce_eager=True,
                gpu_memory_utilization=0.4,
                max_model_len=2048,
            ),
            placement_group_config={"bundles": [{"GPU": 1}]},
            experimental_configs={"KV_EVENTS_PORT_BASE": 21800},
            runtime_env=dict(
                env_vars={
                    "RAY_SERVE_ENABLE_DIRECT_INGRESS": "1",
                    "RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING": "1",
                }
            ),
            log_engine_metrics=False,
        )
        # Swap the ingress for the introspection LLMRouter so the tracker's
        # per-adapter view of the KV index is reachable over the handle.
        with patch_ingress():
            handle = serve.run(build_kv_app(llm_config), name=APP_NAME)
        yield handle
        serve.shutdown()

    @pytest.mark.asyncio
    @pytest.mark.timeout(600)
    async def test_adapter_kv_cache_affinity(self, deployed_handle):
        router = serve.get_deployment_handle("LLMRouter", app_name=APP_NAME)
        app_url = get_application_url(app_name=APP_NAME, use_localhost=True)
        endpoints = await registered_endpoints(deployed_handle, router)
        workers = sorted(endpoints)
        # Which adapter lands where is fixed here, not by routing: each request
        # goes straight to one replica's direct-ingress port.
        warmed = dict(zip((ADAPTER_A, ADAPTER_B), workers))
        for adapter, worker_id in warmed.items():
            response = post_completion(endpoints[worker_id], adapter)
            assert response["model"] == adapter
            # Nothing had this prompt cached under this adapter yet.
            assert cached_tokens(response) == 0

        token_ids = tokenize(endpoints[workers[0]])
        prompt_blocks = num_prompt_blocks(token_ids)
        assert prompt_blocks >= 2

        async def each_adapter_indexed_on_its_own_worker():
            overlaps = {
                adapter: await router.get_kv_overlap_blocks.remote(token_ids, adapter)
                for adapter in warmed
            }
            return all(
                overlaps[adapter].get(worker_id) == prompt_blocks
                for adapter, worker_id in warmed.items()
            )

        await async_wait_for_condition(
            each_adapter_indexed_on_its_own_worker, timeout=120
        )

        # A cache hit is only possible on the replica that prefilled this prompt
        # under this adapter,.
        for adapter in warmed:
            response = post_completion(app_url, adapter)
            assert response["model"] == adapter
            assert cached_tokens(response) >= prompt_blocks * BLOCK_SIZE

        # Last, because it caches the prompt in the base model's namespace.
        response = post_completion(app_url, BASE_MODEL_ID)
        assert response["model"] == BASE_MODEL_ID
        assert cached_tokens(response) == 0

    @pytest.mark.asyncio
    @pytest.mark.timeout(600)
    async def test_adapter_affinity_without_overlap(self, deployed_handle):
        """With no KV overlap to score on, an adapter request still lands on a
        replica already holding that adapter rather than one that would have to
        load it first."""
        router = serve.get_deployment_handle("LLMRouter", app_name=APP_NAME)
        app_url = get_application_url(app_name=APP_NAME, use_localhost=True)
        endpoints = await registered_endpoints(deployed_handle, router)
        adapter_worker = sorted(endpoints)[0]
        post_completion(endpoints[adapter_worker], ADAPTER_A)

        unseen_tokens = tokenize(endpoints[adapter_worker], UNSEEN_PROMPT)
        unseen_blocks = num_prompt_blocks(unseen_tokens)
        assert unseen_blocks >= 2

        # Only the replica already serving adapter A is a multiplex candidate,
        # so that is the only place this prefix can end up cached.
        response = post_completion(app_url, ADAPTER_A, prompt=UNSEEN_PROMPT)
        assert response["model"] == ADAPTER_A
        assert cached_tokens(response) == 0

        async def cached_on_the_adapters_worker_only():
            overlap = await router.get_kv_overlap_blocks.remote(
                unseen_tokens, ADAPTER_A
            )
            return overlap.get(adapter_worker) == unseen_blocks and all(
                blocks == 0
                for worker_id, blocks in overlap.items()
                if worker_id != adapter_worker
            )

        await async_wait_for_condition(cached_on_the_adapters_worker_only, timeout=120)


if __name__ == "__main__":
    if not ray.is_initialized():
        ray.init(address="auto")
    sys.exit(pytest.main(["-v", "-s", __file__]))
