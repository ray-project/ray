import asyncio
import json
import math
import sys

import pytest
import requests
from dynamo.llm import compute_block_hash_for_seq

import ray
from ray import serve
from ray._common.test_utils import async_wait_for_condition
from ray.llm._internal.serve.routing_policies.kv_aware.vllm.kv_events import (
    configure_kv_events_for_kv_routing,
)
from ray.serve.config import RequestRouterConfig
from ray.serve.llm import LLMConfig, ModelLoadingConfig, build_openai_app

from utils import _TestKVAwareRouter, patch_ingress

MODEL_ID = "Qwen/Qwen3-0.6B"
APP_NAME = "lifecycle_tracking_gpu_test"
REQUEST_ID = "gpu-req-1"
BLOCK_SIZE = 16
MAX_TOKENS = 48
# A multi-block prompt so the engine caches retrievable KV blocks.
PROMPT_TEXT = (
    "Repeat the following instruction carefully and then answer it in full "
    "detail: describe the water cycle, including evaporation, condensation, "
    "precipitation, and collection, giving a concrete real-world example for "
    "each of the four stages and how they connect."
)
# Token tracking reports Serve's canonical request id, matching the id used for
# routing, even if vLLM derives a separate engine-level id internally.
LIFECYCLE_REQUEST_ID = REQUEST_ID

# Each LLMRouter ingress replica builds its own KVTokenTracker, and Serve runs one
# per proxy node (every node with a replica, plus the head), so this test's head +
# GPU worker cluster gets two. The engine's lifecycle events load-balance across
# them, splitting a request's state.
# TODO (jeffreywang): Re-enable this after #65010 which enables multiple LLMRouter
# ingress replicas lands.
pytestmark = pytest.mark.skip(
    reason=(
        "Lifecycle accounting requires a single LLMRouter ingress replica; "
        "re-enable after #65010."
    )
)


def num_prompt_blocks(token_ids):
    """Number of full KV blocks in a token sequence (matches the indexer)."""
    return len(compute_block_hash_for_seq(list(token_ids), BLOCK_SIZE))


class TestLifecycleTracking:
    """The KVTokenTracker books request lifecycle events through the LLMRouter's
    ``on_lifecycle_events`` handle method; the test ``LLMRouter`` subclass
    records those events and exposes the tracker's state over the deployment
    handle so the test can assert exact accounting."""

    @pytest.fixture(scope="class")
    def deployed_handle(self):
        """Deploy a direct-streaming LLMServer with KV events on and the
        introspection ingress."""
        if not ray.is_initialized():
            ray.init(address="auto")
        serve.shutdown()

        engine_kwargs = dict(
            max_model_len=2048,
            enforce_eager=True,
            gpu_memory_utilization=0.4,
            use_tqdm_on_load=False,
        )
        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id=MODEL_ID,
                model_source=MODEL_ID,
            ),
            deployment_config=dict(
                autoscaling_config=dict(min_replicas=1, max_replicas=1),
                # KVAwareRouter gates engine token tracking and the KV-events
                # plane; the ingress builds the KVTokenTracker.
                request_router_config=RequestRouterConfig(
                    request_router_class=_TestKVAwareRouter
                ),
            ),
            engine_kwargs=engine_kwargs,
            placement_group_config={"bundles": [{"GPU": 1}]},
            experimental_configs={"KV_EVENTS_PORT_BASE": 21600},
            runtime_env=dict(env_vars={"VLLM_DISABLE_COMPILE_CACHE": "1"}),
            log_engine_metrics=False,
        )
        # Emit engine KV-cache events so the tracker registers the replica's
        # worker (making it schedulable, required to book a reservation) and the
        # service indexes the prompt's blocks.
        configure_kv_events_for_kv_routing(llm_config)

        # Swap the ingress for the introspection LLMRouter so booked lifecycle
        # events and the tracker's state are reachable over the deployment handle.
        with patch_ingress():
            app = build_openai_app({"llm_configs": [llm_config]})
            handle = serve.run(app, name=APP_NAME)
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

    async def _registered_worker(self, router):
        """Wait for the replica's worker to register (schedulable) and return it."""

        async def registered():
            return len(await router.get_registered_worker_ids.remote()) == 1

        await async_wait_for_condition(registered, timeout=90, retry_interval_ms=1000)
        return (await router.get_registered_worker_ids.remote())[0]

    @pytest.mark.asyncio
    async def test_exact_lifecycle_tracking(self, deployed_handle):
        router = serve.get_deployment_handle("LLMRouter", app_name=APP_NAME)
        host, port = await self._backend_endpoint(deployed_handle)
        # The replica's worker must register before a request can book against it.
        worker_id = await self._registered_worker(router)
        assert await router.get_worker_active_requests.remote(worker_id) == 0

        # X-Request-Id pins the engine request id; include_usage returns the
        # engine's own token counts as ground truth.
        url = f"http://{host}:{port}/v1/chat/completions"
        payload = {
            "model": MODEL_ID,
            "messages": [{"role": "user", "content": PROMPT_TEXT}],
            "stream": True,
            "stream_options": {"include_usage": True},
            "max_tokens": MAX_TOKENS,
            "temperature": 0.0,
        }
        headers = {"X-Request-Id": REQUEST_ID}

        # Snapshot the tracker's view and the worker's tracked load after every
        # streamed chunk: completion evicts the request, so its in-flight state
        # is only observable while streaming.
        usage = None
        snapshots = []
        live_active_requests = []
        with requests.post(
            url, json=payload, headers=headers, stream=True, timeout=120
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
                snapshot = await router.get_request_lifecycle.remote(
                    LIFECYCLE_REQUEST_ID
                )
                if snapshot is not None:
                    snapshots.append(snapshot)
                live_active_requests.append(
                    await router.get_worker_active_requests.remote(worker_id)
                )

        assert usage is not None, "expected a final usage chunk"
        assert snapshots, "request was never observed in flight on the tracker"

        # Every in-flight snapshot upholds exact token and block accounting.
        block_size = await router.get_block_size.remote()
        previous_output_tokens = 0
        for snapshot in snapshots:
            assert snapshot["prompt_tokens"] == usage["prompt_tokens"]
            assert previous_output_tokens <= snapshot["output_tokens"]
            assert snapshot["output_tokens"] <= usage["completion_tokens"]
            previous_output_tokens = snapshot["output_tokens"]
            total_blocks = math.ceil(
                (usage["prompt_tokens"] + snapshot["output_tokens"]) / block_size
            )
            assert snapshot["total_blocks"] == total_blocks
            if snapshot["output_tokens"] > 0:
                assert snapshot["prefill_completed"] is True

        # The request was booked as active load on its worker while in flight,
        # and every hook's call into the live selection service succeeded.
        assert max(live_active_requests) >= 1, "request was never booked as active load"
        assert await router.get_errors.remote() == []

        # Completion frees the request from both the tracker view and the load
        # tracker (no leaked active load to skew later scoring).
        worker_id = snapshots[-1]["worker_id"]

        async def request_freed():
            return (
                await router.get_request_lifecycle.remote(LIFECYCLE_REQUEST_ID)
            ) is None

        await async_wait_for_condition(request_freed, timeout=15, retry_interval_ms=200)
        assert await router.get_active_request_ids.remote() == []

        async def worker_load_cleared():
            return await router.get_worker_active_requests.remote(worker_id) == 0

        await async_wait_for_condition(
            worker_load_cleared, timeout=15, retry_interval_ms=200
        )

        events = [
            (name, args)
            for name, args in await router.get_event_log.remote()
            if args[0] == LIFECYCLE_REQUEST_ID
        ]
        names = [name for name, _ in events]
        assert names[0] == "on_request_added"
        assert names[1] == "on_prefill_complete"
        assert names[-1] == "on_request_completed"
        assert names[2:-1] == ["on_decode_progress"] * (len(names) - 3)

        added_args = events[0][1]
        assert added_args[1] == worker_id
        prompt_token_ids = added_args[2]
        assert len(prompt_token_ids) == usage["prompt_tokens"]  # token ids booked
        assert added_args[3] == MAX_TOKENS  # client max_tokens drives decode decay
        decode_counts = [
            args[1] for name, args in events if name == "on_decode_progress"
        ]
        assert decode_counts == sorted(set(decode_counts))  # strictly increasing
        assert decode_counts[-1] == usage["completion_tokens"]

        # The engine indexed the prompt's KV blocks; they show as cache overlap
        # on the worker (the prefix the next overlapping request would reuse).
        prompt_blocks = num_prompt_blocks(prompt_token_ids)
        assert prompt_blocks >= 1

        async def overlap_indexed():
            overlaps = await router.get_kv_overlap_blocks.remote(prompt_token_ids)
            return overlaps.get(worker_id, 0) == prompt_blocks

        await async_wait_for_condition(
            overlap_indexed, timeout=60, retry_interval_ms=500
        )

    @pytest.mark.asyncio
    async def test_booked_reservation_changes_scoring_load(self, deployed_handle):
        """A reservation booked through the lifecycle hooks shows up as active
        load on the worker (the value scoring consumes) and freeing it restores
        baseline."""
        router = serve.get_deployment_handle("LLMRouter", app_name=APP_NAME)
        worker_id = await self._registered_worker(router)
        assert await router.get_worker_active_requests.remote(worker_id) == 0

        await router.on_request_added.remote(
            "probe", worker_id, list(range(64)), expected_output_tokens=32
        )
        assert await router.get_worker_active_requests.remote(worker_id) == 1

        await router.on_request_completed.remote("probe")

        async def worker_load_cleared():
            return await router.get_worker_active_requests.remote(worker_id) == 0

        await async_wait_for_condition(
            worker_load_cleared, timeout=15, retry_interval_ms=200
        )


if __name__ == "__main__":
    if not ray.is_initialized():
        ray.init(address="auto")
    sys.exit(pytest.main(["-v", "-s", __file__]))
