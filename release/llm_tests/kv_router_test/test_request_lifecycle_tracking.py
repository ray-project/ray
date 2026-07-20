import asyncio
import json
import math
import sys
from dataclasses import asdict
from unittest import mock

import pytest
import requests
from dynamo.llm import compute_block_hash_for_seq

import ray
from ray import serve
from ray._common.test_utils import async_wait_for_condition
from ray.llm._internal.serve.routing_policies.kv_aware.kv_aware_actor import (
    _MODEL_NAME,
    _TENANT_ID,
    KV_ROUTER_ACTOR_NAME,
    KVRouterActor,
)
from ray.llm._internal.serve.routing_policies.kv_aware.vllm.kv_events import (
    configure_kv_events_for_kv_routing,
)
from ray.serve.config import RequestRouterConfig
from ray.serve.llm import LLMConfig, ModelLoadingConfig, build_openai_app

from utils import _TestKVAwareRouter, discover_deployment_actor

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


class RecordingKVRouterActor(KVRouterActor):
    """The real KVRouterActor, additionally recording every event it applies
    and any error raised while booking it into the live selection service.

    Eviction on completion makes the hook calls unobservable from state alone;
    the event log preserves them, and the error log proves each hook's call into
    the live service succeeded rather than being swallowed by the reporter pump.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._event_log = []
        self._errors = []

    async def on_lifecycle_events(self, events):
        self._event_log.extend(events)
        for hook_name, args in events:
            try:
                await getattr(self, hook_name)(*args)
            except Exception as e:
                self._errors.append((hook_name, repr(e)))

    def get_event_log(self):
        return self._event_log

    def get_errors(self):
        return self._errors

    def get_registered_worker_ids(self):
        """(Test only) Worker ids the selection service can currently schedule."""
        workers = self._svc.list_workers(model_name=_MODEL_NAME, tenant_id=_TENANT_ID)
        return sorted(
            w["worker_id"] for w in workers if w["lifecycle"] == "schedulable"
        )

    async def get_worker_active_requests(self, worker_id):
        """(Test only) In-flight requests the service tracks as active load on
        ``worker_id`` -- the count scoring factors in."""
        for model in self._svc.loads(model_name=_MODEL_NAME, tenant_id=_TENANT_ID):
            for load in model["loads"]:
                if load["worker_id"] == worker_id:
                    return load["active_requests"]
        return 0

    async def get_request_lifecycle(self, request_id):
        """(Test only) Snapshot of a request's local lifecycle state, or ``None``."""
        state = self._requests.get(request_id)
        if state is None:
            return None
        snapshot = asdict(state)
        snapshot.pop("created_at", None)
        return snapshot

    async def get_active_request_ids(self):
        """(Test only) Ids of the requests in the actor's in-flight view."""
        return list(self._requests)

    async def get_overlap_blocks(self, token_ids):
        """(Test only) Per-worker device-tier KV overlap blocks for a sequence."""
        scores = await self._svc.overlap_scores(
            {
                "model_name": _MODEL_NAME,
                "tenant_id": _TENANT_ID,
                "token_ids": list(token_ids),
            }
        )
        return {w["worker_id"]: w["device_blocks"] for w in scores["workers"]}


def num_prompt_blocks(token_ids):
    """Number of full KV blocks in a token sequence (matches the indexer)."""
    return len(compute_block_hash_for_seq(list(token_ids), BLOCK_SIZE))


class TestLifecycleTracking:
    @pytest.fixture(scope="class")
    def deployed_handle(self):
        """Deploy a direct-streaming LLMServer with KV events on and the
        recording actor attached."""
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
                # plane; the builder auto-attaches the KV router actor.
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
        # Emit engine KV-cache events so the actor registers the replica's worker
        # (making it schedulable, required to book a reservation) and the service
        # indexes the prompt's blocks.
        configure_kv_events_for_kv_routing(llm_config)

        # Patch in the recording subclass so the builder-attached, deployment-
        # scoped KV router actor exposes the received lifecycle events.
        with mock.patch(
            "ray.llm._internal.serve.routing_policies.kv_aware.utils.KVRouterActor",
            RecordingKVRouterActor,
        ):
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

    async def _registered_worker(self, actor):
        """Wait for the replica's worker to register (schedulable) and return it."""
        await async_wait_for_condition(
            lambda: len(ray.get(actor.get_registered_worker_ids.remote())) == 1,
            timeout=90,
            retry_interval_ms=1000,
        )
        return (await actor.get_registered_worker_ids.remote())[0]

    @pytest.mark.asyncio
    async def test_exact_lifecycle_tracking(self, deployed_handle):
        actor = discover_deployment_actor(
            APP_NAME, deployed_handle.deployment_name, KV_ROUTER_ACTOR_NAME
        )
        assert actor is not None, "KV router actor was not discoverable"
        host, port = await self._backend_endpoint(deployed_handle)
        # The replica's worker must register before a request can book against it.
        worker_id = await self._registered_worker(actor)
        assert await actor.get_worker_active_requests.remote(worker_id) == 0

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

        # Snapshot the actor's view and the worker's tracked load after every
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
                snapshot = await actor.get_request_lifecycle.remote(
                    LIFECYCLE_REQUEST_ID
                )
                if snapshot is not None:
                    snapshots.append(snapshot)
                live_active_requests.append(
                    await actor.get_worker_active_requests.remote(worker_id)
                )

        assert usage is not None, "expected a final usage chunk"
        assert snapshots, "request was never observed in flight on the actor"

        # Every in-flight snapshot upholds exact token and block accounting.
        block_size = await actor.get_block_size.remote()
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
        assert await actor.get_errors.remote() == []

        # Completion frees the request from both the actor view and the load
        # tracker (no leaked active load to skew later scoring).
        worker_id = snapshots[-1]["worker_id"]
        await async_wait_for_condition(
            lambda: ray.get(actor.get_request_lifecycle.remote(LIFECYCLE_REQUEST_ID))
            is None,
            timeout=15,
            retry_interval_ms=200,
        )
        assert await actor.get_active_request_ids.remote() == []
        await async_wait_for_condition(
            lambda: ray.get(actor.get_worker_active_requests.remote(worker_id)) == 0,
            timeout=15,
            retry_interval_ms=200,
        )

        events = [
            (name, args)
            for name, args in await actor.get_event_log.remote()
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
        await async_wait_for_condition(
            lambda: ray.get(actor.get_overlap_blocks.remote(prompt_token_ids)).get(
                worker_id, 0
            )
            == prompt_blocks,
            timeout=60,
            retry_interval_ms=500,
        )

    @pytest.mark.asyncio
    async def test_booked_reservation_changes_scoring_load(self, deployed_handle):
        """A reservation booked through the lifecycle hooks shows up as active
        load on the worker (the value scoring consumes) and freeing it restores
        baseline."""
        actor = discover_deployment_actor(
            APP_NAME, deployed_handle.deployment_name, KV_ROUTER_ACTOR_NAME
        )
        worker_id = await self._registered_worker(actor)
        assert await actor.get_worker_active_requests.remote(worker_id) == 0

        await actor.on_request_added.remote(
            "probe", worker_id, list(range(64)), expected_output_tokens=32
        )
        assert await actor.get_worker_active_requests.remote(worker_id) == 1

        await actor.on_request_completed.remote("probe")
        await async_wait_for_condition(
            lambda: ray.get(actor.get_worker_active_requests.remote(worker_id)) == 0,
            timeout=15,
            retry_interval_ms=200,
        )


if __name__ == "__main__":
    if not ray.is_initialized():
        ray.init(address="auto")
    sys.exit(pytest.main(["-v", "-s", __file__]))
