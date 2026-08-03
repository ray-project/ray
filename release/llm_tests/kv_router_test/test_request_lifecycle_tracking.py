import asyncio
from collections import defaultdict
import json
import math
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

        # Built before patch_ingress; see build_kv_config.
        llm_config = build_kv_config(
            request_router_class=_TestKVAwareRouter,
            kv_events_port_base=21600,
        )
        # Swap the ingress for the introspection LLMRouter so booked lifecycle
        # events and the tracker's state are reachable over the deployment handle.
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
        """Wait for every ingress replica to see the worker as schedulable."""

        async def registered():
            per_replica = await router.broadcast(
                "get_registered_worker_ids"
            ).results_async()
            return len(per_replica) == num_ingress_replicas and all(
                len(ids) == 1 for ids in per_replica
            )

        await async_wait_for_condition(registered, timeout=90, retry_interval_ms=1000)
        return (await router.broadcast("get_registered_worker_ids").results_async())[0][
            0
        ]

    @pytest.mark.asyncio
    async def test_exact_lifecycle_tracking(self, deployed_handle):
        router = serve.get_deployment_handle("LLMRouter", app_name=APP_NAME)
        host, port = await self._backend_endpoint(deployed_handle)
        replica_ids = await router.broadcast("get_replica_id").results_async()
        # The replica's worker must register before a request can book against it.
        worker_id = await self._registered_worker(router, len(replica_ids))
        assert all(
            active == 0
            for active in await router.broadcast(
                "get_worker_active_requests", worker_id
            ).results_async()
        )

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

        # Snapshot every tracker's view and the worker's tracked load after
        # each streamed chunk: completion evicts the request, so its in-flight
        # state is only observable while streaming.
        usage = None
        snapshots = defaultdict(list)
        live_active_requests = defaultdict(list)
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
                for reading in await router.broadcast(
                    "get_lifecycle_snapshot", LIFECYCLE_REQUEST_ID, worker_id
                ).results_async():
                    replica_id = reading["replica_id"]
                    if reading["lifecycle"] is not None:
                        snapshots[replica_id].append(reading["lifecycle"])
                    live_active_requests[replica_id].append(reading["active_requests"])

        assert usage is not None, "expected a final usage chunk"
        # The broadcast reached every tracker, not just the routing one.
        assert set(snapshots) == set(
            replica_ids
        ), "request was never observed in flight on every ingress tracker"

        # Every in-flight snapshot upholds exact token and block accounting,
        # per replica: trackers apply a batch concurrently, so a peer can lag.
        block_size = (await router.broadcast("get_block_size").results_async())[0]
        for replica_snapshots in snapshots.values():
            previous_output_tokens = 0
            for snapshot in replica_snapshots:
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
        assert all(
            max(actives) >= 1 for actives in live_active_requests.values()
        ), "request was never booked as active load"
        assert all(
            errors == []
            for errors in await router.broadcast("get_errors").results_async()
        )

        # Completion frees the request from both the tracker view and the load
        # tracker (no leaked active load to skew later scoring).
        async def request_freed():
            return all(
                lifecycle is None
                for lifecycle in await router.broadcast(
                    "get_request_lifecycle", LIFECYCLE_REQUEST_ID
                ).results_async()
            )

        await async_wait_for_condition(request_freed, timeout=15, retry_interval_ms=200)
        assert all(
            request_ids == []
            for request_ids in await router.broadcast(
                "get_active_request_ids"
            ).results_async()
        )

        async def worker_load_cleared():
            return all(
                active == 0
                for active in await router.broadcast(
                    "get_worker_active_requests", worker_id
                ).results_async()
            )

        await async_wait_for_condition(
            worker_load_cleared, timeout=15, retry_interval_ms=200
        )

        # Every replica received the request's whole ordered event stream.
        per_replica_events = [
            [
                (name, args)
                for name, args in event_log
                if args[0] == LIFECYCLE_REQUEST_ID
            ]
            for event_log in await router.broadcast("get_event_log").results_async()
        ]
        for events in per_replica_events:
            names = [name for name, _ in events]
            assert names[0] == "on_request_added"
            assert names[1] == "on_prefill_complete"
            assert names[-1] == "on_request_completed"
            assert names[2:-1] == ["on_decode_progress"] * (len(names) - 3)

            added_args = events[0][1]
            assert added_args[1] == worker_id
            assert len(added_args[2]) == usage["prompt_tokens"]  # token ids booked
            assert added_args[3] == MAX_TOKENS  # client max_tokens drives decode decay
            decode_counts = [
                args[1] for name, args in events if name == "on_decode_progress"
            ]
            assert decode_counts == sorted(set(decode_counts))  # strictly increasing
            assert decode_counts[-1] == usage["completion_tokens"]

        # Identical on every replica.
        prompt_token_ids = per_replica_events[0][0][1][2]

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
        """A reservation booked through the lifecycle hooks shows up as active
        load on the worker (the value scoring consumes) and freeing it restores
        baseline."""
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

        await router.broadcast(
            "on_request_added", "probe", worker_id, list(range(64)), 32
        ).results_async()
        assert all(active == 1 for active in await worker_active_requests())

        await router.broadcast("on_request_completed", "probe").results_async()

        async def worker_load_cleared():
            return all(active == 0 for active in await worker_active_requests())

        await async_wait_for_condition(
            worker_load_cleared, timeout=15, retry_interval_ms=200
        )


if __name__ == "__main__":
    if not ray.is_initialized():
        ray.init(address="auto")
    sys.exit(pytest.main(["-v", "-s", __file__]))
