import sys
from typing import Dict, List

import pytest
from vllm.distributed.kv_events import (
    BlockRemoved,
    BlockStored,
    KVEventBatch,
    ZmqEventPublisher,
)

from ray._common.test_utils import async_wait_for_condition
from ray.llm._internal.serve.routing_policies.kv_aware.kv_token_tracker import (
    _MODEL_NAME,
    _TENANT_ID,
    KVTokenTracker,
    get_worker_id,
)
from ray.serve._private.common import (
    DeploymentID,
    DeploymentTargetInfo,
    ReplicaID,
    RunningReplicaInfo,
)

BLOCK_SIZE = 16
MAX_NUM_BATCHED_TOKENS = 8192


class _TestKVTokenTracker(KVTokenTracker):
    """KVTokenTracker augmented with test-only introspection.

    The tracker is a plain object built by the LLMRouter, so these tests
    construct it and drive its methods directly (no actor, no ``.remote()``).
    """

    async def get_candidate_worker_ids(self) -> List[int]:
        """(Test only) The workers currently tracked from running replicas."""
        return sorted(self._replica_id_by_worker)

    def get_registered_worker_ids(self) -> List[int]:
        """(Test only) Worker ids the selection service can currently schedule."""
        if self._svc is None:
            return []
        workers = self._svc.list_workers(
            model_name=_MODEL_NAME, routing_group=_TENANT_ID
        )
        return sorted(
            w["worker_id"] for w in workers if w["lifecycle"] == "schedulable"
        )

    async def get_kv_overlap_blocks(self, token_ids: List[int]) -> Dict[int, int]:
        """(Test only) Per-worker device-tier KV overlap blocks for a token sequence.

        Returns the number of leading blocks of ``token_ids`` each worker has cached.
        """
        scores = await self.get_kv_overlap_scores(token_ids)
        return {
            worker_id: score["device_blocks"] for worker_id, score in scores.items()
        }

    async def get_kv_overlap_scores(self, token_ids: List[int]) -> Dict[int, dict]:
        """(Test only) Per-worker overlap across every KV storage tier."""
        if self._svc is None:
            return {}
        scores = await self._svc.overlap_scores(
            {
                "model_name": _MODEL_NAME,
                "tenant_id": _TENANT_ID,
                "token_ids": list(token_ids),
            }
        )
        return {worker["worker_id"]: worker for worker in scores["workers"]}

    async def get_potential_loads(self, token_ids: List[int]) -> Dict[int, dict]:
        """(Test only) Per-worker projected load for routing ``token_ids``."""
        if self._svc is None:
            return {}
        loads = await self._svc.potential_loads(
            {
                "model_name": _MODEL_NAME,
                "tenant_id": _TENANT_ID,
                "token_ids": list(token_ids),
            }
        )
        return {load["worker_id"]: load for load in loads}

    def get_loads(self) -> Dict[int, dict]:
        """(Test only) Current per-worker active load."""
        if self._svc is None:
            return {}
        model_loads = self._svc.loads(model_name=_MODEL_NAME, routing_group=_TENANT_ID)
        if not model_loads:
            return {}
        return {load["worker_id"]: load for load in model_loads[0]["loads"]}


class _LocalKVTokenTracker(_TestKVTokenTracker):
    """The real KVTokenTracker with Serve LongPoll tracking disabled as there is
    no Serve controller in these tests; tests drive _on_deployment_targets
    directly with synthetic replica snapshots."""

    def _start_replica_tracking(self) -> None:
        pass


class FakeReplica:
    """Mocks a real LLM replica's engine KV-event emission.

    It runs no engine; instead it publishes synthetic KV events through vLLM's
    real ``ZmqEventPublisher`` -- the exact path and wire format a real engine
    uses to emit KV events over ZMQ -- so the selection service's connect-out
    listener ingests them identically to production.
    """

    def __init__(self, port: int):
        self._port = port
        self._replay_port = port + 1000
        self._pub = ZmqEventPublisher(
            data_parallel_rank=0,
            endpoint=f"tcp://*:{port}",
            replay_endpoint=f"tcp://*:{self._replay_port}",
            topic="",
        )

    def endpoint(self) -> str:
        return f"tcp://127.0.0.1:{self._port}"

    def replay_endpoint(self) -> str:
        return f"tcp://127.0.0.1:{self._replay_port}"

    def publish_stored(self, block_hashes, token_ids, medium="GPU") -> None:
        self._pub.publish(
            KVEventBatch(
                ts=1.0,
                events=[
                    BlockStored(
                        block_hashes=list(block_hashes),
                        parent_block_hash=None,
                        token_ids=list(token_ids),
                        block_size=BLOCK_SIZE,
                        lora_id=None,
                        medium=medium,
                        lora_name=None,
                    )
                ],
            )
        )

    def publish_removed(self, block_hashes, medium="GPU") -> None:
        self._pub.publish(
            KVEventBatch(
                ts=2.0,
                events=[BlockRemoved(block_hashes=list(block_hashes), medium=medium)],
            )
        )

    def close(self) -> None:
        self._pub.shutdown()


def running_replica(
    unique_id: str, endpoint: str, replay_endpoint: str, dp_rank: int = 0
):
    """A RunningReplicaInfo whose routing_stats advertise a KV-events endpoint,
    exactly as a real replica's record_routing_stats would surface it."""
    return RunningReplicaInfo(
        replica_id=ReplicaID(
            unique_id=unique_id,
            deployment_id=DeploymentID(name="llm", app_name="app"),
        ),
        node_id="node-1",
        node_ip="127.0.0.1",
        availability_zone=None,
        actor_name=f"actor-{unique_id}",
        max_ongoing_requests=10,
        routing_stats={
            "kv_event_metadata": {
                "endpoint": endpoint,
                "block_size": BLOCK_SIZE,
                "max_num_batched_tokens": MAX_NUM_BATCHED_TOKENS,
                "dp_rank": dp_rank,
                "replay_endpoint": replay_endpoint,
            }
        },
    )


def targets(*replicas):
    return DeploymentTargetInfo(is_available=True, running_replicas=list(replicas))


async def wait_registered(tracker, worker_ids):
    await async_wait_for_condition(
        lambda: tracker.get_registered_worker_ids() == sorted(worker_ids),
        timeout=15,
    )


async def wait_for_overlap(tracker, token_ids, predicate, publish=None, timeout=30):
    """Poll the tracker's overlap view until ``predicate`` holds."""

    async def condition():
        if publish is not None:
            publish()
        try:
            overlap = await tracker.get_kv_overlap_blocks(list(token_ids))
        except Exception:
            return False
        return predicate(overlap)

    await async_wait_for_condition(condition, timeout=timeout, retry_interval_ms=500)


async def wait_for_overlap_scores(
    tracker, token_ids, predicate, publish=None, timeout=30
):
    """Poll the tracker's tiered overlap view until ``predicate`` holds."""

    async def condition():
        if publish is not None:
            publish()
        try:
            scores = await tracker.get_kv_overlap_scores(list(token_ids))
        except Exception:
            return False
        return predicate(scores)

    await async_wait_for_condition(condition, timeout=timeout, retry_interval_ms=500)


async def book_uncached_load(tracker, worker_id, count, base):
    """Book ``count`` reservations of distinct, uncached tokens onto ``worker_id``
    so each adds full prefill load to it; returns their reservation ids."""
    reservation_ids = []
    for i in range(count):
        reservation_id = f"load-{base}-{i}"
        start = base + i * 4 * BLOCK_SIZE
        token_ids = list(range(start, start + 4 * BLOCK_SIZE))
        selection = await tracker.select_worker(reservation_id, token_ids, [worker_id])
        assert selection["worker_id"] == worker_id
        assert selection["effective_prefill_tokens"] == len(token_ids)
        reservation_ids.append(reservation_id)
    return reservation_ids


async def book_decode_load(
    tracker,
    worker_id,
    reservation_id,
    token_ids,
    output_tokens,
    expected_output_tokens=None,
):
    """Book a request, move it to decode, and add output blocks."""
    selection = await tracker.select_worker(
        reservation_id, token_ids, [worker_id], expected_output_tokens
    )
    assert selection["worker_id"] == worker_id
    await tracker.on_prefill_complete(reservation_id)
    await tracker.on_decode_progress(reservation_id, output_tokens)
    return reservation_id


class TestKvEventIngestion:
    """Validates that a replica advertises its vLLM ZMQ PUB endpoint on the
    LongPoll snapshot -> the KVTokenTracker registers it -> the selection
    service's connect-out listener -> the global KV indexer."""

    @pytest.mark.asyncio
    async def test_register_then_ingest_events(self):
        """A replica appearing on the snapshot with a KV-events endpoint makes the
        selection service dial it and index the KV events it publishes."""
        tracker = _LocalKVTokenTracker()
        replica = FakeReplica(23901)
        try:
            worker_id = get_worker_id("replica-A")
            # The LongPoll snapshot now carries this replica's KV-events endpoint.
            tracker._on_deployment_targets(
                targets(
                    running_replica(
                        "replica-A", replica.endpoint(), replica.replay_endpoint()
                    )
                )
            )
            await wait_registered(tracker, [worker_id])

            # Two full blocks of prompt; once ingested both overlap the query.
            token_ids = list(range(2 * BLOCK_SIZE))
            await wait_for_overlap(
                tracker,
                token_ids,
                lambda overlap: overlap.get(worker_id) == 2,
                publish=lambda: replica.publish_stored([101, 102], token_ids),
            )

            # Removing the leaf block drops it from the worker's overlap.
            await wait_for_overlap(
                tracker,
                token_ids,
                lambda overlap: overlap.get(worker_id) == 1,
                publish=lambda: replica.publish_removed([102]),
            )
        finally:
            replica.close()

    @pytest.mark.asyncio
    async def test_per_worker_isolation(self):
        """Each worker's overlap reflects only the blocks its own replica cached."""
        tracker = _LocalKVTokenTracker()
        a = FakeReplica(23902)
        b = FakeReplica(23903)
        workers = {
            "replica-A": get_worker_id("replica-A"),
            "replica-B": get_worker_id("replica-B"),
        }
        try:
            # Both replicas advertise their endpoints in a single snapshot.
            tracker._on_deployment_targets(
                targets(
                    running_replica("replica-A", a.endpoint(), a.replay_endpoint()),
                    running_replica("replica-B", b.endpoint(), b.replay_endpoint()),
                )
            )
            await wait_registered(tracker, list(workers.values()))

            tokens_a = list(range(BLOCK_SIZE))
            tokens_b = list(range(BLOCK_SIZE, 2 * BLOCK_SIZE))

            await wait_for_overlap(
                tracker,
                tokens_a,
                lambda overlap: overlap.get(workers["replica-A"]) == 1,
                publish=lambda: a.publish_stored([201], tokens_a),
            )
            await wait_for_overlap(
                tracker,
                tokens_b,
                lambda overlap: overlap.get(workers["replica-B"]) == 1,
                publish=lambda: b.publish_stored([301], tokens_b),
            )
            # A's content does not overlap B's worker and vice versa.
            overlap_a = await tracker.get_kv_overlap_blocks(tokens_a)
            assert overlap_a.get(workers["replica-B"], 0) == 0
        finally:
            a.close()
            b.close()

    @pytest.mark.asyncio
    async def test_departed_replica_is_evicted(self):
        """A replica dropping off the snapshot tears down its listener and drops
        it from the selection service catalog."""
        tracker = _LocalKVTokenTracker()
        replica = FakeReplica(23904)
        worker_id = get_worker_id("replica-A")
        try:
            tracker._on_deployment_targets(
                targets(
                    running_replica(
                        "replica-A", replica.endpoint(), replica.replay_endpoint()
                    )
                )
            )
            token_ids = list(range(2 * BLOCK_SIZE))
            await wait_for_overlap(
                tracker,
                token_ids,
                lambda overlap: overlap.get(worker_id) == 2,
                publish=lambda: replica.publish_stored([401, 402], token_ids),
            )

            # The replica departs: an empty snapshot evicts its worker.
            tracker._on_deployment_targets(targets())
            await wait_registered(tracker, [])
            assert await tracker.get_candidate_worker_ids() == []
        finally:
            replica.close()

    @pytest.mark.asyncio
    async def test_select_worker_prefers_overlap(self):
        """select_worker scores via the selection service and picks the worker
        whose cached blocks overlap the prompt, within the allowed candidate set."""
        tracker = _LocalKVTokenTracker()
        a = FakeReplica(23905)
        b = FakeReplica(23906)
        worker_a = get_worker_id("replica-A")
        worker_b = get_worker_id("replica-B")
        try:
            tracker._on_deployment_targets(
                targets(
                    running_replica("replica-A", a.endpoint(), a.replay_endpoint()),
                    running_replica("replica-B", b.endpoint(), b.replay_endpoint()),
                )
            )
            await wait_registered(tracker, [worker_a, worker_b])

            # A caches the prompt's two blocks; B caches nothing.
            token_ids = list(range(2 * BLOCK_SIZE))
            await wait_for_overlap(
                tracker,
                token_ids,
                lambda overlap: overlap.get(worker_a) == 2,
                publish=lambda: a.publish_stored([501, 502], token_ids),
            )

            selection = await tracker.select_worker(
                "req-1", token_ids, [worker_a, worker_b]
            )
            assert selection["worker_id"] == worker_a
            assert selection["overlap_tokens"] >= BLOCK_SIZE
        finally:
            a.close()
            b.close()

    @pytest.mark.asyncio
    async def test_cpu_offload_routing(self):
        """CPU overlap breaks GPU-overlap ties but loses to a larger GPU hit."""
        tracker = _LocalKVTokenTracker()
        a = FakeReplica(23913)
        b = FakeReplica(23914)
        worker_a = get_worker_id("replica-A")
        worker_b = get_worker_id("replica-B")
        block_hashes = [701, 702, 703]
        token_ids = list(range(3 * BLOCK_SIZE))
        try:
            tracker._on_deployment_targets(
                targets(
                    running_replica("replica-A", a.endpoint(), a.replay_endpoint()),
                    running_replica("replica-B", b.endpoint(), b.replay_endpoint()),
                )
            )
            await wait_registered(tracker, [worker_a, worker_b])

            a.publish_stored(block_hashes, token_ids)
            a.publish_stored(block_hashes, token_ids, medium="CPU")
            a.publish_removed(block_hashes[1:])
            b.publish_stored(block_hashes[:1], token_ids[:BLOCK_SIZE])
            await wait_for_overlap_scores(
                tracker,
                token_ids,
                lambda scores: (
                    scores.get(worker_a, {}).get("device_blocks") == 1
                    and scores.get(worker_a, {}).get("host_pinned_extension_blocks")
                    == 2
                    and scores.get(worker_b, {}).get("device_blocks") == 1
                ),
            )

            cpu_selection = await tracker.select_worker(
                "cpu-tiebreak", token_ids, [worker_a, worker_b]
            )
            assert cpu_selection["worker_id"] == worker_a
            await tracker.on_request_completed("cpu-tiebreak")

            b.publish_stored(block_hashes, token_ids)
            await wait_for_overlap_scores(
                tracker,
                token_ids,
                lambda scores: scores.get(worker_b, {}).get("device_blocks") == 3,
            )

            scores = await tracker.get_kv_overlap_scores(token_ids)
            assert scores[worker_a]["host_pinned_extension_blocks"] == 2
            assert 1 < scores[worker_a]["router_credit_blocks"] < 3
            assert scores[worker_b]["router_credit_blocks"] == 3

            gpu_selection = await tracker.select_worker(
                "gpu-hit", token_ids, [worker_a, worker_b]
            )
            assert gpu_selection["worker_id"] == worker_b
            assert gpu_selection["effective_prefill_tokens"] == 0
            await tracker.on_request_completed("gpu-hit")

            b.publish_removed(block_hashes)
            await wait_for_overlap(
                tracker,
                token_ids,
                lambda overlap: overlap.get(worker_b) == 0,
            )

            cpu_selection = await tracker.select_worker(
                "cpu-reload", token_ids, [worker_a, worker_b]
            )
            assert cpu_selection["worker_id"] == worker_a
            assert 0 < cpu_selection["effective_prefill_tokens"] < len(token_ids)
        finally:
            a.close()
            b.close()

    @pytest.mark.asyncio
    async def test_select_worker_prefers_lower_load(self):
        """With equal KV overlap on both workers, select_worker routes to the
        worker carrying less active load. Booking heavy load onto one worker
        sends the next equally-overlapping request to the other."""
        tracker = _LocalKVTokenTracker()
        a = FakeReplica(23907)
        b = FakeReplica(23908)
        worker_a = get_worker_id("replica-A")
        worker_b = get_worker_id("replica-B")
        try:
            tracker._on_deployment_targets(
                targets(
                    running_replica("replica-A", a.endpoint(), a.replay_endpoint()),
                    running_replica("replica-B", b.endpoint(), b.replay_endpoint()),
                )
            )
            await wait_registered(tracker, [worker_a, worker_b])

            # Both replicas cache the same prompt blocks: overlap is equal, so the
            # selection turns purely on each worker's active load.
            token_ids = list(range(2 * BLOCK_SIZE))
            await wait_for_overlap(
                tracker,
                token_ids,
                lambda overlap: overlap.get(worker_a) == 2,
                publish=lambda: a.publish_stored([601, 602], token_ids),
            )
            await wait_for_overlap(
                tracker,
                token_ids,
                lambda overlap: overlap.get(worker_b) == 2,
                publish=lambda: b.publish_stored([601, 602], token_ids),
            )

            # Heavy active load on A (uncached prompts -> full prefill load): the
            # equally-overlapping request is routed to the idle worker B.
            load_a = await book_uncached_load(tracker, worker_a, count=8, base=100_000)
            loads = await tracker.get_potential_loads(token_ids)
            assert (
                loads[worker_a]["potential_prefill_tokens"]
                > loads[worker_b]["potential_prefill_tokens"]
            )
            selection = await tracker.select_worker(
                "probe-1", token_ids, [worker_a, worker_b]
            )
            assert selection["worker_id"] == worker_b

            # Free A and load B instead: the choice flips to A.
            for reservation_id in load_a:
                await tracker.on_request_completed(reservation_id)
            await book_uncached_load(tracker, worker_b, count=8, base=500_000)
            loads = await tracker.get_potential_loads(token_ids)
            assert (
                loads[worker_b]["potential_prefill_tokens"]
                > loads[worker_a]["potential_prefill_tokens"]
            )
            selection = await tracker.select_worker(
                "probe-2", token_ids, [worker_a, worker_b]
            )
            assert selection["worker_id"] == worker_a
        finally:
            a.close()
            b.close()

    @pytest.mark.asyncio
    async def test_select_worker_prefers_lower_decode_load(self):
        """With equal KV overlap and no active prefill, a long decode on one
        worker makes the selector choose the lower decode-load worker."""
        tracker = _LocalKVTokenTracker()
        a = FakeReplica(23909)
        b = FakeReplica(23910)
        worker_a = get_worker_id("replica-A")
        worker_b = get_worker_id("replica-B")
        try:
            tracker._on_deployment_targets(
                targets(
                    running_replica("replica-A", a.endpoint(), a.replay_endpoint()),
                    running_replica("replica-B", b.endpoint(), b.replay_endpoint()),
                )
            )
            await wait_registered(tracker, [worker_a, worker_b])

            token_ids = list(range(2 * BLOCK_SIZE))
            await wait_for_overlap(
                tracker,
                token_ids,
                lambda overlap: overlap.get(worker_a) == 2,
                publish=lambda: a.publish_stored([701, 702], token_ids),
            )
            await wait_for_overlap(
                tracker,
                token_ids,
                lambda overlap: overlap.get(worker_b) == 2,
                publish=lambda: b.publish_stored([701, 702], token_ids),
            )

            await book_decode_load(
                tracker,
                worker_a,
                "decode-load-a",
                token_ids,
                output_tokens=8 * BLOCK_SIZE,
            )

            loads = await tracker.get_potential_loads(token_ids)
            assert (
                loads[worker_a]["potential_prefill_tokens"]
                == loads[worker_b]["potential_prefill_tokens"]
            )
            assert (
                loads[worker_a]["potential_decode_blocks"]
                > loads[worker_b]["potential_decode_blocks"]
            )

            selection = await tracker.select_worker(
                "probe-decode", token_ids, [worker_a, worker_b]
            )
            assert selection["worker_id"] == worker_b
        finally:
            a.close()
            b.close()

    @pytest.mark.asyncio
    async def test_decode_load_uses_expected_output_decay(self):
        """Expected output length decays decode load as generation nears the
        estimate, so the selector prefers the lower remaining-load worker."""
        tracker = _LocalKVTokenTracker()
        a = FakeReplica(23911)
        b = FakeReplica(23912)
        worker_a = get_worker_id("replica-A")
        worker_b = get_worker_id("replica-B")
        try:
            tracker._on_deployment_targets(
                targets(
                    running_replica("replica-A", a.endpoint(), a.replay_endpoint()),
                    running_replica("replica-B", b.endpoint(), b.replay_endpoint()),
                )
            )
            await wait_registered(tracker, [worker_a, worker_b])

            token_ids = list(range(2 * BLOCK_SIZE))
            await wait_for_overlap(
                tracker,
                token_ids,
                lambda overlap: overlap.get(worker_a) == 2,
                publish=lambda: a.publish_stored([801, 802], token_ids),
            )
            await wait_for_overlap(
                tracker,
                token_ids,
                lambda overlap: overlap.get(worker_b) == 2,
                publish=lambda: b.publish_stored([801, 802], token_ids),
            )

            output_tokens = 4 * BLOCK_SIZE - 1
            await book_decode_load(
                tracker,
                worker_a,
                "decayed-decode-a",
                token_ids,
                output_tokens=output_tokens,
                expected_output_tokens=4 * BLOCK_SIZE,
            )
            await book_decode_load(
                tracker,
                worker_b,
                "full-decode-b",
                token_ids,
                output_tokens=output_tokens,
            )

            active_loads = tracker.get_loads()
            assert active_loads[worker_a]["potential_prefill_tokens"] == 0
            assert active_loads[worker_b]["potential_prefill_tokens"] == 0
            assert (
                active_loads[worker_a]["potential_decode_blocks"]
                < active_loads[worker_b]["potential_decode_blocks"]
            )

            selection = await tracker.select_worker(
                "probe-decay", token_ids, [worker_a, worker_b]
            )
            assert selection["worker_id"] == worker_a
        finally:
            a.close()
            b.close()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
