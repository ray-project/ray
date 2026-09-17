"""Validation for the router-local KV selector: build + process-global
registration, ``KVAwareRouter`` binding to it, and the
``LLMRouter`` -> ``KVTokenTracker`` -> Dynamo selection-service booking path.
"""

import asyncio
import sys

import pytest

import ray
import ray.cloudpickle
from ray import serve
from ray._common.test_utils import async_wait_for_condition
from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.core.ingress.router import LLMRouter
from ray.llm._internal.serve.routing_policies.kv_aware import kv_token_tracker
from ray.llm._internal.serve.routing_policies.kv_aware.kv_token_tracker import (
    KVTokenTracker,
    build_kv_token_tracker,
    get_kv_token_tracker,
    get_worker_id,
    set_kv_token_tracker,
)
from ray.serve._private.common import DeploymentID
from ray.serve.llm.request_router import KVAwareRouter


@pytest.fixture(autouse=True)
def reset_process_global():
    set_kv_token_tracker(None)
    yield
    set_kv_token_tracker(None)


class _SpyTracker:
    """Records constructor kwargs; skips the real selection service + LongPoll."""

    def __init__(self, *, indexer_threads, serve_deployment_id):
        self.indexer_threads = indexer_threads
        self.serve_deployment_id = serve_deployment_id


class _NoLongPollTracker(KVTokenTracker):
    """Real selection service without a controller membership subscription."""

    def _start_replica_tracking(self):
        pass

    async def flush_reservation_broadcast(self):
        if self._reservation_forwarder is not None:
            await self._reservation_forwarder.flush()

    async def flush_reservation_updates(self):
        await self._reservation_updates.join()


def _llm_config(experimental_configs=None) -> LLMConfig:
    return LLMConfig(
        model_loading_config={
            "model_id": "qwen3-0.6b",
            "model_source": "Qwen/Qwen3-0.6B",
        },
        accelerator_type=None,
        experimental_configs=experimental_configs or {},
    )


def test_build_registers_tracker(monkeypatch):
    """build_kv_token_tracker builds the tracker with the explicit deployment
    id and registers it so the same-process router/ingress can reach it."""
    monkeypatch.setattr(kv_token_tracker, "KVTokenTracker", _SpyTracker)
    tracker = build_kv_token_tracker(_llm_config(), "dep-id")
    assert get_kv_token_tracker() is tracker
    assert tracker.serve_deployment_id == "dep-id"


def test_indexer_threads_from_config(monkeypatch):
    """The KV_INDEXER_THREADS experimental config reaches the router-local tracker."""
    monkeypatch.setattr(kv_token_tracker, "KVTokenTracker", _SpyTracker)
    tracker = build_kv_token_tracker(_llm_config({"KV_INDEXER_THREADS": 8}), "dep-id")
    assert tracker.indexer_threads == 8


def test_router_binds_tracker():
    """KVAwareRouter picks up the tracker the LLMRouter registered."""
    sentinel = object()
    set_kv_token_tracker(sentinel)
    router = KVAwareRouter.__new__(KVAwareRouter)
    router.initialize_state()
    assert router._kv_token_tracker is sentinel


def test_router_degrades_without_tracker():
    """A process with no router-local tracker (proxy fallback router) degrades to
    load-balanced selection rather than erroring."""
    router = KVAwareRouter.__new__(KVAwareRouter)
    router._deployment_id = "dep"
    router.initialize_state()
    assert router._kv_token_tracker is None


def _worker_load(tracker: KVTokenTracker, worker_id: int, field: str) -> int:
    """A booked-load field for ``worker_id`` in the tracker's selection service."""
    for model in tracker._svc.loads():
        for load in model["loads"]:
            if load["worker_id"] == worker_id:
                return load[field]
    return 0


def _active_requests(tracker: KVTokenTracker, worker_id: int) -> int:
    """Active requests the tracker's selection service books on ``worker_id``."""
    return _worker_load(tracker, worker_id, "active_requests")


class _TrackerBroadcastHandle:
    """Minimal DeploymentHandle broadcast surface for local trackers."""

    def __init__(self, *trackers):
        self._trackers = trackers
        self._tasks = set()

    def broadcast(self, method_name, *args):
        handle = self

        class _Response:
            async def results_async(self, *, timeout_s=None, return_exceptions=False):
                gather = asyncio.gather(
                    *[
                        getattr(tracker, method_name)(*args)
                        for tracker in handle._trackers
                    ],
                    return_exceptions=return_exceptions,
                )
                if timeout_s is None:
                    return list(await gather)
                return list(await asyncio.wait_for(gather, timeout=timeout_s))

            def _ensure_scheduled(self):
                task = asyncio.create_task(handle._run(method_name, *args))
                handle._tasks.add(task)
                task.add_done_callback(handle._tasks.discard)
                return task

        return _Response()

    async def _run(self, method_name, *args):
        for tracker in self._trackers:
            await getattr(tracker, method_name)(*args)

    async def flush(self):
        if self._tasks:
            await asyncio.gather(*self._tasks)


class _RecordingSelectionService:
    """Proxy a real SelectionService while recording reservation attempts."""

    def __init__(self, inner):
        self._inner = inner
        self.create_reservation_calls = []

    def __getattr__(self, name):
        return getattr(self._inner, name)

    async def create_reservation(self, request):
        self.create_reservation_calls.append(dict(request))
        return await self._inner.create_reservation(request)


@pytest.mark.asyncio
async def test_lifecycle_booking_end_to_end():
    """End-to-end router-local path on the real Dynamo selection service:
    select_and_reserve books load immediately, and engine lifecycle events move
    prefill to decode and then free the request."""
    pytest.importorskip("dynamo.llm")
    worker_id = get_worker_id("replica-0")

    tracker = _NoLongPollTracker()
    try:
        tracker._register_block_size(16, "replica-0")
        await tracker._upsert_worker(
            worker_id,
            "replica-0",
            {
                "endpoint": "tcp://127.0.0.1:59999",
                "block_size": 16,
                "max_num_batched_tokens": 8192,
                "dp_rank": 0,
            },
        )
        selection = await tracker.select_worker(
            "req-e2e", [1, 2, 3, 4], [worker_id], 20
        )
        assert selection["worker_id"] == worker_id
        assert "req-e2e" in tracker._requests
        assert _active_requests(tracker, worker_id) == 1

        router = LLMRouter.__new__(LLMRouter)
        router._kv_token_tracker = tracker
        await router.on_lifecycle_events([("on_prefill_complete", ("req-e2e",))])
        assert _active_requests(tracker, worker_id) == 1
        await router.on_lifecycle_events([("on_request_completed", ("req-e2e",))])
        assert "req-e2e" not in tracker._requests
        assert _active_requests(tracker, worker_id) == 0
    finally:
        if tracker._svc is not None:
            await tracker._svc.delete_worker(worker_id)


@pytest.mark.asyncio
async def test_peer_trackers_converge_through_request_lifecycle():
    """One atomic selection is replicated so every selection service books the
    same request, applies prefill completion, and frees it."""
    pytest.importorskip("dynamo.llm")

    routing = _NoLongPollTracker(ingress_replica_rank=0)
    peer = _NoLongPollTracker(ingress_replica_rank=1)
    trackers = (routing, peer)
    worker_id = get_worker_id("engine-replica-0")
    token_ids = list(range(64))
    try:
        # Both ingress replicas track the same LLMServer deployment, so the
        # same engine worker is registered on both.
        for tracker in trackers:
            tracker._register_block_size(16, "engine-replica-0")
            await tracker._upsert_worker(
                worker_id,
                "engine-replica-0",
                {
                    "endpoint": "tcp://127.0.0.1:59998",
                    "block_size": 16,
                    "max_num_batched_tokens": 8192,
                    "dp_rank": 0,
                },
            )

        # Exactly one ingress scores and reserves. Its background broadcast
        # tells every peer to book the already-selected worker.
        broadcast_handle = _TrackerBroadcastHandle(*trackers)
        routing.start_reservation_broadcast(broadcast_handle)
        selection = await routing.select_worker("req-bcast", token_ids, [worker_id], 32)
        assert selection["worker_id"] == worker_id
        assert _active_requests(routing, worker_id) == 1
        await routing.flush_reservation_broadcast()
        await broadcast_handle.flush()
        for tracker in trackers:
            await tracker.flush_reservation_updates()

        load_fields = (
            "active_requests",
            "potential_prefill_tokens",
            "potential_decode_blocks",
        )
        loads = [
            tuple(_worker_load(tracker, worker_id, field) for field in load_fields)
            for tracker in trackers
        ]
        assert len(set(loads)) == 1
        assert loads[0][0] == 1
        assert loads[0][1] > 0
        assert all("req-bcast" in tracker._requests for tracker in trackers)

        prefill = [("on_prefill_complete", ("req-bcast",))]
        for tracker in trackers:
            await tracker.on_lifecycle_events(prefill)
        assert all(_active_requests(tracker, worker_id) == 1 for tracker in trackers)
        assert all(
            _worker_load(tracker, worker_id, "potential_prefill_tokens") == 0
            for tracker in trackers
        )
        assert all(
            tracker._requests["req-bcast"].prefill_completed for tracker in trackers
        )

        completed = [("on_request_completed", ("req-bcast",))]
        for tracker in trackers:
            await tracker.on_lifecycle_events(completed)
        for tracker in trackers:
            assert _active_requests(tracker, worker_id) == 0
            assert "req-bcast" not in tracker._requests
    finally:
        if routing._reservation_forwarder is not None:
            routing._reservation_forwarder.close()
        for tracker in trackers:
            if tracker._reservation_apply_task is not None:
                tracker._reservation_apply_task.cancel()
            if tracker._svc is not None:
                await tracker._svc.delete_worker(worker_id)


@pytest.mark.asyncio
async def test_delayed_self_broadcast_does_not_resurrect_completed_request():
    """The selecting ingress must ignore its own reservation broadcast even if
    the request completed before that background broadcast is delivered."""
    pytest.importorskip("dynamo.llm")
    worker_id = get_worker_id("engine-replica-0")
    token_ids = list(range(64))

    tracker = _NoLongPollTracker(ingress_replica_rank=0)
    try:
        tracker._register_block_size(16, "engine-replica-0")
        await tracker._upsert_worker(
            worker_id,
            "engine-replica-0",
            {
                "endpoint": "tcp://127.0.0.1:59998",
                "block_size": 16,
                "max_num_batched_tokens": 8192,
                "dp_rank": 0,
            },
        )
        selection = await tracker.select_worker(
            "req-delayed-self", token_ids, [worker_id], 32
        )
        descriptor = {
            "request_id": "req-delayed-self",
            "source_ingress_replica_rank": 0,
            "worker_id": selection["worker_id"],
            "dp_rank": selection["dp_rank"],
            "sequence_hashes": [1],
            "isl_tokens": len(token_ids),
            "expected_output_tokens": 32,
            "effective_prefill_tokens": selection["effective_prefill_tokens"],
        }

        await tracker.on_request_completed("req-delayed-self")
        assert _active_requests(tracker, worker_id) == 0

        await tracker.on_reservations_created([descriptor])
        assert "req-delayed-self" not in tracker._requests
        assert _active_requests(tracker, worker_id) == 0
    finally:
        if tracker._svc is not None:
            await tracker._svc.delete_worker(worker_id)


@pytest.mark.asyncio
async def test_delayed_peer_broadcast_does_not_resurrect_completed_request():
    """A peer that sees completion before reservation admission must not
    recreate active load when the delayed reservation broadcast is applied."""
    pytest.importorskip("dynamo.llm")
    worker_id = get_worker_id("engine-replica-0")
    token_ids = list(range(64))

    routing = _NoLongPollTracker(ingress_replica_rank=0)
    peer = _NoLongPollTracker(ingress_replica_rank=1)
    trackers = (routing, peer)
    try:
        for tracker in trackers:
            tracker._register_block_size(16, "engine-replica-0")
            await tracker._upsert_worker(
                worker_id,
                "engine-replica-0",
                {
                    "endpoint": "tcp://127.0.0.1:59998",
                    "block_size": 16,
                    "max_num_batched_tokens": 8192,
                    "dp_rank": 0,
                },
            )

        selection = await routing.select_worker(
            "req-delayed-peer", token_ids, [worker_id], 32
        )
        peer._svc = _RecordingSelectionService(peer._svc)
        descriptor = {
            "request_id": "req-delayed-peer",
            "source_ingress_replica_rank": 0,
            "worker_id": selection["worker_id"],
            "dp_rank": selection["dp_rank"],
            "sequence_hashes": [1],
            "isl_tokens": len(token_ids),
            "expected_output_tokens": 32,
            "effective_prefill_tokens": selection["effective_prefill_tokens"],
        }

        await peer.on_request_completed("req-delayed-peer")
        await peer.on_reservations_created([descriptor])
        await peer.flush_reservation_updates()

        assert "req-delayed-peer" not in peer._requests
        assert _active_requests(peer, worker_id) == 0
        assert peer._svc.create_reservation_calls == []
    finally:
        for tracker in trackers:
            if tracker._reservation_apply_task is not None:
                tracker._reservation_apply_task.cancel()
            if tracker._svc is not None:
                await tracker._svc.delete_worker(worker_id)


# ---- LongPoll replica-membership tracking -----------------------------------


@pytest.fixture(scope="module")
def serve_instance():
    if not ray.is_initialized():
        ray.init()
    yield
    serve.shutdown()


_REPLICATION_APP_NAME = "kv-reservation-replication-test"
_REPLICATION_WORKER_ID = get_worker_id("replicated-engine-worker")


@serve.deployment(num_replicas=2)
class ReservationReplicationDeployment:
    """Two real Serve replicas, each with an independent selection service."""

    async def __init__(self):
        self._tracker = _NoLongPollTracker()
        self._tracker._register_block_size(16, "replicated-engine-worker")
        await self._tracker._upsert_worker(
            _REPLICATION_WORKER_ID,
            "replicated-engine-worker",
            {
                "endpoint": "tcp://127.0.0.1:59997",
                "block_size": 16,
                "max_num_batched_tokens": 8192,
                "dp_rank": 0,
            },
        )
        handle = serve.get_deployment_handle(
            "ReservationReplicationDeployment",
            app_name=serve.get_replica_context().app_name,
        )
        self._tracker.start_reservation_broadcast(handle)

    async def select_worker(self, request_id, token_ids):
        return await self._tracker.select_worker(
            request_id, token_ids, [_REPLICATION_WORKER_ID]
        )

    async def on_reservations_created(self, reservations):
        await self._tracker.on_reservations_created(reservations)

    async def on_lifecycle_events(self, events):
        await self._tracker.on_lifecycle_events(events)

    async def get_request_state(self, request_id):
        state = self._tracker._requests.get(request_id)
        return None if state is None else state.prefill_completed

    async def get_worker_load(self):
        return {
            field: _worker_load(self._tracker, _REPLICATION_WORKER_ID, field)
            for field in (
                "active_requests",
                "potential_prefill_tokens",
                "potential_decode_blocks",
            )
        }


async def _broadcast(handle, method_name, *args):
    return await handle.broadcast(method_name, *args).results_async()


@pytest.mark.asyncio
async def test_serve_replicas_converge_through_request_lifecycle(serve_instance):
    """The production Serve self-broadcast keeps independent services aligned."""
    pytest.importorskip("dynamo.llm")
    module = sys.modules[__name__]
    ray.cloudpickle.register_pickle_by_value(module)
    request_id = "req-serve-broadcast"
    try:
        handle = serve.run(
            ReservationReplicationDeployment.bind(),
            name=_REPLICATION_APP_NAME,
            route_prefix=None,
        )
        # A normal handle call lands on exactly one replica. That replica makes
        # the atomic decision and broadcasts its already-selected booking.
        selection = await handle.select_worker.remote(request_id, list(range(64)))
        assert selection["worker_id"] == _REPLICATION_WORKER_ID

        async def reservation_converged():
            states = await _broadcast(handle, "get_request_state", request_id)
            return len(states) == 2 and states == [False, False]

        await async_wait_for_condition(
            reservation_converged, timeout=30, retry_interval_ms=100
        )
        loads = await _broadcast(handle, "get_worker_load")
        assert loads[0] == loads[1]
        assert loads[0]["active_requests"] == 1
        assert loads[0]["potential_prefill_tokens"] > 0

        await _broadcast(
            handle,
            "on_lifecycle_events",
            [("on_prefill_complete", (request_id,))],
        )
        assert await _broadcast(handle, "get_request_state", request_id) == [True, True]
        loads = await _broadcast(handle, "get_worker_load")
        assert all(load["potential_prefill_tokens"] == 0 for load in loads)

        await _broadcast(
            handle,
            "on_lifecycle_events",
            [("on_request_completed", (request_id,))],
        )
        assert await _broadcast(handle, "get_request_state", request_id) == [None, None]
        loads = await _broadcast(handle, "get_worker_load")
        assert all(load["active_requests"] == 0 for load in loads)
    finally:
        serve.delete(_REPLICATION_APP_NAME)
        ray.cloudpickle.unregister_pickle_by_value(module)


@serve.deployment
class ReplicaTrackingDeployment:
    """Dummy deployment that advertises a per-replica KV-events endpoint via
    ``record_routing_stats`` as a real engine replica would, so a KVTokenTracker
    watching this deployment registers each replica as a worker."""

    async def __call__(self) -> str:
        return "ok"

    async def record_routing_stats(self) -> dict:
        rank = serve.get_replica_context().rank.local_rank
        return {
            "kv_event_metadata": {
                "endpoint": f"tcp://127.0.0.1:{25000 + rank}",
                "block_size": 16,
                "max_num_batched_tokens": 8192,
                "dp_rank": 0,
            }
        }


class _MembershipTracker(KVTokenTracker):
    """Real LongPoll replica tracking with the selection service disabled, so the
    DEPLOYMENT_TARGETS subscription and reconciliation are exercised without
    dynamo."""

    def _create_selection_service(self) -> None:
        self._svc = None


@pytest.mark.asyncio
async def test_longpoll_tracks_membership(serve_instance):
    """The tracker's LongPoll subscription reconciles its tracked workers to the
    deployment's live replicas across scale up and down -- the same membership
    tracking the KVTokenTracker does inside the LLMRouter ingress replica."""
    app_name = "kv-replica-tracking"

    def deploy(num_replicas):
        serve.run(
            ReplicaTrackingDeployment.options(num_replicas=num_replicas).bind(),
            name=app_name,
            route_prefix="/kv_track",
        )

    deploy(2)
    # Build the tracker the way the LLMRouter ingress would, pointed at the dummy
    # deployment; its LongPollClient binds to this test's event loop.
    tracker = _MembershipTracker(
        serve_deployment_id=DeploymentID(
            name="ReplicaTrackingDeployment", app_name=app_name
        )
    )
    try:
        await async_wait_for_condition(
            lambda: len(tracker._replica_id_by_worker) == 2, timeout=30
        )
        deploy(4)  # upscale: the new replicas are picked up over LongPoll
        await async_wait_for_condition(
            lambda: len(tracker._replica_id_by_worker) == 4, timeout=30
        )
        deploy(2)  # downscale: the departed replicas are dropped
        await async_wait_for_condition(
            lambda: len(tracker._replica_id_by_worker) == 2, timeout=30
        )
    finally:
        serve.delete(app_name, _blocking=True)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
