"""Validation for the router-local KV selector: build + process-global
registration, ``KVAwareRouter`` binding to it, and the
``LLMRouter`` -> ``KVTokenTracker`` -> Dynamo selection-service booking path.
"""

import sys

import pytest

import ray
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


@pytest.mark.asyncio
async def test_lifecycle_booking_end_to_end():
    """End-to-end router-local path on the real Dynamo selection service: select a
    worker, then book and free the request through LLMRouter.on_lifecycle_events
    (the engine-facing handle). Exercises the LLMRouter -> KVTokenTracker
    delegation and the ray<->dynamo booking API the mocked tests cannot."""
    pytest.importorskip("dynamo.llm")
    worker_id = get_worker_id("replica-0")

    class _NoLongPollTracker(KVTokenTracker):
        # Real SelectionService, but no Serve LongPoll (no controller in a unit test).
        def _start_replica_tracking(self):
            pass

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
        selection = await tracker.select_worker("req-e2e", [1, 2, 3, 4], [worker_id])
        assert selection["worker_id"] == worker_id

        router = LLMRouter.__new__(LLMRouter)
        router._kv_token_tracker = tracker
        await router.on_lifecycle_events(
            [("on_request_added", ("req-e2e", worker_id, [1, 2, 3, 4], 20))]
        )
        assert "req-e2e" in tracker._requests
        # Assert the booking landed in the selection service, not just the
        # local view: on_lifecycle_events swallows hook errors by design.
        assert _active_requests(tracker, worker_id) == 1
        await router.on_lifecycle_events([("on_request_completed", ("req-e2e",))])
        assert "req-e2e" not in tracker._requests
        assert _active_requests(tracker, worker_id) == 0
    finally:
        if tracker._svc is not None:
            await tracker._svc.delete_worker(worker_id)


@pytest.mark.asyncio
async def test_peer_tracker_books_same_load():
    """A tracker that did not route a request books the same load from the broadcast
    admission event as the one that did, and a completion frees it on both."""
    pytest.importorskip("dynamo.llm")

    class _NoLongPollTracker(KVTokenTracker):
        # Real SelectionService, but no Serve LongPoll (no controller in a unit test).
        def _start_replica_tracking(self):
            pass

    routing = _NoLongPollTracker()
    peer = _NoLongPollTracker()
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

        # Only the routing tracker scores the request; the engine's admission
        # event is broadcast to both.
        selection = await routing.select_worker("req-bcast", token_ids, [worker_id])
        assert selection["worker_id"] == worker_id

        added = [("on_request_added", ("req-bcast", worker_id, token_ids, 32))]
        for tracker in trackers:
            await tracker.on_lifecycle_events(added)
        for tracker in trackers:
            assert _active_requests(tracker, worker_id) == 1
        # The peer's locally scored prefill load matches the routing replica's.
        assert _worker_load(peer, worker_id, "potential_prefill_tokens") == (
            _worker_load(routing, worker_id, "potential_prefill_tokens")
        )
        assert _worker_load(peer, worker_id, "potential_prefill_tokens") > 0

        completed = [("on_request_completed", ("req-bcast",))]
        for tracker in trackers:
            await tracker.on_lifecycle_events(completed)
        for tracker in trackers:
            assert _active_requests(tracker, worker_id) == 0
    finally:
        for tracker in trackers:
            if tracker._svc is not None:
                await tracker._svc.delete_worker(worker_id)


# ---- LongPoll replica-membership tracking -----------------------------------


@pytest.fixture(scope="module")
def serve_instance():
    if not ray.is_initialized():
        ray.init()
    yield
    serve.shutdown()


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
