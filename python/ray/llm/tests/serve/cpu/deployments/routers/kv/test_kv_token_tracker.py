"""Validation for the router-local KV selector: build + process-global
registration, ``KVAwareRouter`` binding to it, and the
``LLMRouter`` -> ``KVTokenTracker`` -> Dynamo selection-service booking path.
"""

import asyncio
import sys
from typing import List
from unittest.mock import MagicMock

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
from ray.serve.config import RequestRouterConfig
from ray.serve.llm.request_router import KVAwareRouter


@pytest.fixture(autouse=True)
def reset_process_global():
    set_kv_token_tracker(None)
    yield
    set_kv_token_tracker(None)


class _SpyTracker:
    """Records constructor kwargs; skips the real selection service + LongPoll."""

    def __init__(
        self,
        *,
        indexer_threads,
        serve_deployment_id,
        session_affinity_ttl_s,
        router_handle_id,
        router_actor_id,
    ):
        self.session_affinity_ttl_s = session_affinity_ttl_s
        self.indexer_threads = indexer_threads
        self.serve_deployment_id = serve_deployment_id
        self.router_handle_id = router_handle_id
        self.router_actor_id = router_actor_id


class _NoLongPollTracker(KVTokenTracker):
    """Real selection service without a controller membership subscription."""

    def _start_replica_tracking(self):
        pass

    async def flush_replica_broadcast(self):
        if self._replica_forwarder is not None:
            await self._replica_forwarder.flush()

    async def flush_replica_updates(self):
        await self._replica_updates.join()

    def stop_replica_broadcast(self):
        if self._replica_forwarder is not None:
            self._replica_forwarder.close()
        for task in (self._session_affinity_task, self._replica_apply_task):
            if task is not None:
                task.cancel()


_SESSION = "session-a"
_WORKER_ID = get_worker_id("engine-replica-0")


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
    tracker = build_kv_token_tracker(
        _llm_config(), "dep-id", router_handle_id="handle", router_actor_id="actor"
    )
    assert get_kv_token_tracker() is tracker
    assert tracker.serve_deployment_id == "dep-id"
    assert tracker.session_affinity_ttl_s == 1800
    assert tracker.router_handle_id == "handle"
    assert tracker.router_actor_id == "actor"


@pytest.fixture
def tracker_with_metrics(monkeypatch):
    gauge = MagicMock()
    monkeypatch.setattr(
        kv_token_tracker.metrics, "Gauge", MagicMock(return_value=gauge)
    )
    monkeypatch.setattr(KVTokenTracker, "_create_selection_service", lambda self: None)
    monkeypatch.setattr(KVTokenTracker, "_start_replica_tracking", lambda self: None)
    tracker = KVTokenTracker(
        serve_deployment_id=DeploymentID("model", "app"),
        router_handle_id="handle",
        router_actor_id="actor",
    )
    return tracker, gauge


def test_tracker_initializes_prefill_gauge(tracker_with_metrics):
    _, gauge = tracker_with_metrics
    assert kv_token_tracker.metrics.Gauge.call_args.args == (
        "serve_router_uncached_prefill_tokens",
    )
    gauge.set_default_tags.assert_called_once_with(
        {
            "deployment": "model",
            "application": "app",
            "handle": "handle",
            "actor_id": "actor",
        }
    )
    gauge.set.assert_called_once_with(0)


def test_tracker_publishes_prefill_gauge_and_autoscaling_snapshot(tracker_with_metrics):
    tracker, gauge = tracker_with_metrics
    tracker._outstanding_effective_prefill_tokens = 1000
    tracker._publish_autoscaling_snapshot()
    gauge.set.assert_called_with(1000)

    published = []
    tracker.set_autoscaling_metrics_callback(published.append)
    for tokens in (300, 0):
        tracker._outstanding_effective_prefill_tokens = tokens
        tracker._publish_autoscaling_snapshot()
        gauge.set.assert_called_with(tokens)
        assert published[-1] == tracker._get_autoscaling_snapshot()


def test_ingress_passes_router_identity_to_tracker(monkeypatch):
    from ray.llm._internal.serve.routing_policies.kv_aware import token_channel

    builder = MagicMock()
    monkeypatch.setattr(kv_token_tracker, "build_kv_token_tracker", builder)
    monkeypatch.setattr(kv_token_tracker, "get_llm_router_handle", MagicMock())
    monkeypatch.setattr(token_channel, "TokenSender", MagicMock())
    monkeypatch.setattr(
        ray, "get_runtime_context", lambda: MagicMock(get_actor_id=lambda: "actor")
    )
    server = MagicMock(deployment_id=DeploymentID("model", "app"), handle_id="handle")
    config = _llm_config()
    router = LLMRouter.__new__(LLMRouter)
    router._start_kv_token_tracking(config, server)
    builder.assert_called_once_with(
        config, server.deployment_id, router_handle_id="handle", router_actor_id="actor"
    )


def test_indexer_threads_from_config(monkeypatch):
    """The KV_INDEXER_THREADS experimental config reaches the router-local tracker."""
    monkeypatch.setattr(kv_token_tracker, "KVTokenTracker", _SpyTracker)
    tracker = build_kv_token_tracker(_llm_config({"KV_INDEXER_THREADS": 8}), "dep-id")
    assert tracker.indexer_threads == 8


@pytest.mark.parametrize("ttl_s,as_object", [(60.0, False), (0, True)])
def test_session_ttl_config(monkeypatch, ttl_s, as_object):
    monkeypatch.setattr(kv_token_tracker, "KVTokenTracker", _SpyTracker)
    config = _llm_config()
    router_config = {
        "request_router_class": KVAwareRouter,
        "request_router_kwargs": {"session_affinity_ttl_s": ttl_s},
    }
    config.deployment_config["request_router_config"] = (
        RequestRouterConfig(**router_config) if as_object else router_config
    )
    tracker = build_kv_token_tracker(config, "dep-id")
    assert tracker.session_affinity_ttl_s == ttl_s


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


@pytest.mark.asyncio
async def test_llm_router_connects_autoscaling_metrics_callback(monkeypatch):
    """The ingress forwards tracker snapshots into its deployment handle."""
    tracker = MagicMock()

    def start_kv_token_tracking(router, _llm_config, _server):
        router._kv_token_tracker = tracker

    async def skip_tokenizer_init(_func, *_args):
        return object()

    monkeypatch.setattr(LLMRouter, "_start_kv_token_tracking", start_kv_token_tracking)
    monkeypatch.setattr(asyncio, "to_thread", skip_tokenizer_init)

    server = MagicMock()
    router = LLMRouter.__new__(LLMRouter)
    await router.__init__(server, _llm_config())

    server._init.assert_called_once_with()
    tracker.set_autoscaling_metrics_callback.assert_called_once_with(
        server._set_router_autoscaling_metrics
    )


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
        self.updates = []

    def broadcast(self, method_name, *args):
        if method_name == "on_replica_updates":
            self.updates.extend(args[1])
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

        # Peers apply broadcasts in background tasks, so wait until every
        # tracker holds the same requests.
        def converged():
            expected = {
                request_id
                for tracker in self._trackers
                for request_id in tracker._requests
            }
            return all(set(tracker._requests) == expected for tracker in self._trackers)

        await async_wait_for_condition(converged, timeout=5, retry_interval_ms=10)


class _RecordingSelectionService:
    """Proxy a real SelectionService while recording reservation attempts."""

    def __init__(self, inner):
        self._inner = inner
        self.reservation_calls = []

    def __getattr__(self, name):
        return getattr(self._inner, name)

    async def create_reservation(self, request):
        self.reservation_calls.append(request)
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

    routing = _NoLongPollTracker()
    peer = _NoLongPollTracker()
    trackers = (routing, peer)
    worker_id = get_worker_id("engine-replica-0")
    token_ids = list(range(63))
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
        routing.start_replica_broadcast(broadcast_handle)
        peer._svc = _RecordingSelectionService(peer._svc)
        selection = await routing.select_worker(
            "req-bcast", token_ids, [worker_id], 32, lora_name="adapter-a"
        )
        assert selection["worker_id"] == worker_id
        assert _active_requests(routing, worker_id) == 1
        await routing.flush_replica_broadcast()
        await broadcast_handle.flush()
        for tracker in trackers:
            await tracker.flush_replica_updates()

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
        assert peer._svc.reservation_calls[0]["lora_name"] == "adapter-a"

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

        # Exact prompt length matters at the next block boundary: 63 + 1
        # tokens still occupy 4 blocks; the second output token needs 5.
        for output_tokens, blocks in [(1, 4), (2, 5)]:
            for tracker in trackers:
                await tracker.on_decode_progress("req-bcast", output_tokens)
                assert tracker._requests["req-bcast"].prompt_tokens == 63
                assert tracker._requests["req-bcast"].total_blocks == blocks
            assert _worker_load(
                routing, worker_id, "potential_decode_blocks"
            ) == _worker_load(peer, worker_id, "potential_decode_blocks")

        completed = [("on_request_completed", ("req-bcast",))]
        for tracker in trackers:
            await tracker.on_lifecycle_events(completed)
        for tracker in trackers:
            assert _active_requests(tracker, worker_id) == 0
            assert "req-bcast" not in tracker._requests
    finally:
        routing.stop_replica_broadcast()
        for tracker in trackers:
            if tracker._replica_apply_task is not None:
                tracker._replica_apply_task.cancel()
            if tracker._svc is not None:
                await tracker._svc.delete_worker(worker_id)


@pytest.mark.asyncio
async def test_delayed_self_broadcast_does_not_resurrect_completed_request():
    """The selecting ingress must ignore its own reservation broadcast even if
    the request completed before that background broadcast is delivered."""
    pytest.importorskip("dynamo.llm")
    worker_id = get_worker_id("engine-replica-0")
    token_ids = list(range(64))

    tracker = _NoLongPollTracker()
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
        handle = _TrackerBroadcastHandle()
        tracker.start_replica_broadcast(handle)
        await tracker.select_worker("req-delayed-self", token_ids, [worker_id], 32)
        await tracker.flush_replica_broadcast()

        await tracker.on_request_completed("req-delayed-self")
        assert _active_requests(tracker, worker_id) == 0

        await tracker.on_replica_updates(tracker._replication_writer_id, handle.updates)
        assert "req-delayed-self" not in tracker._requests
        assert _active_requests(tracker, worker_id) == 0
    finally:
        tracker.stop_replica_broadcast()
        if tracker._svc is not None:
            await tracker._svc.delete_worker(worker_id)


async def _broadcast_one_selection(session_id=None):
    """Select on a routing tracker and return a peer plus the broadcast updates."""
    routing, peer = _NoLongPollTracker(), _NoLongPollTracker()
    for tracker in (routing, peer):
        await _register_workers(tracker, ["engine-replica-0"])
    handle = _TrackerBroadcastHandle()
    routing.start_replica_broadcast(handle)
    await routing.select_worker(
        "req", _turn(1), [_WORKER_ID], 32, session_id=session_id
    )
    expected = 2 if session_id else 1
    await async_wait_for_condition(lambda: len(handle.updates) == expected, timeout=5)
    await routing.flush_replica_broadcast()
    peer._svc = _RecordingSelectionService(peer._svc)
    updates = {update["kind"]: update for update in handle.updates}
    return routing, peer, updates


async def _apply_from(source, peer, updates) -> None:
    """Deliver ``updates`` from ``source`` through the peer's LLMRouter intake."""
    router = LLMRouter.__new__(LLMRouter)
    router._kv_token_tracker = peer
    await router.on_replica_updates(source._replication_writer_id, updates)
    await peer.flush_replica_updates()


@pytest.mark.asyncio
async def test_delayed_peer_broadcast_does_not_resurrect_completed_request():
    """A peer that sees completion before reservation admission must not
    recreate active load when the delayed reservation broadcast is applied."""
    pytest.importorskip("dynamo.llm")
    routing, peer, updates = await _broadcast_one_selection()
    try:
        await peer.on_request_completed("req")
        await _apply_from(routing, peer, [updates["load_reservation"]])

        assert "req" not in peer._requests
        assert _active_requests(peer, _WORKER_ID) == 0
        assert peer._svc.reservation_calls == []
    finally:
        for tracker in (routing, peer):
            tracker.stop_replica_broadcast()
            await _delete_workers(tracker, [_WORKER_ID])


@pytest.mark.asyncio
async def test_binding_survives_failed_booking():
    """A failed load reservation does not drop the binding batched with it."""
    pytest.importorskip("dynamo.llm")
    routing, peer, updates = await _broadcast_one_selection(session_id=_SESSION)
    peer_bindings = peer._svc.session_binding_updates()
    try:
        updates["load_reservation"]["event"]["effective_prefill_tokens"] = -1
        await _apply_from(
            routing, peer, [updates["load_reservation"], updates["session_affinity"]]
        )

        # The next turn commits the owner's binding rather than a new one.
        await peer.select_worker(
            "next-turn", _turn(2), [_WORKER_ID], 32, session_id=_SESSION
        )
        assert await anext(peer_bindings) == [updates["session_affinity"]["event"]]
    finally:
        for tracker in (routing, peer):
            tracker.stop_replica_broadcast()
            await _delete_workers(tracker, [_WORKER_ID])


# ---- Session affinity -------------------------------------------------------


async def _register_workers(tracker: KVTokenTracker, replica_ids) -> List[int]:
    """Register one engine worker per replica id on ``tracker``."""
    worker_ids = []
    for port, replica_id in enumerate(replica_ids, start=59900):
        worker_id = get_worker_id(replica_id)
        tracker._register_block_size(16, replica_id)
        await tracker._upsert_worker(
            worker_id,
            replica_id,
            {
                "endpoint": f"tcp://127.0.0.1:{port}",
                "block_size": 16,
                "max_num_batched_tokens": 8192,
                "dp_rank": 0,
            },
        )
        worker_ids.append(worker_id)
    return worker_ids


async def _delete_workers(tracker: KVTokenTracker, worker_ids) -> None:
    if tracker._svc is not None:
        for worker_id in worker_ids:
            await tracker._svc.delete_worker(worker_id)


async def _load_worker(tracker, worker_id, count, prefix="load") -> None:
    """Book ``count`` long non-session requests on ``worker_id`` only, so normal
    scoring prefers any other worker."""
    for i in range(count):
        await tracker.select_worker(
            f"{prefix}-{worker_id}-{i}",
            list(range(10_000 * (i + 1), 10_000 * (i + 1) + 4096)),
            [worker_id],
            32,
        )


def _turn(n: int) -> List[int]:
    """A short prompt with a distinct prefix."""
    return list(range(n * 100, n * 100 + 32))


def _other(worker_ids, worker_id) -> int:
    return next(w for w in worker_ids if w != worker_id)


@pytest.mark.asyncio
async def test_session_pinning_disabled():
    """With a zero TTL, a session's later turns follow load like unpinned ones."""
    pytest.importorskip("dynamo.llm")
    tracker = _NoLongPollTracker(session_affinity_ttl_s=0)
    worker_ids = await _register_workers(tracker, ["sess-r0", "sess-r1"])
    try:
        first = await tracker.select_worker(
            "turn-1", _turn(1), worker_ids, 32, session_id=_SESSION
        )
        bound = first["worker_id"]
        await _load_worker(tracker, bound, 3)
        second = await tracker.select_worker(
            "turn-2", _turn(2), worker_ids, 32, session_id=_SESSION
        )
        assert second["worker_id"] == _other(worker_ids, bound)
    finally:
        await _delete_workers(tracker, worker_ids)


@pytest.mark.asyncio
async def test_session_bindings_forwarded_without_drops():
    """Every session binding reaches the peer broadcast under selection bursts,
    which overflow Dynamo's 4,096-slot queue if read one binding at a time."""
    pytest.importorskip("dynamo.llm")
    tracker = _NoLongPollTracker()
    worker_ids = await _register_workers(tracker, ["engine-replica-0"])
    handle = _TrackerBroadcastHandle()
    tracker.start_replica_broadcast(handle)
    bursts, per_burst = 8, 1000
    expected = bursts * per_burst
    try:
        for burst in range(bursts):
            await asyncio.gather(
                *(
                    tracker.select_worker(
                        f"req-{burst}-{i}",
                        _turn(1),
                        worker_ids,
                        32,
                        session_id=f"session-{burst}-{i}",
                    )
                    for i in range(per_burst)
                )
            )

        def all_forwarded():
            forwarded = sum(u["kind"] == "session_affinity" for u in handle.updates)
            assert forwarded == expected, f"{forwarded}/{expected} bindings forwarded"
            return True

        await async_wait_for_condition(all_forwarded, timeout=10)
    finally:
        tracker.stop_replica_broadcast()
        await _delete_workers(tracker, worker_ids)


class _SlowResumeSelectionService:
    """Book on the real service, then hold the result until ``release`` is set,
    standing in for an ingress event loop too busy to resume the caller."""

    def __init__(self, inner):
        self._inner = inner
        self.booked = asyncio.Event()
        self.release = asyncio.Event()

    def __getattr__(self, name):
        return getattr(self._inner, name)

    async def select_and_reserve(self, request):
        selection = await self._inner.select_and_reserve(request)
        self.booked.set()
        await self.release.wait()
        return selection


@pytest.mark.asyncio
async def test_cancelled_selection_frees_its_booking():
    """A route cancelled after the selection service booked it (HAProxy gave up
    on a slow router) must not leave untracked load booked."""
    pytest.importorskip("dynamo.llm")
    worker_id = get_worker_id("engine-replica-0")

    tracker = _NoLongPollTracker()
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
        tracker._svc = _SlowResumeSelectionService(tracker._svc)

        select = asyncio.create_task(
            tracker.select_worker("req-abandoned", list(range(64)), [worker_id], 32)
        )
        await tracker._svc.booked.wait()
        assert _active_requests(tracker, worker_id) == 1
        select.cancel()
        with pytest.raises(asyncio.CancelledError):
            await select
        tracker._svc.release.set()
        await async_wait_for_condition(
            lambda: _active_requests(tracker, worker_id) == 0,
            timeout=5,
            retry_interval_ms=10,
        )
    finally:
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
        self._tracker.start_replica_broadcast(handle)

    async def select_worker(self, request_id, token_ids):
        return await self._tracker.select_worker(
            request_id, token_ids, [_REPLICATION_WORKER_ID]
        )

    async def on_replica_updates(self, source_writer_id, updates):
        await self._tracker.on_replica_updates(source_writer_id, updates)

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
