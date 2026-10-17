"""Validation for the router-local KV selector: build + process-global
registration, ``KVAwareRouter`` binding to it, and the
``LLMRouter`` -> ``KVTokenTracker`` -> Dynamo selection-service booking path.
"""

import asyncio
from collections import OrderedDict
import sys
import types
from typing import List

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
        self.calls = []

    def broadcast(self, method_name, *args):
        handle = self
        self.calls.append((method_name, args))

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
        self.select_and_reserve_calls = []

    def __getattr__(self, name):
        return getattr(self._inner, name)

    async def select_and_reserve(self, request):
        self.select_and_reserve_calls.append(dict(request))
        return await self._inner.select_and_reserve(request)

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
        peer._svc = _RecordingSelectionService(peer._svc)
        selection = await routing.select_worker(
            "req-bcast", token_ids, [worker_id], 32, lora_name="adapter-a"
        )
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
        assert peer._svc.create_reservation_calls[0]["lora_name"] == "adapter-a"

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
            "lora_name": None,
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
            "lora_name": None,
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


# ---- Session affinity -------------------------------------------------------

_SESSION = "session-a"
_SESSION_TTL_S = 1800.0


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
    """Short prompt for a session turn, so load (not prompt size) dominates."""
    return list(range(n * 100, n * 100 + 32))


def _other(worker_ids, worker_id) -> int:
    return next(w for w in worker_ids if w != worker_id)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "control_kwargs",
    [
        {},
        {"session_id": _SESSION, "session_affinity_ttl_s": 0},
        {"session_id": "session-b", "session_affinity_ttl_s": _SESSION_TTL_S},
    ],
    ids=["no-session", "ttl-zero", "new-session"],
)
async def test_session_later_turn_sticks_to_loaded_worker(control_kwargs):
    """A session's later turn stays on the worker that served its first turn
    even when that worker is more loaded; requests without an active binding
    are scored normally and go to the less-loaded worker."""
    pytest.importorskip("dynamo.llm")
    tracker = _NoLongPollTracker()
    worker_ids = await _register_workers(tracker, ["sess-r0", "sess-r1"])
    try:
        tracker._svc = _RecordingSelectionService(tracker._svc)
        first = await tracker.select_worker(
            "turn-1",
            _turn(1),
            worker_ids,
            32,
            session_id=_SESSION,
            session_affinity_ttl_s=_SESSION_TTL_S,
        )
        bound = first["worker_id"]
        other = _other(worker_ids, bound)
        # The first turn has no binding yet, so it is scored normally.
        assert "affinity_target" not in tracker._svc.select_and_reserve_calls[0]
        assert tracker._session_workers[_SESSION][0] == bound

        await _load_worker(tracker, bound, 3)
        assert _active_requests(tracker, bound) > _active_requests(tracker, other)

        second = await tracker.select_worker(
            "turn-2",
            _turn(2),
            worker_ids,
            32,
            session_id=_SESSION,
            session_affinity_ttl_s=_SESSION_TTL_S,
        )
        assert second["worker_id"] == bound
        assert tracker._svc.select_and_reserve_calls[-1]["affinity_target"] == {
            "worker_id": bound
        }
        # Dynamo still books the affinity turn's load on the bound worker.
        assert _active_requests(tracker, bound) == 5

        control = await tracker.select_worker(
            "control", _turn(3), worker_ids, 32, **control_kwargs
        )
        assert control["worker_id"] == other
        assert "affinity_target" not in tracker._svc.select_and_reserve_calls[-1]
        # A disabled-affinity request neither consults nor rebinds the session.
        assert tracker._session_workers[_SESSION][0] == bound
    finally:
        await _delete_workers(tracker, worker_ids)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "max_imbalance,num_load_requests,stays",
    [
        (3, 3, True),
        (2, 2, True),
        (2, 3, False),
        (0, 1, False),
    ],
)
async def test_session_max_imbalance(max_imbalance, num_load_requests, stays):
    """A session leaves its worker only once that worker has more than
    ``max_imbalance`` in-flight requests above the least-loaded allowed
    worker, and is rebound to the new choice when it moves."""
    pytest.importorskip("dynamo.llm")
    tracker = _NoLongPollTracker()
    worker_ids = await _register_workers(tracker, ["imb-r0", "imb-r1"])
    affinity = {
        "session_id": _SESSION,
        "session_affinity_ttl_s": _SESSION_TTL_S,
        "session_affinity_max_imbalance": max_imbalance,
    }
    try:
        first = await tracker.select_worker(
            "turn-1", _turn(1), worker_ids, 32, **affinity
        )
        bound = first["worker_id"]
        other = _other(worker_ids, bound)
        # Finish the first turn so in-flight counts are just the extra load.
        await tracker.on_request_completed("turn-1")
        await _load_worker(tracker, bound, num_load_requests)
        assert len(tracker._request_ids_by_worker[bound]) == num_load_requests
        assert not tracker._request_ids_by_worker.get(other)

        second = await tracker.select_worker(
            "turn-2", _turn(2), worker_ids, 32, **affinity
        )
        expected = bound if stays else other
        assert second["worker_id"] == expected
        assert tracker._session_workers[_SESSION][0] == expected
    finally:
        await _delete_workers(tracker, worker_ids)


@pytest.mark.asyncio
async def test_session_bound_worker_not_allowed_rebinds():
    """When the bound worker is not a candidate (departed, or filtered out for
    a LoRA adapter), the turn is scored normally and the session is rebound."""
    pytest.importorskip("dynamo.llm")
    tracker = _NoLongPollTracker()
    worker_ids = await _register_workers(tracker, ["allow-r0", "allow-r1", "allow-r2"])
    affinity = {"session_id": _SESSION, "session_affinity_ttl_s": _SESSION_TTL_S}
    try:
        tracker._svc = _RecordingSelectionService(tracker._svc)
        bound = worker_ids[0]
        await tracker.select_worker("turn-1", _turn(1), [bound], 32, **affinity)
        assert tracker._session_workers[_SESSION][0] == bound

        # Of the remaining candidates, the loaded one loses normal scoring.
        loaded, idle = worker_ids[1], worker_ids[2]
        await _load_worker(tracker, loaded, 2)
        second = await tracker.select_worker(
            "turn-2", _turn(2), [loaded, idle], 32, **affinity
        )
        assert "affinity_target" not in tracker._svc.select_and_reserve_calls[-1]
        assert second["worker_id"] == idle
        assert tracker._session_workers[_SESSION][0] == idle

        # The next turn follows the new binding even with the old worker back.
        third = await tracker.select_worker(
            "turn-3", _turn(3), worker_ids, 32, **affinity
        )
        assert tracker._svc.select_and_reserve_calls[-1]["affinity_target"] == {
            "worker_id": idle
        }
        assert third["worker_id"] == idle
    finally:
        await _delete_workers(tracker, worker_ids)


class _FakeClock:
    def __init__(self):
        self.now = 1000.0

    def monotonic(self) -> float:
        return self.now


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "elapsed_s,stays",
    [(4.0, True), (5.0, False), (6.0, False)],
)
async def test_session_affinity_ttl_expiry(monkeypatch, elapsed_s, stays):
    """A binding idle for the TTL or longer is dropped, so the turn is scored
    normally and rebound; within the TTL it keeps its affinity target."""
    pytest.importorskip("dynamo.llm")
    clock = _FakeClock()
    monkeypatch.setattr(
        kv_token_tracker,
        "time",
        types.SimpleNamespace(monotonic=clock.monotonic, time=clock.monotonic),
    )
    ttl_s = 5.0
    tracker = _NoLongPollTracker()
    worker_ids = await _register_workers(tracker, ["ttl-r0", "ttl-r1"])
    affinity = {"session_id": _SESSION, "session_affinity_ttl_s": ttl_s}
    try:
        tracker._svc = _RecordingSelectionService(tracker._svc)
        first = await tracker.select_worker(
            "turn-1", _turn(1), worker_ids, 32, **affinity
        )
        bound = first["worker_id"]
        other = _other(worker_ids, bound)
        await _load_worker(tracker, bound, 3)

        clock.now += elapsed_s
        second = await tracker.select_worker(
            "turn-2", _turn(2), worker_ids, 32, **affinity
        )
        request = tracker._svc.select_and_reserve_calls[-1]
        if stays:
            assert request["affinity_target"] == {"worker_id": bound}
            assert second["worker_id"] == bound
        else:
            assert "affinity_target" not in request
            assert second["worker_id"] == other
        # Either way the turn refreshes the binding to the selected worker.
        assert tracker._session_workers[_SESSION][:2] == (
            second["worker_id"],
            clock.now,
        )
    finally:
        await _delete_workers(tracker, worker_ids)


@pytest.mark.asyncio
async def test_session_affinity_ttl_evicts_oldest_bindings(monkeypatch):
    """A lookup sweeps expired bindings off the oldest end and keeps fresh
    ones."""
    pytest.importorskip("dynamo.llm")
    clock = _FakeClock()
    monkeypatch.setattr(
        kv_token_tracker,
        "time",
        types.SimpleNamespace(monotonic=clock.monotonic, time=clock.monotonic),
    )
    tracker = _NoLongPollTracker()
    worker_ids = await _register_workers(tracker, ["sweep-r0", "sweep-r1"])
    try:
        for i, session_id in enumerate(["s-old", "s-mid", "s-new"]):
            await tracker.select_worker(
                f"req-{session_id}",
                _turn(i),
                worker_ids,
                32,
                session_id=session_id,
                session_affinity_ttl_s=10.0,
            )
            clock.now += 4.0
        # Now: s-old is 12s idle, s-mid 8s, s-new 4s.
        assert tracker._get_session_worker("s-new", worker_ids, 10.0, None) in (
            worker_ids
        )
        assert list(tracker._session_workers) == ["s-mid", "s-new"]
    finally:
        await _delete_workers(tracker, worker_ids)


@pytest.mark.asyncio
async def test_session_affinity_lru_bound(monkeypatch):
    """Bindings are bounded: past the limit the least recently bound session
    is evicted, and rebinding a session refreshes it."""
    pytest.importorskip("dynamo.llm")
    monkeypatch.setattr(kv_token_tracker, "SESSION_AFFINITY_MAX_ENTRIES", 2)
    tracker = _NoLongPollTracker()
    worker_ids = await _register_workers(tracker, ["lru-r0", "lru-r1"])

    async def turn(request_id, session_id):
        await tracker.select_worker(
            request_id,
            _turn(len(request_id)),
            worker_ids,
            32,
            session_id=session_id,
            session_affinity_ttl_s=_SESSION_TTL_S,
        )

    try:
        await turn("r1", "s1")
        await turn("r2", "s2")
        await turn("r3", "s1")  # refreshes s1, so s2 is now least recent
        await turn("r4", "s3")
        assert list(tracker._session_workers) == ["s1", "s3"]
    finally:
        await _delete_workers(tracker, worker_ids)


def test_older_peer_session_binding_does_not_override_newer():
    """A delayed broadcast for an older turn leaves a newer binding in place,
    while a newer peer binding replaces an older local one."""
    tracker = _NoLongPollTracker.__new__(_NoLongPollTracker)
    tracker._session_workers = OrderedDict()
    newer = tracker._bind_session(_SESSION, 1)
    assert tracker._bind_session(_SESSION, 2, bound_at=newer - 5) == newer
    assert tracker._session_workers[_SESSION][0] == 1
    assert tracker._bind_session(_SESSION, 3, bound_at=newer + 5) == newer + 5
    assert tracker._session_workers[_SESSION][0] == 3


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "ttl_s,expected_session_id",
    [(_SESSION_TTL_S, _SESSION), (0, None)],
    ids=["affinity", "disabled"],
)
async def test_peer_trackers_converge_on_session_binding(ttl_s, expected_session_id):
    """The reservation broadcast carries the session, so a peer ingress binds
    it to the same worker and routes the session's next turn there even when
    its normal scoring would pick another worker."""
    pytest.importorskip("dynamo.llm")
    routing = _NoLongPollTracker(ingress_replica_rank=0)
    peer = _NoLongPollTracker(ingress_replica_rank=1)
    trackers = (routing, peer)
    replica_ids = ["peer-sess-r0", "peer-sess-r1"]
    worker_ids = []
    try:
        for tracker in trackers:
            worker_ids = await _register_workers(tracker, replica_ids)

        broadcast_handle = _TrackerBroadcastHandle(*trackers)
        routing.start_reservation_broadcast(broadcast_handle)
        first = await routing.select_worker(
            "turn-1",
            _turn(1),
            worker_ids,
            32,
            session_id=_SESSION,
            session_affinity_ttl_s=ttl_s,
        )
        bound = first["worker_id"]
        other = _other(worker_ids, bound)
        await routing.flush_reservation_broadcast()
        await broadcast_handle.flush()
        for tracker in trackers:
            await tracker.flush_reservation_updates()

        payloads = [
            reservation
            for method_name, (batch,) in broadcast_handle.calls
            if method_name == "on_reservations_created"
            for reservation in batch
        ]
        assert [p["request_id"] for p in payloads] == ["turn-1"]
        assert payloads[0]["session_id"] == expected_session_id
        assert "turn-1" in peer._requests

        if expected_session_id is None:
            assert _SESSION not in peer._session_workers
            return

        assert peer._session_workers[_SESSION][0] == bound
        # Make the bound worker clearly busier on the peer: normal scoring
        # there picks the other worker, but the session stays put.
        await _load_worker(peer, bound, 3)
        control = await peer.select_worker("control", _turn(3), worker_ids, 32)
        assert control["worker_id"] == other
        second = await peer.select_worker(
            "turn-2",
            _turn(2),
            worker_ids,
            32,
            session_id=_SESSION,
            session_affinity_ttl_s=ttl_s,
        )
        assert second["worker_id"] == bound
    finally:
        if routing._reservation_forwarder is not None:
            routing._reservation_forwarder.close()
        for tracker in trackers:
            if tracker._reservation_apply_task is not None:
                tracker._reservation_apply_task.cancel()
            await _delete_workers(tracker, worker_ids)


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
