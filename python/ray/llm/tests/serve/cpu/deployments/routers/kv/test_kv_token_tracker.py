"""Validation for the router-local KV selector: build and registration,
``KVAwareRouter`` binding, selection service booking, Dynamo mesh replication
between LLMRouter replicas, and LLMServer lifecycle events.
"""

import asyncio
import gc
import socket
import subprocess
import sys
import time
from types import SimpleNamespace
from typing import List
from unittest.mock import patch

import pytest

import ray
import ray.cloudpickle
from ray import serve
from ray._common.network_utils import find_free_port
from ray._common.test_utils import async_wait_for_condition
from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.core.ingress.router import LLMRouter
from ray.llm._internal.serve.routing_policies.kv_aware import kv_token_tracker
from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    KV_LIFECYCLE_METADATA_KEY,
    OUTSTANDING_EFFECTIVE_PREFILL_TOKENS,
    SELECTION_MODEL_NAME,
    SELECTION_ROUTING_GROUP,
)
from ray.llm._internal.serve.routing_policies.kv_aware.kv_token_tracker import (
    KVTokenTracker,
    _SubscriptionReconciler,
    build_kv_token_tracker,
    get_kv_token_tracker,
    get_worker_id,
    set_kv_token_tracker,
)
from ray.llm._internal.serve.routing_policies.kv_aware.lifecycle import (
    LifecycleEvent,
    LifecyclePublisher,
)
from ray.llm._internal.serve.routing_policies.kv_aware.token_channel import TokenSender
from ray.serve._private.common import (
    DeploymentID,
    DeploymentTargetInfo,
    ReplicaID,
    RunningReplicaInfo,
)
from ray.serve._private.constants import SERVE_CONTROLLER_NAME, SERVE_NAMESPACE
from ray.serve._private.long_poll import (
    LongPollClient,
    LongPollNamespace,
    UpdatedObject,
)
from ray.serve.config import RequestRouterConfig
from ray.serve.llm.request_router import KVAwareRouter

import psutil


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
    ):
        self.indexer_threads = indexer_threads
        self.serve_deployment_id = serve_deployment_id
        self.session_affinity_ttl_s = session_affinity_ttl_s


class _StandaloneTracker(KVTokenTracker):
    """Supply the Serve context normally provided by the LLMRouter replica."""

    def __init__(self, *args, **kwargs):
        with patch.object(serve, "get_replica_context") as get_context:
            get_context.return_value.replica_id = _router_replica_id("standalone")
            super().__init__(*args, **kwargs)


class _NoLongPollTracker(_StandaloneTracker):
    """Real selection service without a controller membership subscription."""

    def _start_replica_tracking(self):
        pass


class _NoServiceTracker(_NoLongPollTracker):
    """Membership reconciliation alone, without a selection service."""

    def _create_selection_service(self):
        self._svc = None


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


def test_replica_mesh_retries_a_taken_port(monkeypatch):
    """If another process takes the probed port first, the router binds the
    Dynamo mesh on another free port."""
    pytest.importorskip("dynamo.llm")
    taken = socket.socket()
    taken.bind(("", 0))
    taken.listen()
    ports = iter([taken.getsockname()[1], find_free_port()])
    monkeypatch.setattr(kv_token_tracker, "find_free_port", lambda: next(ports))
    try:
        tracker = _NoLongPollTracker()
        assert tracker._replica_sync_port not in (0, taken.getsockname()[1])
        tracker._svc.shutdown()
    finally:
        taken.close()


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


def _start_publisher(worker_id: int) -> LifecyclePublisher:
    """An engine replica's lifecycle publisher for ``worker_id``'s requests."""
    publisher = LifecyclePublisher()
    publisher.endpoint = publisher.start(worker_id=worker_id, dp_rank=0)
    return publisher


async def _subscribe(trackers, publisher, worker_id) -> None:
    """Subscribe ``trackers`` to ``publisher`` and wait until each receives.
    Events sent before a subscription connects are dropped."""
    endpoint = publisher.endpoint
    for tracker in trackers:
        await tracker._svc.register_worker_publisher(endpoint)
    request_id = f"warmup-{endpoint}"
    for tracker in trackers:
        await tracker.select_worker(
            f"{request_id}-{id(tracker)}",
            list(range(10**6, 10**6 + 16)),
            [worker_id],
        )

    async def freed():
        for tracker in trackers:
            publisher.publish(f"{request_id}-{id(tracker)}", LifecycleEvent.FINISHED)
        await asyncio.sleep(0.05)
        return all(_active_requests(tracker, worker_id) == 0 for tracker in trackers)

    await async_wait_for_condition(freed, timeout=10)


@pytest.mark.asyncio
async def test_engine_events_reach_every_router():
    """The peer books the owner's request over the Dynamo mesh, in its LoRA
    namespace. Both apply the engine's events, and the peer frees the request
    after the owner leaves."""
    pytest.importorskip("dynamo.llm")
    worker_id = get_worker_id("engine-replica-0")
    owner, peer = _NoLongPollTracker(), _NoLongPollTracker()
    publisher = _start_publisher(worker_id)
    try:
        for tracker in (owner, peer):
            await _register_engine_worker(tracker, worker_id)
        await peer._svc.register_replica_peer(
            f"tcp://127.0.0.1:{owner._replica_sync_port}"
        )
        await _subscribe((owner, peer), publisher, worker_id)

        def loads():
            return [
                (
                    _active_requests(tracker, worker_id),
                    _worker_load(tracker, worker_id, "potential_prefill_tokens"),
                )
                for tracker in (owner, peer)
            ]

        async def decode_blocks(lora_name):
            request = {
                "model_name": SELECTION_MODEL_NAME,
                "routing_group": SELECTION_ROUTING_GROUP,
                "token_ids": list(range(64)),
                "lora_name": lora_name,
            }
            return (await peer._svc.potential_loads(request))[0][
                "potential_decode_blocks"
            ]

        await owner.select_worker(
            "req", list(range(64)), [worker_id], 32, lora_name="adapter-a"
        )
        await async_wait_for_condition(lambda: loads()[1] == loads()[0], timeout=10)
        assert loads()[0][0] == 1 and loads()[0][1] > 0
        assert await decode_blocks("adapter-a") < await decode_blocks("adapter-b")

        publisher.publish("req", LifecycleEvent.PREFILL_COMPLETE)
        await async_wait_for_condition(lambda: loads() == [(1, 0)] * 2, timeout=10)

        await owner.shutdown()
        publisher.publish("req", LifecycleEvent.FINISHED)
        await async_wait_for_condition(
            lambda: _active_requests(peer, worker_id) == 0, timeout=10
        )
    finally:
        publisher.close()
        for tracker in (owner, peer):
            await tracker.shutdown()


async def _register_engine_worker(tracker: KVTokenTracker, worker_id: int) -> None:
    """Register one engine replica's KV-event endpoint with ``tracker``."""
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


def _ports_released(endpoints):
    """All listeners can be rebound after shutdown, including after traffic."""
    for endpoint in endpoints:
        port = int(endpoint.rsplit(":", 1)[1])
        with socket.socket() as listener:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                listener.bind(("", port))
            except OSError:
                return False
    return True


@pytest.mark.asyncio
async def test_autoscaling_snapshot_tracks_request_phases(monkeypatch):
    """The snapshot follows the selection service: a selection adds its
    request's prefill tokens, and prefill completion or a free removes them."""
    pytest.importorskip("dynamo.llm")
    monkeypatch.setattr(
        kv_token_tracker, "RAY_SERVE_LLM_AUTOSCALING_METRIC_PUSH_INTERVAL_S", 0.01
    )
    worker_id = get_worker_id("engine-replica-0")
    tracker, peer = _NoLongPollTracker(), _NoLongPollTracker()
    publisher = _start_publisher(worker_id)
    published = []
    try:
        for router in (tracker, peer):
            await _register_engine_worker(router, worker_id)
        await tracker._svc.register_replica_peer(
            f"tcp://127.0.0.1:{peer._replica_sync_port}"
        )
        await _subscribe((tracker, peer), publisher, worker_id)
        tracker.set_autoscaling_metrics_callback(published.append)

        async def wait_snapshot(tokens):
            expected = {OUTSTANDING_EFFECTIVE_PREFILL_TOKENS: tokens}

            async def matches():
                return (
                    await tracker.get_autoscaling_snapshot() == expected
                    and bool(published)
                    and published[-1] == expected
                )

            await async_wait_for_condition(matches, timeout=10)

        await tracker.select_worker("a", list(range(64)), [worker_id])
        assert published[-1] == {OUTSTANDING_EFFECTIVE_PREFILL_TOKENS: 64}
        # A peer books 3 uncached tokens from a 32-token prompt.
        await peer._svc.create_reservation(
            {
                "model_name": SELECTION_MODEL_NAME,
                "routing_group": SELECTION_ROUTING_GROUP,
                "selection_id": "b",
                "worker_id": worker_id,
                "token_ids": list(range(100, 132)),
                "effective_prefill_tokens": 3,
            }
        )
        await wait_snapshot(67)

        publisher.publish("a", LifecycleEvent.PREFILL_COMPLETE)
        publisher.publish("a", LifecycleEvent.PREFILL_COMPLETE)
        await wait_snapshot(3)
        publisher.publish("a", LifecycleEvent.FINISHED)
        await async_wait_for_condition(
            lambda: _active_requests(tracker, worker_id) == 1, timeout=10
        )
        await wait_snapshot(3)
        publisher.publish("b", LifecycleEvent.FINISHED)
        await wait_snapshot(0)

        await peer.select_worker("removed", list(range(200, 264)), [worker_id])
        await wait_snapshot(64)
        tracker.remove_worker(worker_id)
        await wait_snapshot(0)
    finally:
        publisher.close()
        for router in (tracker, peer):
            await router.shutdown()


@pytest.mark.asyncio
async def test_raising_autoscaling_callback_does_not_fail_selection():
    pytest.importorskip("dynamo.llm")
    worker_id = get_worker_id("engine-replica-0")
    tracker = _NoLongPollTracker()
    try:
        await _register_engine_worker(tracker, worker_id)

        def raising_callback(snapshot):
            raise RuntimeError("metrics not ready")

        tracker.set_autoscaling_metrics_callback(raising_callback)
        selection = await tracker.select_worker("a", list(range(64)), [worker_id])
        assert selection["worker_id"] == worker_id
        assert _active_requests(tracker, worker_id) == 1
    finally:
        await tracker.shutdown()


def test_router_shutdown_closes_sockets():
    """Queued membership updates cannot reopen sockets during router shutdown."""
    pytest.importorskip("dynamo.llm")
    # Ray's driver opens gRPC connections lazily. Measure in a fresh process
    # so unrelated Ray connections cannot change the descriptor baseline.
    subprocess.run(
        [
            sys.executable,
            "-c",
            "import asyncio, runpy, sys; "
            "asyncio.run(runpy.run_path(sys.argv[1])['_check_router_shutdown']())",
            __file__,
        ],
        check=True,
        timeout=30,
    )


async def _check_router_shutdown():
    process = psutil.Process()
    baseline = None
    # The first lifetime initializes process-wide runtimes.
    for _ in range(6):
        # Keep the real LongPoll client; only the controller RPC is stubbed.
        with patch.object(ray, "get_actor"):
            tracker = _StandaloneTracker(
                serve_deployment_id=DeploymentID(name="LLMServer", app_name="app")
            )
        sender = TokenSender()
        tracker.subscribe_token_endpoints(sender.connect)
        tracker.set_autoscaling_metrics_callback(lambda snapshot: None)
        publisher = _start_publisher(get_worker_id("engine"))
        try:
            endpoints = [f"tcp://127.0.0.1:{tracker._replica_sync_port}"]
            token_endpoint = f"tcp://127.0.0.1:{find_free_port()}"
            await tracker._svc.register_replica_peer(endpoints[0])
            await tracker._svc.register_worker_publisher(publisher.endpoint)
            publisher.publish("finished", LifecycleEvent.FINISHED)

            workers = _router_targets({"engine": None})
            workers.running_replicas[0].routing_stats.update(
                {
                    "kv_event_metadata": {
                        "endpoint": endpoints[0],
                        "block_size": 16,
                        "max_num_batched_tokens": 8192,
                        "dp_rank": 0,
                    },
                    "kv_token_metadata": {"endpoint": token_endpoint},
                    KV_LIFECYCLE_METADATA_KEY: {"endpoint": publisher.endpoint},
                }
            )
            sender.connect([token_endpoint])
            # Queue both callbacks before stop(), without letting them run yet.
            client = tracker._long_poll_client
            client._process_update(
                {
                    key: UpdatedObject(
                        workers
                        if key[1] == tracker._serve_deployment_id
                        else _router_targets({"peer": endpoints[0]}),
                        1,
                        time.time(),
                    )
                    for key in client.key_listeners
                }
            )
        finally:
            publisher.close()
            await LLMRouter.__del__(
                SimpleNamespace(_kv_token_tracker=tracker, _token_sender=sender)
            )
        await asyncio.sleep(0)
        assert not tracker._pending_tasks
        assert tracker._autoscaling_metrics_task is None
        assert tracker._replica_peers._task is None
        assert tracker._lifecycle_sources._task is None
        assert sender._context is None
        assert not sender._sockets
        await async_wait_for_condition(lambda: _ports_released(endpoints), timeout=5)
        gc.collect()
        if baseline is None:
            baseline = process.num_fds()
        else:
            await async_wait_for_condition(
                lambda: process.num_fds() <= baseline, timeout=5
            )


class _RecordingEndpoints:
    """Stand-in for a selection service endpoint set. Like the real one, a
    change takes effect when called, not when awaited."""

    def __init__(self):
        self.calls = []
        self.endpoints = set()
        # Endpoints whose next register fails.
        self.fail_next = set()

    def _done(self, result):
        future = asyncio.get_running_loop().create_future()
        future.set_result(result)
        return future

    def register(self, endpoint):
        self.calls.append(("register", endpoint))
        if endpoint in self.fail_next:
            self.fail_next.discard(endpoint)
            future = asyncio.get_running_loop().create_future()
            future.set_exception(RuntimeError(f"cannot reach {endpoint}"))
            return future
        added = endpoint not in self.endpoints
        self.endpoints.add(endpoint)
        return self._done(added)

    def deregister(self, endpoint):
        self.calls.append(("deregister", endpoint))
        removed = endpoint in self.endpoints
        self.endpoints.discard(endpoint)
        return self._done(removed)

    def sync(self):
        return _SubscriptionReconciler(
            "endpoint", lambda: sorted(self.endpoints), self.register, self.deregister
        )


async def _converged(reconciler):
    if reconciler._task is not None:
        await reconciler._task


def _router_replica_id(unique_id):
    return ReplicaID(
        unique_id=unique_id,
        deployment_id=DeploymentID(name="LLMRouter", app_name="app"),
    )


def _router_targets(endpoints_by_id):
    return DeploymentTargetInfo(
        is_available=True,
        running_replicas=[
            RunningReplicaInfo(
                replica_id=_router_replica_id(uid),
                node_id="node",
                node_ip="10.0.0.1",
                availability_zone="az",
                actor_name=f"actor-{uid}",
                max_ongoing_requests=1000,
                routing_stats=(
                    {"kv_replica_sync_metadata": {"endpoint": endpoint}}
                    if endpoint
                    else {}
                ),
            )
            for uid, endpoint in endpoints_by_id.items()
        ],
    )


def _engine_targets(lifecycle_endpoints_by_id):
    targets = _router_targets(dict.fromkeys(lifecycle_endpoints_by_id))
    for replica, endpoint in zip(
        targets.running_replicas, lifecycle_endpoints_by_id.values()
    ):
        replica.routing_stats["kv_event_metadata"] = {
            "endpoint": "tcp://10.0.0.1:5557",
            "block_size": 16,
            "max_num_batched_tokens": 8192,
            "dp_rank": 0,
        }
        if endpoint:
            replica.routing_stats[KV_LIFECYCLE_METADATA_KEY] = {"endpoint": endpoint}
    return targets


@pytest.mark.asyncio
async def test_longpoll_drives_mesh_peers_and_lifecycle_sources():
    """Mesh peers follow the other LLMRouter replicas and lifecycle sources the
    LLMServer replicas, as replicas join, leave and restart."""
    tracker = _NoServiceTracker()
    peers, sources = _RecordingEndpoints(), _RecordingEndpoints()
    tracker._replica_peers = peers.sync()
    tracker._lifecycle_sources = sources.sync()
    tracker._router_replica_id = _router_replica_id("self")

    async def update(routers, engines):
        peers.calls.clear()
        sources.calls.clear()
        tracker._on_router_targets(_router_targets(routers))
        tracker._on_deployment_targets(_engine_targets(engines))
        await _converged(tracker._replica_peers)
        await _converged(tracker._lifecycle_sources)
        return sorted(peers.calls), sorted(sources.calls)

    assert await update(
        {"self": "tcp://r:0", "r1": "tcp://r:1", "r2": None},
        {"e1": "tcp://e:1", "e2": None},
    ) == ([("register", "tcp://r:1")], [("register", "tcp://e:1")])
    # r1 and e1 restart on new endpoints; r2 and e2 start advertising.
    assert await update(
        {"self": "tcp://r:0", "r1b": "tcp://r:2", "r2": "tcp://r:3"},
        {"e1b": "tcp://e:2", "e2": "tcp://e:3"},
    ) == (
        [
            ("deregister", "tcp://r:1"),
            ("register", "tcp://r:2"),
            ("register", "tcp://r:3"),
        ],
        [
            ("deregister", "tcp://e:1"),
            ("register", "tcp://e:2"),
            ("register", "tcp://e:3"),
        ],
    )


@pytest.mark.asyncio
async def test_reconciler_retries_and_wakes_on_update(monkeypatch):
    """A failed register is retried, and a new snapshot ends the retry backoff
    early instead of waiting it out."""
    monkeypatch.setattr(kv_token_tracker, "_ENDPOINT_SYNC_RETRY_INITIAL_S", 60)
    endpoints = _RecordingEndpoints()
    endpoints.fail_next = {"tcp://a:1"}
    reconciler = endpoints.sync()
    try:
        reconciler.update({"tcp://a:1"})
        await async_wait_for_condition(lambda: not endpoints.fail_next, timeout=5)
        reconciler.update({"tcp://a:1", "tcp://b:1"})
        await asyncio.wait_for(_converged(reconciler), timeout=5)
        assert endpoints.endpoints == {"tcp://a:1", "tcp://b:1"}
    finally:
        await reconciler.stop()


@pytest.mark.asyncio
async def test_router_restarted_on_its_endpoint_stays_a_peer():
    """A peer stays subscribed to a router's endpoint across a restart and
    replicates the restarted router's bookings."""
    pytest.importorskip("dynamo.llm")
    worker_id = get_worker_id("engine-replica-0")
    owner = _NoLongPollTracker()
    port = owner._replica_sync_port
    endpoint = f"tcp://127.0.0.1:{port}"
    peer = _NoLongPollTracker()
    trackers = [owner, peer]
    try:
        for tracker in trackers:
            await _register_engine_worker(tracker, worker_id)
        await peer._svc.register_replica_peer(endpoint)
        await asyncio.sleep(0.5)
        await owner.select_worker("req-old", list(range(64)), [worker_id], 32)
        await async_wait_for_condition(
            lambda: _active_requests(peer, worker_id) == 1, timeout=10
        )

        await owner.shutdown()
        with patch.object(kv_token_tracker, "find_free_port", return_value=port):
            restarted = trackers[0] = _NoLongPollTracker()
        await _register_engine_worker(restarted, worker_id)

        # The peer redials the restarted router in the background, so book until
        # a booking reaches it. "req-old" stays booked until it expires.
        for i in range(50):
            tokens = list(range(64 * (i + 1), 64 * (i + 2)))
            await restarted.select_worker(f"req-new-{i}", tokens, [worker_id], 32)
            await asyncio.sleep(0.2)
            if _active_requests(peer, worker_id) > 1:
                break
        assert _active_requests(peer, worker_id) > 1
    finally:
        for tracker in trackers:
            await tracker.shutdown()


# ---- Session affinity -------------------------------------------------------

_SESSION = "session-a"


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


async def _pinned_worker(tracker, session_id, worker_ids) -> int:
    """The worker ``tracker`` would pick for ``session_id``'s next turn, without
    booking load or binding the session."""
    selection = await tracker._svc.select(
        {
            "model_name": SELECTION_MODEL_NAME,
            "routing_group": SELECTION_ROUTING_GROUP,
            "token_ids": _turn(2),
            "allowed_worker_ids": worker_ids,
            "session_id": session_id,
        }
    )
    return selection["worker_id"]


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
async def test_replica_mesh_replicates_bursts_without_drops():
    """Every booking and session binding reaches the peer under selection
    bursts. The selection service drops bindings once its 4,096-slot mesh
    channel is full."""
    pytest.importorskip("dynamo.llm")
    owner = _NoLongPollTracker()
    peer = _NoLongPollTracker()
    ports = [owner._replica_sync_port, peer._replica_sync_port]
    trackers = (owner, peer)
    worker_ids = []
    bursts, per_burst = 8, 1000
    sessions = [f"session-{b}-{i}" for b in range(bursts) for i in range(per_burst)]
    try:
        for tracker in trackers:
            worker_ids = await _register_workers(tracker, ["burst-r0", "burst-r1"])
        bound = worker_ids[0]
        await owner._svc.register_replica_peer(f"tcp://127.0.0.1:{ports[1]}")
        await peer._svc.register_replica_peer(f"tcp://127.0.0.1:{ports[0]}")
        await asyncio.sleep(0.5)

        for b in range(bursts):
            await asyncio.gather(
                *(
                    owner.select_worker(
                        f"req-{session}", _turn(1), [bound], 32, session_id=session
                    )
                    for session in sessions[b * per_burst : (b + 1) * per_burst]
                )
            )
        await async_wait_for_condition(
            lambda: _active_requests(peer, bound) == len(sessions), timeout=10
        )

        # The replicated bookings make the peer's load favor the other worker,
        # so only a replicated binding keeps a session on ``bound``.
        assert await _pinned_worker(peer, "unbound", worker_ids) != bound
        unpinned = set(sessions)

        async def all_pinned():
            for session in list(unpinned):
                if await _pinned_worker(peer, session, worker_ids) != bound:
                    return False
                unpinned.discard(session)
            return True

        await async_wait_for_condition(all_pinned, timeout=30)
    finally:
        for tracker in trackers:
            await _delete_workers(tracker, worker_ids)


@pytest.mark.asyncio
async def test_session_binding_replicates_over_replica_mesh():
    """A session bound on one router pins its next turn on a peer router, even
    when the peer's load favors another worker."""
    pytest.importorskip("dynamo.llm")
    owner = _NoLongPollTracker()
    peer = _NoLongPollTracker()
    ports = [owner._replica_sync_port, peer._replica_sync_port]
    trackers = (owner, peer)
    replica_ids = ["sess-r0", "sess-r1"]
    worker_ids = []
    try:
        for tracker in trackers:
            worker_ids = await _register_workers(tracker, replica_ids)
        owner._replica_peers.update({f"tcp://127.0.0.1:{ports[1]}"})
        peer._replica_peers.update({f"tcp://127.0.0.1:{ports[0]}"})
        for tracker in trackers:
            await _converged(tracker._replica_peers)
        await asyncio.sleep(0.5)

        bound = (
            await owner.select_worker(
                "turn-1", _turn(1), worker_ids, 32, session_id=_SESSION
            )
        )["worker_id"]
        # The binding is published with the booking, so wait for the booking.
        await async_wait_for_condition(
            lambda: _active_requests(peer, bound) == 1, timeout=10
        )
        await asyncio.sleep(0.5)

        await _load_worker(peer, bound, 3)
        second = await peer.select_worker(
            "turn-2", _turn(2), worker_ids, 32, session_id=_SESSION
        )
        assert second["worker_id"] == bound
    finally:
        for tracker in trackers:
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
    on a slow router) must not leave untracked load booked, on this router or on
    the peer the booking was replicated to."""
    pytest.importorskip("dynamo.llm")
    worker_id = get_worker_id("engine-replica-0")
    owner, peer = _NoLongPollTracker(), _NoLongPollTracker()
    try:
        for tracker in (owner, peer):
            await _register_engine_worker(tracker, worker_id)
        await peer._svc.register_replica_peer(
            f"tcp://127.0.0.1:{owner._replica_sync_port}"
        )
        # The mesh SUB socket connects in the background.
        await asyncio.sleep(0.5)
        owner._svc = _SlowResumeSelectionService(owner._svc)

        select = asyncio.create_task(
            owner.select_worker("req-abandoned", list(range(64)), [worker_id], 32)
        )
        await owner._svc.booked.wait()
        # The booking reaches the peer before the caller gives up.
        await async_wait_for_condition(
            lambda: _active_requests(peer, worker_id) == 1, timeout=10
        )
        select.cancel()
        with pytest.raises(asyncio.CancelledError):
            await select
        owner._svc.release.set()
        # Freeing the abandoned booking releases it on the peer too.
        await async_wait_for_condition(
            lambda: [_active_requests(t, worker_id) for t in (owner, peer)] == [0, 0],
            timeout=10,
        )
    finally:
        for tracker in (owner, peer):
            await tracker.shutdown()


# ---- LongPoll replica-membership tracking -----------------------------------


@pytest.fixture(scope="module")
def serve_instance():
    if not ray.is_initialized():
        ray.init()
    yield
    serve.shutdown()


_REPLICATION_APP_NAME = "kv-replica-mesh-test"
_REPLICATION_WORKER_ID = get_worker_id("replicated-engine-worker")


class _RouterMeshTracker(KVTokenTracker):
    """Real selection service that follows only its own deployment's replicas,
    its Dynamo mesh peers, over LongPoll."""

    def _start_replica_tracking(self):
        controller = ray.get_actor(SERVE_CONTROLLER_NAME, namespace=SERVE_NAMESPACE)
        self._long_poll_client = LongPollClient(
            controller,
            {
                (
                    LongPollNamespace.DEPLOYMENT_TARGETS,
                    self._router_replica_id.deployment_id,
                ): self._on_router_targets
            },
            call_in_event_loop=asyncio.get_running_loop(),
            client_id=f"{type(self).__name__}:{self._router_replica_id.deployment_id}",
        )


@serve.deployment(
    num_replicas=2,
    request_router_config=RequestRouterConfig(request_routing_stats_period_s=0.5),
)
class ReplicaMeshDeployment:
    """Two real Serve replicas, each with its own selection service, that find
    each other through routing stats like LLMRouter replicas."""

    async def __init__(self):
        self._tracker = _RouterMeshTracker()
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

    async def record_routing_stats(self) -> dict:
        return self._tracker.replica_sync_routing_stats()

    async def __del__(self):
        await self._tracker.shutdown()

    async def network_resources(self):
        return (
            ray.get_runtime_context().current_actor,
            f"tcp://127.0.0.1:{self._tracker._replica_sync_port}",
        )

    async def num_peers(self) -> int:
        return len(self._tracker._svc.list_replica_peers())

    async def select_worker(self, request_id, token_ids):
        await self._tracker.select_worker(
            request_id, token_ids, [_REPLICATION_WORKER_ID]
        )

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
@pytest.mark.parametrize("shutdown", ["graceful", "kill"])
async def test_serve_replicas_converge_over_replica_mesh(serve_instance, shutdown):
    """Routers find each other through routing stats, and the selecting
    router's booking reaches its peer over the Dynamo mesh."""
    pytest.importorskip("dynamo.llm")
    module = sys.modules[__name__]
    ray.cloudpickle.register_pickle_by_value(module)
    resources = []
    try:
        handle = serve.run(
            ReplicaMeshDeployment.bind(),
            name=_REPLICATION_APP_NAME,
            route_prefix=None,
        )

        async def peers_connected():
            return await _broadcast(handle, "num_peers") == [1, 1]

        await async_wait_for_condition(
            peers_connected, timeout=60, retry_interval_ms=200
        )
        resources = await _broadcast(handle, "network_resources")
        # The mesh SUB sockets connect in the background.
        await asyncio.sleep(1)

        async def booked_on_both():
            loads = await _broadcast(handle, "get_worker_load")
            return len(loads) == 2 and all(
                load["active_requests"] == 1 and load["potential_prefill_tokens"] > 0
                for load in loads
            )

        # A handle call lands on one replica, which books and replicates.
        await handle.select_worker.remote("req-serve-mesh", list(range(64)))
        await async_wait_for_condition(booked_on_both, timeout=10)
    finally:
        if shutdown == "kill":
            for actor, *_ in resources:
                ray.kill(actor, no_restart=True)
        serve.delete(_REPLICATION_APP_NAME, _blocking=True)
        ray.cloudpickle.unregister_pickle_by_value(module)
    endpoints = [endpoint for _, *addresses in resources for endpoint in addresses]
    await async_wait_for_condition(lambda: _ports_released(endpoints), timeout=5)


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


class _MembershipTracker(_StandaloneTracker):
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
