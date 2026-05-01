import asyncio
import sys
from collections import Counter
from typing import Optional

import pytest

from ray._common.utils import get_or_create_event_loop
from ray.exceptions import ActorDiedError, ActorUnavailableError
from ray.serve._private.common import (
    DeploymentHandleSource,
    ReplicaID,
    RequestMetadata,
)
from ray.serve._private.request_router import PendingRequest
from ray.serve._private.test_utils import (
    FakeCounter,
    FakeGauge,
    FakeHistogram,
    MockTimer,
)
from ray.serve._private.utils import generate_request_id
from ray.serve.experimental.consistent_hash_router import (
    DEFAULT_FALLBACK_REPLICAS,
    DEFAULT_VIRTUAL_NODES,
    SESSION_AFFINITY_LRU_MAX,
    TOP_SESSIONS_MAX,
    ConsistentHashRouter,
    _hash_bytes,
)
from ray.serve.tests.unit.conftest import (
    FAKE_REPLICA_DEFAULT_MAX_ONGOING_REQUESTS as DEFAULT_MAX_ONGOING_REQUESTS,
    FAKE_REPLICA_DEPLOYMENT_ID as DEPLOYMENT_ID,
    FakeRunningReplica,
)

TIMER = MockTimer()


def _make_request(
    session_id: str = "", request_id: Optional[str] = None
) -> PendingRequest:
    internal_id = request_id if request_id is not None else generate_request_id()
    return PendingRequest(
        args=[],
        kwargs={},
        metadata=RequestMetadata(
            request_id=generate_request_id(),
            internal_request_id=internal_id,
            session_id=session_id,
        ),
    )


@pytest.fixture
def router(request) -> ConsistentHashRouter:
    """Construct a ConsistentHashRouter on its own loop so its
    asyncio.Event attaches to the correct loop, then return it to
    the active test loop."""
    params = getattr(request, "param", {})

    async def build(loop: asyncio.AbstractEventLoop) -> ConsistentHashRouter:
        r = ConsistentHashRouter(
            deployment_id=DEPLOYMENT_ID,
            handle_source=DeploymentHandleSource.REPLICA,
            self_actor_id="fake-actor-id",
            self_actor_handle=None,
            use_replica_queue_len_cache=False,
            get_curr_time_s=TIMER.time,
        )
        r.initialize_state(**params)
        r.initial_backoff_s = 0.001
        r.backoff_multiplier = 1
        r.max_backoff_s = 0.001
        return r

    s = asyncio.new_event_loop().run_until_complete(build(get_or_create_event_loop()))
    TIMER.reset()
    yield s
    # Confirm routing loop exits cleanly when there's no residual work.
    assert s.curr_num_routing_tasks == 0
    assert s.num_pending_requests == 0


def test_hash_bytes():
    assert _hash_bytes(b"hello") == _hash_bytes(b"hello")
    assert _hash_bytes(b"sess_0000") != _hash_bytes(b"sess_0001")

    h = _hash_bytes(b"arbitrary")
    assert isinstance(h, int)
    assert 0 <= h < (1 << 64)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"virtual_nodes": 0},
        {"virtual_nodes": -5},
        {"virtual_nodes": "many"},
        {"fallback_replicas": -1},
    ],
)
def test_initialize_state_invalid_args(kwargs):
    r = ConsistentHashRouter(
        deployment_id=DEPLOYMENT_ID,
        handle_source=DeploymentHandleSource.REPLICA,
        self_actor_id="a",
        self_actor_handle=None,
    )
    with pytest.raises(ValueError):
        r.initialize_state(**kwargs)


def test_initialize_state_valid_args():
    r = ConsistentHashRouter(
        deployment_id=DEPLOYMENT_ID,
        handle_source=DeploymentHandleSource.REPLICA,
        self_actor_id="a",
        self_actor_handle=None,
    )
    r.initialize_state(virtual_nodes=42, fallback_replicas=7)
    assert r._virtual_nodes == 42
    assert r._fallback_replicas == 7


def test_ring_defaults(router):
    assert router._virtual_nodes == DEFAULT_VIRTUAL_NODES
    assert router._fallback_replicas == DEFAULT_FALLBACK_REPLICAS


@pytest.mark.asyncio
async def test_choose_replicas_no_replicas_returns_empty(router):
    result = await router.choose_replicas(
        candidate_replicas=[], pending_request=_make_request("sess_x")
    )
    assert result == []


@pytest.mark.asyncio
async def test_choose_replicas_stickiness(router):
    """Repeated calls with the same session_id must map to the same primary replica."""
    replicas = [FakeRunningReplica(f"r{i}") for i in range(4)]
    router.update_replicas(replicas)

    sess = "user_123"
    first = await router.choose_replicas(
        candidate_replicas=replicas, pending_request=_make_request(sess)
    )
    for _ in range(10):
        again = await router.choose_replicas(
            candidate_replicas=replicas, pending_request=_make_request(sess)
        )
        assert again[0][0].replica_id == first[0][0].replica_id


@pytest.mark.asyncio
@pytest.mark.parametrize("router", [{"fallback_replicas": 0}], indirect=True)
async def test_choose_replicas_no_fallbacks(router):
    """K=0 -> a single rank containing only the primary."""
    replicas = [FakeRunningReplica(f"r{i}") for i in range(5)]
    router.update_replicas(replicas)

    ranks = await router.choose_replicas(
        candidate_replicas=replicas, pending_request=_make_request("sess_x")
    )
    assert len(ranks) == 1
    assert len(ranks[0]) == 1


@pytest.mark.asyncio
async def test_choose_replicas_fallback_caps_at_replica_count(router):
    replicas = [FakeRunningReplica("ra"), FakeRunningReplica("rb")]
    router.update_replicas(replicas)

    ranks = await router.choose_replicas(
        candidate_replicas=replicas, pending_request=_make_request("sess_x")
    )
    assert len(ranks) == 2
    flat = [rep.replica_id for rank in ranks for rep in rank]
    assert len(flat) == len(set(flat)) == 2


@pytest.mark.asyncio
async def test_choose_replicas_missing_session_spreads_across_replicas(router):
    replicas = [FakeRunningReplica(f"r{i}") for i in range(3)]
    router.update_replicas(replicas)

    primaries = set()
    for _ in range(20):
        ranks = await router.choose_replicas(
            candidate_replicas=replicas,
            pending_request=_make_request(session_id=""),
        )
        primaries.add(ranks[0][0].replica_id)

    assert len(primaries) > 1


@pytest.mark.asyncio
async def test_lookup_wraps_around_past_max_vnode(router, monkeypatch):
    replicas = [FakeRunningReplica(f"r{i}") for i in range(3)]
    router.update_replicas(replicas)

    max_vnode = max(router._ring_hashes)
    monkeypatch.setattr(
        "ray.serve.experimental.consistent_hash_router._hash_bytes",
        lambda key: max_vnode + 1,
    )

    ranked = router._lookup_ranked_replicas("anything")
    # Wrapping around sends us to index 0 so the primary is whichever replica
    # owns the smallest vnode.
    assert ranked[0] == router._ring_replicas[0]


@pytest.mark.asyncio
@pytest.mark.parametrize("key_hash", [0, (1 << 64) - 1])
async def test_lookup_at_hash_space_boundaries(router, monkeypatch, key_hash):
    """Key hashing to exactly 0 or 2**64 - 1 must still produce a valid owner."""
    replicas = [FakeRunningReplica(f"r{i}") for i in range(3)]
    router.update_replicas(replicas)

    monkeypatch.setattr(
        "ray.serve.experimental.consistent_hash_router._hash_bytes",
        lambda key: key_hash,
    )
    ranked = router._lookup_ranked_replicas("anything")

    assert len(ranked) == 1 + DEFAULT_FALLBACK_REPLICAS
    assert ranked[0] in {r.replica_id for r in replicas}


@pytest.mark.asyncio
async def test_balanced_distribution(router):
    """100 vnodes per replica should distribute random sessions close to uniform."""
    replicas = [FakeRunningReplica(f"r{i}") for i in range(5)]
    router.update_replicas(replicas)

    counts: Counter = Counter()
    for i in range(2000):
        ranks = await router.choose_replicas(
            candidate_replicas=replicas,
            pending_request=_make_request(f"sess_{i}"),
        )
        counts[ranks[0][0].replica_id.unique_id] += 1

    fair = 2000 / 5
    for c in counts.values():
        assert 0.5 * fair < c < 2 * fair


@pytest.mark.asyncio
async def test_scale_up_remaps_small_fraction(router):
    """Adding the Nth replica should remap ~1/N of sessions."""
    initial = [FakeRunningReplica(f"r{i}") for i in range(4)]
    router.update_replicas(initial)

    sessions = [f"sess_{i}" for i in range(1000)]
    mapping_before = {}
    for s in sessions:
        ranks = await router.choose_replicas(
            candidate_replicas=initial, pending_request=_make_request(s)
        )
        mapping_before[s] = ranks[0][0].replica_id

    # Scale from 4 to 5.
    scaled = initial + [FakeRunningReplica("r_new")]
    router.update_replicas(scaled)

    moved = 0
    for s in sessions:
        ranks = await router.choose_replicas(
            candidate_replicas=scaled, pending_request=_make_request(s)
        )
        if ranks[0][0].replica_id != mapping_before[s]:
            moved += 1

    # Expected: 1/5 = 20%. Upper bound generous to avoid flake on
    # random-cluster draws of the hash.
    assert moved < 0.4 * len(sessions), (
        f"Scale-up remapped {moved}/{len(sessions)} sessions; expected ~200"
    )

    assert moved > 0


@pytest.mark.asyncio
async def test_scale_down_remaps_only_owned_sessions(router):
    """Removing one replica out of R should remap roughly 1/R sessions.
    Sessions that were not on the removed replica must stay put."""
    initial = [FakeRunningReplica(f"r{i}") for i in range(4)]
    router.update_replicas(initial)

    sessions = [f"sess_{i}" for i in range(1000)]
    mapping_before = {}
    for s in sessions:
        ranks = await router.choose_replicas(
            candidate_replicas=initial, pending_request=_make_request(s)
        )
        mapping_before[s] = ranks[0][0].replica_id

    # Drop the first replica from the set.
    dropped_id = initial[0].replica_id
    remaining = initial[1:]
    router.update_replicas(remaining)

    moved_owned = 0
    moved_other = 0
    for s in sessions:
        ranks = await router.choose_replicas(
            candidate_replicas=remaining, pending_request=_make_request(s)
        )
        new = ranks[0][0].replica_id
        prev = mapping_before[s]
        if prev == dropped_id:
            moved_owned += 1
        elif new != prev:
            moved_other += 1

    # Every session that was on the dropped replica must move.
    expected_owned = sum(1 for v in mapping_before.values() if v == dropped_id)
    assert moved_owned == expected_owned
    # Sessions not owned by the dropped replica stay put.
    assert moved_other == 0


@pytest.mark.asyncio
async def test_fallback_when_primary_at_max_ongoing(router):
    """Primary saturated -> the outer retry loop must walk to fallback_1."""
    loop = get_or_create_event_loop()
    replicas = [FakeRunningReplica(f"r{i}") for i in range(4)]
    router.update_replicas(replicas)

    ranks = await router.choose_replicas(
        candidate_replicas=replicas, pending_request=_make_request("user_42")
    )
    primary_id = ranks[0][0].replica_id
    fallback_id = ranks[1][0].replica_id

    # Saturate the primary, leave fallbacks free.
    for r in replicas:
        if r.replica_id == primary_id:
            r.set_queue_len_response(DEFAULT_MAX_ONGOING_REQUESTS)
        else:
            r.set_queue_len_response(0)

    chosen = await loop.create_task(
        router._choose_replica_for_request(_make_request("user_42"))
    )
    assert chosen.replica_id == fallback_id


@pytest.mark.asyncio
async def test_backoff_when_all_exhausted_then_retries_on_drain(router):
    """When every ranked replica is at max_ongoing_requests, the routing
    task backs off and loops. When capacity opens up, it picks the newly
    freed replica on the next iteration -- without us having to call
    update_replicas."""
    loop = get_or_create_event_loop()
    # K+1 = 3 replicas so every one is in the ranked set.
    replicas = [FakeRunningReplica(f"r{i}") for i in range(3)]
    for r in replicas:
        r.set_queue_len_response(DEFAULT_MAX_ONGOING_REQUESTS)
    router.update_replicas(replicas)

    task = loop.create_task(
        router._choose_replica_for_request(_make_request("user_42"))
    )
    # Nothing should be ready: the task is stuck in the backoff loop
    # because every replica reports itself as full.
    done, _ = await asyncio.wait([task], timeout=0.02)
    assert len(done) == 0

    # Drain the primary. The retry loop's next probe sees the open slot
    # and the task completes.
    ranks = await router.choose_replicas(
        candidate_replicas=replicas, pending_request=_make_request("user_42")
    )
    primary_id = ranks[0][0].replica_id
    for r in replicas:
        if r.replica_id == primary_id:
            r.set_queue_len_response(0)

    chosen = await task
    assert chosen.replica_id == primary_id


@pytest.mark.asyncio
async def test_dead_primary_routes_to_fallback(router):
    """``ActorDiedError`` drops the primary from ``_replicas``; the
    ring's stale reference is filtered out and the request lands on
    the fallback instead of hanging."""
    replicas = [FakeRunningReplica(f"r{i}") for i in range(3)]
    router.update_replicas(replicas)

    # Find the primary + fallback for "user_42" through the ring.
    ranks = await router.choose_replicas(
        candidate_replicas=replicas, pending_request=_make_request("user_42")
    )
    primary_id = ranks[0][0].replica_id
    fallback_id = ranks[1][0].replica_id

    # Primary's probe raises ActorDiedError; fallbacks respond normally.
    for r in replicas:
        if r.replica_id == primary_id:
            r.set_queue_len_response(queue_len=0, exception=ActorDiedError())
        else:
            r.set_queue_len_response(0)

    chosen = await router._choose_replica_for_request(_make_request("user_42"))
    assert chosen.replica_id == fallback_id

    # The dead replica should be gone from _replicas, so subsequent
    # requests on this session never probe it again.
    assert primary_id not in router._replicas


@pytest.mark.asyncio
async def test_unavailable_primary_falls_over_then_recovers(router):
    """``ActorUnavailableError`` is transient: request falls over to
    the fallback, the replica stays in ``_replicas``, and sticky
    traffic returns to the primary once it recovers."""
    replicas = [FakeRunningReplica(f"r{i}") for i in range(3)]
    router.update_replicas(replicas)

    ranks = await router.choose_replicas(
        candidate_replicas=replicas, pending_request=_make_request("user_42")
    )
    primary_id = ranks[0][0].replica_id
    fallback_id = ranks[1][0].replica_id

    primary = next(r for r in replicas if r.replica_id == primary_id)

    # Primary raises ActorUnavailableError; fallbacks accept.
    primary.set_queue_len_response(
        queue_len=0,
        exception=ActorUnavailableError(
            error_message="temporarily unavailable",
            actor_id=b"a" * 16,
        ),
    )
    for r in replicas:
        if r.replica_id != primary_id:
            r.set_queue_len_response(0)

    chosen = await router._choose_replica_for_request(_make_request("user_42"))
    assert chosen.replica_id == fallback_id
    # Unlike the died case, the replica must still be in _replicas --
    # ActorUnavailableError is transient.
    assert primary_id in router._replicas

    # Recover the primary. Next sticky request must return to it.
    primary.set_queue_len_response(queue_len=0, exception=None)
    chosen2 = await router._choose_replica_for_request(_make_request("user_42"))
    assert chosen2.replica_id == primary_id


@pytest.mark.asyncio
async def test_backoff_engaged_when_all_ranks_saturated(router):
    router.initial_backoff_s = 999
    router.backoff_multiplier = 1
    router.max_backoff_s = 999

    loop = get_or_create_event_loop()

    # 1 + DEFAULT_FALLBACK_REPLICAS replicas -> the full ranked set is
    # saturated, so _choose_replicas_with_backoff exhausts every yielded rank
    # and falls through to the backoff branch.
    replicas = [
        FakeRunningReplica(f"r{i}") for i in range(1 + DEFAULT_FALLBACK_REPLICAS)
    ]
    for r in replicas:
        r.set_queue_len_response(DEFAULT_MAX_ONGOING_REQUESTS)
    router.update_replicas(replicas)

    task = loop.create_task(
        router._choose_replica_for_request(_make_request("user_42"))
    )
    done, _ = await asyncio.wait([task], timeout=0.1)
    assert len(done) == 0

    # The routing task must have entered the backoff branch.
    assert router.num_routing_tasks_in_backoff >= 1

    # Free up the primary. With a 999s backoff sleep already in flight, the
    # task must remain pending.
    ranks = await router.choose_replicas(
        candidate_replicas=replicas, pending_request=_make_request("user_42")
    )
    primary_id = ranks[0][0].replica_id
    for r in replicas:
        if r.replica_id == primary_id:
            r.set_queue_len_response(0)

    done, _ = await asyncio.wait([task], timeout=0.1)
    assert len(done) == 0, (
        "routing task completed within the sleep window -- "
        "exponential backoff is not being applied"
    )

    task.cancel()
    try:
        await task
    except BaseException:
        pass
    router._routing_tasks.clear()
    router._pending_requests_to_fulfill.clear()
    router._pending_requests_to_route.clear()


@pytest.mark.asyncio
async def test_concurrent_same_session_does_not_orphan(router):
    """Concurrent same-session requests at the rank-list size must all
    resolve, not livelock.

    Regression: the routing task that pops a pending from the route deque
    is the one responsible for fulfilling it. If the inner retry loop
    breaks early under load (e.g., on the routing-task-shedding check after
    a probe miss) without fulfilling, that pending used to become an
    orphan: still in `_pending_requests_to_fulfill` / `_pending_requests_by_id`
    but with no task actively routing it. Surviving routing tasks would
    then loop with `request_metadata=None`, and because ConsistentHashRouter
    uses strict-metadata-match fulfillment (no FIFO fallback like pow2),
    they couldn't pick up the orphan, livelocking the router.
    """
    loop = get_or_create_event_loop()

    # 1 + DEFAULT_FALLBACK_REPLICAS replicas so the full rank list is
    # exactly the same set across requests (no replica outside the ranks).
    replicas = [
        FakeRunningReplica(f"r{i}") for i in range(1 + DEFAULT_FALLBACK_REPLICAS)
    ]
    for r in replicas:
        r.set_queue_len_response(0)
    router.update_replicas(replicas)

    # Fire (1 + fallback_replicas + 2) concurrent same-session requests.
    # That's strictly more than the rank-list size, exercising the path
    # where the routing task that owns a popped pending might temporarily
    # exceed the dynamically computed task target.
    burst_size = 1 + DEFAULT_FALLBACK_REPLICAS + 2
    tasks = [
        loop.create_task(
            router._choose_replica_for_request(_make_request("burst-session"))
        )
        for _ in range(burst_size)
    ]
    done, pending = await asyncio.wait(tasks, timeout=2.0)
    assert len(pending) == 0, (
        f"{len(pending)}/{burst_size} concurrent same-session requests "
        "did not resolve -- ConsistentHashRouter livelocked under load."
    )

    chosen_replicas = {t.result().replica_id for t in done}
    # Affinity preserved: every burst request lands on the same primary.
    assert len(chosen_replicas) == 1


@pytest.mark.asyncio
async def test_two_routers_route_same_session_to_same_replica(router):
    """Two independent routers with the same replica set must pick the same
    replica for the same session_id."""
    replicas_a = [FakeRunningReplica(f"r{i}") for i in range(5)]
    replicas_b = [FakeRunningReplica(f"r{i}") for i in range(5)]
    router.update_replicas(replicas_a)

    other = ConsistentHashRouter(
        deployment_id=DEPLOYMENT_ID,
        handle_source=DeploymentHandleSource.REPLICA,
        self_actor_id="other",
        self_actor_handle=None,
    )
    other.initialize_state()
    other.update_replicas(replicas_b)

    for i in range(20):
        sess = f"user_{i}"
        ranks_a = await router.choose_replicas(
            candidate_replicas=replicas_a,
            pending_request=_make_request(sess),
        )
        ranks_b = await other.choose_replicas(
            candidate_replicas=replicas_b,
            pending_request=_make_request(sess),
        )
        assert ranks_a[0][0].replica_id == ranks_b[0][0].replica_id, (
            f"Session {sess} diverged: {ranks_a[0][0].replica_id} vs "
            f"{ranks_b[0][0].replica_id}"
        )


def _install_fake_metrics(router: ConsistentHashRouter) -> dict:
    """Swap router metrics for ``Fake*`` equivalents from ``_private/test_utils``."""
    common = ("application", "deployment", "actor_id", "handle_source")
    default_tags = {
        "application": router._deployment_id.app_name,
        "deployment": router._deployment_id.name,
        "actor_id": router._self_actor_id,
        "handle_source": router._handle_source.value,
    }
    router.fallback_counter = FakeCounter(tag_keys=common + ("rank",)).set_default_tags(
        default_tags
    )
    router.top_sessions_gauge = FakeGauge(
        tag_keys=common + ("session_id",)
    ).set_default_tags(default_tags)
    router.ring_rebuilds_counter = FakeCounter(tag_keys=common).set_default_tags(
        default_tags
    )
    router.ring_rebuild_duration_ms_histogram = FakeHistogram(
        tag_keys=common
    ).set_default_tags(default_tags)
    router.session_observations_counter = FakeCounter(tag_keys=common).set_default_tags(
        default_tags
    )
    router.session_affinity_hits_counter = FakeCounter(
        tag_keys=common
    ).set_default_tags(default_tags)
    return {
        "fallback": router.fallback_counter,
        "top_sessions": router.top_sessions_gauge,
        "ring_rebuilds": router.ring_rebuilds_counter,
        "ring_rebuild_duration_ms": router.ring_rebuild_duration_ms_histogram,
        "session_observations": router.session_observations_counter,
        "session_affinity_hits": router.session_affinity_hits_counter,
        "default_tags": default_tags,
    }


async def _route_session(router, session_id: str):
    """Route one request end-to-end and return the accepting ``ReplicaID``."""
    loop = get_or_create_event_loop()
    pr = _make_request(session_id)
    chosen = await loop.create_task(router._choose_replica_for_request(pr))
    router.on_request_routed(pr, chosen.replica_id, result=None)
    return chosen.replica_id


def _ready_replicas(n: int) -> list:
    reps = [FakeRunningReplica(f"r{i}") for i in range(n)]
    for r in reps:
        r.set_queue_len_response(0)
    return reps


@pytest.mark.asyncio
async def test_ring_rebuild_metrics_fire_once_per_rebuild(router):
    """Replica-set change fires once; no-op ticks don't."""
    fakes = _install_fake_metrics(router)
    tags = fakes["default_tags"]

    router.update_replicas(_ready_replicas(3))
    assert fakes["ring_rebuilds"].get_count(tags) == 1
    obs = fakes["ring_rebuild_duration_ms"].get_observations(tags)
    assert len(obs) == 1 and obs[0] >= 0

    router.update_replicas(_ready_replicas(3))
    assert fakes["ring_rebuilds"].get_count(tags) == 1
    assert len(fakes["ring_rebuild_duration_ms"].get_observations(tags)) == 1


@pytest.mark.asyncio
async def test_fallback_counter_tags_primary_rank(router):
    """Fresh session on a steady ring lands at rank 0."""
    router.update_replicas(_ready_replicas(5))
    fakes = _install_fake_metrics(router)

    await _route_session(router, "sess_xyz")

    tags = {**fakes["default_tags"], "rank": "0"}
    assert fakes["fallback"].get_count(tags) == 1


@pytest.mark.asyncio
async def test_fallback_counter_tags_fallback_rank_when_primary_saturated(router):
    """Saturated primary tags the rank=1 fallback bucket."""
    replicas = _ready_replicas(3)
    router.update_replicas(replicas)
    fakes = _install_fake_metrics(router)

    ranked = router._lookup_ranked_replicas("sess_fb")
    primary_id, fallback_id = ranked[0], ranked[1]

    for r in replicas:
        if r.replica_id == primary_id:
            r.set_queue_len_response(DEFAULT_MAX_ONGOING_REQUESTS)
        else:
            r.set_queue_len_response(0)

    chosen = await _route_session(router, "sess_fb")
    assert chosen == fallback_id

    tags = {**fakes["default_tags"], "rank": "1"}
    assert fakes["fallback"].get_count(tags) == 1


@pytest.mark.asyncio
async def test_fallback_counter_tags_stale_rank_on_ring_drift(router):
    """rank=-1 fires when the accepting replica isn't on the current ring."""
    router.update_replicas(_ready_replicas(3))
    fakes = _install_fake_metrics(router)

    stale = ReplicaID(unique_id="not_on_ring", deployment_id=DEPLOYMENT_ID)
    router.on_request_routed(_make_request("sess_x"), stale, result=None)

    tags = {**fakes["default_tags"], "rank": "-1"}
    assert fakes["fallback"].get_count(tags) == 1


@pytest.mark.asyncio
async def test_top_sessions_gauge_tracks_count_per_session(router):
    """Three requests for one session yield a gauge of 3."""
    router.update_replicas(_ready_replicas(3))
    fakes = _install_fake_metrics(router)

    for _ in range(3):
        await _route_session(router, "sess_hot")

    tags = {**fakes["default_tags"], "session_id": "sess_hot"}
    assert fakes["top_sessions"].get_value(tags) == 3


@pytest.mark.asyncio
async def test_top_sessions_lru_evicts_and_zeros_gauge(router):
    """LRU overflow evicts the LRU entry and zeros its gauge series."""
    router.update_replicas(_ready_replicas(1))
    fakes = _install_fake_metrics(router)

    for i in range(TOP_SESSIONS_MAX + 1):
        await _route_session(router, f"sess_{i}")

    evicted_tags = {**fakes["default_tags"], "session_id": "sess_0"}
    assert fakes["top_sessions"].get_value(evicted_tags) == 0
    assert len(router._top_sessions_lru) == TOP_SESSIONS_MAX
    assert "sess_0" not in router._top_sessions_lru


@pytest.mark.asyncio
async def test_top_sessions_lru_touches_recency_on_repeat_hit(router):
    """Repeat hit moves session to MRU; overflow evicts an older entry."""
    router.update_replicas(_ready_replicas(1))
    _install_fake_metrics(router)

    await _route_session(router, "sess_A")
    await _route_session(router, "sess_B")
    await _route_session(router, "sess_A")  # A now MRU, B now LRU

    for i in range(TOP_SESSIONS_MAX - 2):
        await _route_session(router, f"filler_{i}")

    await _route_session(router, "sess_C")  # overflow -> evicts sess_B

    assert "sess_A" in router._top_sessions_lru
    assert "sess_B" not in router._top_sessions_lru


@pytest.mark.asyncio
async def test_anonymous_traffic_skips_top_sessions(router):
    """Session-less request increments rank but skips top-sessions."""
    router.update_replicas(_ready_replicas(1))
    fakes = _install_fake_metrics(router)

    await _route_session(router, session_id="")

    assert fakes["fallback"].get_count({**fakes["default_tags"], "rank": "0"}) == 1
    assert fakes["top_sessions"].values == {}
    assert router._top_sessions_lru == {}


# ---------------------------------------------------------------------------
# Session-affinity metric (the dip-and-recover SLI for ring stability).
# ---------------------------------------------------------------------------


def _count(counter, tags) -> int:
    """``FakeCounter.get_count`` returns None for unseen tag combinations;
    normalize to 0 so tests can do arithmetic with the result."""
    return counter.get_count(tags) or 0


@pytest.mark.asyncio
async def test_session_affinity_first_request_records_no_observation(router):
    """First sighting of a session_id seeds the LRU but emits no observation
    (there is no prior mapping to compare against)."""
    router.update_replicas(_ready_replicas(3))
    fakes = _install_fake_metrics(router)
    tags = fakes["default_tags"]

    chosen = await _route_session(router, "sess_first")

    assert _count(fakes["session_observations"], tags) == 0
    assert _count(fakes["session_affinity_hits"], tags) == 0
    # LRU seeded with the chosen replica.
    assert router._last_replica_per_session["sess_first"] == chosen


@pytest.mark.asyncio
async def test_session_affinity_hit_when_ring_unchanged(router):
    """Repeated requests on a stable ring all hit the same replica → 100%
    affinity rate after the first request (which seeds the LRU)."""
    router.update_replicas(_ready_replicas(3))
    fakes = _install_fake_metrics(router)
    tags = fakes["default_tags"]

    chosen_ids = [await _route_session(router, "sess_sticky") for _ in range(5)]
    # All 5 requests land on the same replica (consistent hashing is
    # deterministic on a stable ring).
    assert len(set(chosen_ids)) == 1

    # 4 subsequent observations after the first seeded the LRU; all 4 hit.
    assert _count(fakes["session_observations"], tags) == 4
    assert _count(fakes["session_affinity_hits"], tags) == 4


@pytest.mark.asyncio
async def test_session_affinity_miss_on_ring_remap(router):
    """When a ring rebuild relocates a session's primary to a new replica,
    the next observation counts as a miss — the dip in the affinity rate.
    The replica set at scale-up is chosen so that ``sess_remap`` provably
    moves to the newly-added replica."""
    initial = _ready_replicas(4)
    router.update_replicas(initial)

    sess = "sess_remap_probe"
    routing_key = router._routing_key(_make_request(sess))
    pre_primary = router._lookup_ranked_replicas(routing_key)[0]

    # Construct a replica set whose ring positions guarantee the new
    # replica intercepts ``sess_remap_probe`` ahead of ``pre_primary``.
    # Try replicas r_new_0..r_new_99 until one moves the session.
    new_primary = None
    for i in range(200):
        candidate = FakeRunningReplica(f"r_new_{i}")
        candidate.set_queue_len_response(0)
        scaled = list(initial) + [candidate]
        # Probe what the rebuild *would* yield without permanently
        # reconfiguring the router.
        probe_router = ConsistentHashRouter(
            deployment_id=DEPLOYMENT_ID,
            handle_source=DeploymentHandleSource.REPLICA,
            self_actor_id="probe",
            self_actor_handle=None,
            use_replica_queue_len_cache=False,
        )
        probe_router.initialize_state()
        probe_router.update_replicas(scaled)
        new_primary_candidate = probe_router._lookup_ranked_replicas(routing_key)[0]
        if new_primary_candidate != pre_primary:
            new_primary = new_primary_candidate
            scale_up_with = candidate
            break

    assert new_primary is not None, "No new replica relocates the test session"

    # Pre-rebuild routing seeds the LRU with the old primary.
    fakes = _install_fake_metrics(router)
    tags = fakes["default_tags"]
    first = await _route_session(router, sess)
    assert first == pre_primary
    assert _count(fakes["session_observations"], tags) == 0  # first sighting

    # Scale up; new replica's vnodes intercept the routing key.
    router.update_replicas(initial + [scale_up_with])

    # Post-rebuild routing lands on the new primary -> miss.
    second = await _route_session(router, sess)
    assert second == new_primary
    assert _count(fakes["session_observations"], tags) == 1
    assert _count(fakes["session_affinity_hits"], tags) == 0

    # Subsequent requests hit the new mapping -> hit (the "recover").
    third = await _route_session(router, sess)
    assert third == new_primary
    assert _count(fakes["session_observations"], tags) == 2
    assert _count(fakes["session_affinity_hits"], tags) == 1


@pytest.mark.asyncio
async def test_session_affinity_miss_on_saturation_fallback(router):
    """When the primary is saturated and the request falls through to the
    fallback, the affinity check observes the change of replica → miss.
    This is correct: the session genuinely landed on a different replica
    than the previous request, even though the ring itself didn't change."""
    replicas = _ready_replicas(3)
    router.update_replicas(replicas)
    fakes = _install_fake_metrics(router)
    tags = fakes["default_tags"]

    sess = "sess_sat"
    ranked = router._lookup_ranked_replicas(sess)
    primary_id, fallback_id = ranked[0], ranked[1]

    # First call lands on the primary (no saturation).
    first = await _route_session(router, sess)
    assert first == primary_id
    assert _count(fakes["session_observations"], tags) == 0  # first sighting

    # Saturate the primary. Next call falls through to fallback_1 → miss.
    for r in replicas:
        if r.replica_id == primary_id:
            r.set_queue_len_response(DEFAULT_MAX_ONGOING_REQUESTS)
        else:
            r.set_queue_len_response(0)

    second = await _route_session(router, sess)
    assert second == fallback_id
    assert _count(fakes["session_observations"], tags) == 1
    assert _count(fakes["session_affinity_hits"], tags) == 0


@pytest.mark.asyncio
async def test_session_affinity_lru_evicts_oldest(router):
    """Once the LRU fills, the oldest entry is evicted; a return visit to
    the evicted session is counted as a fresh sighting (no observation)."""
    router.update_replicas(_ready_replicas(3))
    fakes = _install_fake_metrics(router)
    tags = fakes["default_tags"]

    # Seed the LRU exactly to the cap: 1 + (LRU_MAX - 1) = LRU_MAX entries.
    await _route_session(router, "sess_evictee")
    for i in range(SESSION_AFFINITY_LRU_MAX - 1):
        await _route_session(router, f"sess_filler_{i}")

    assert "sess_evictee" in router._last_replica_per_session
    assert len(router._last_replica_per_session) == SESSION_AFFINITY_LRU_MAX

    # One more fresh session triggers eviction of the oldest (sess_evictee).
    await _route_session(router, "sess_overflow")
    assert "sess_evictee" not in router._last_replica_per_session

    # All sightings so far were first-time → no observations recorded.
    assert _count(fakes["session_observations"], tags) == 0

    # Return visit to the evicted session counts as a fresh sighting,
    # NOT an observation, because the LRU lost its previous mapping.
    await _route_session(router, "sess_evictee")
    assert _count(fakes["session_observations"], tags) == 0


@pytest.mark.asyncio
async def test_session_affinity_lru_touches_recency_on_repeat_hit(router):
    """Re-sighting of a session moves it to MRU; an older entry gets
    evicted instead. Validates the LRU ordering used to bound memory."""
    router.update_replicas(_ready_replicas(3))
    _install_fake_metrics(router)

    await _route_session(router, "sess_kept")  # oldest
    await _route_session(router, "sess_evicted_first")  # second-oldest
    await _route_session(router, "sess_kept")  # touch -> MRU

    # Fill the LRU up to its cap.
    for i in range(SESSION_AFFINITY_LRU_MAX - 2):
        await _route_session(router, f"sess_filler_{i}")

    # One more fresh session triggers an eviction.
    await _route_session(router, "sess_overflow")
    assert "sess_evicted_first" not in router._last_replica_per_session
    assert "sess_kept" in router._last_replica_per_session


@pytest.mark.asyncio
async def test_anonymous_traffic_skips_session_affinity(router):
    """A session-less request must not seed the affinity LRU or touch
    its counters — there is no session_id to track affinity against."""
    router.update_replicas(_ready_replicas(3))
    fakes = _install_fake_metrics(router)
    tags = fakes["default_tags"]

    await _route_session(router, session_id="")

    assert _count(fakes["session_observations"], tags) == 0
    assert _count(fakes["session_affinity_hits"], tags) == 0
    assert router._last_replica_per_session == {}


@pytest.mark.asyncio
async def test_session_affinity_dip_and_recover_curve(router):
    """End-to-end "dip and recover" check: many sessions on a stable ring
    yield ~100% affinity, a scale-up causes a dip, and post-rebuild
    repeated traffic returns to ~100%. Mirrors the production dashboard
    plot for consistent-hashing correctness."""
    R0 = 4
    initial = _ready_replicas(R0)
    router.update_replicas(initial)
    fakes = _install_fake_metrics(router)
    tags = fakes["default_tags"]

    sessions = [f"user_{i}" for i in range(64)]

    # Phase 1: seed each session once. No observations yet (first sightings).
    for s in sessions:
        await _route_session(router, s)
    assert _count(fakes["session_observations"], tags) == 0
    assert _count(fakes["session_affinity_hits"], tags) == 0

    # Phase 2: stable ring -- second pass yields 100% hits.
    for s in sessions:
        await _route_session(router, s)
    pre_obs = _count(fakes["session_observations"], tags)
    pre_hits = _count(fakes["session_affinity_hits"], tags)
    assert pre_obs == len(sessions)
    assert pre_hits == len(sessions)

    # Phase 3: scale up -> ring rebuild relocates ~1/(R0+1) sessions.
    new_replica = FakeRunningReplica("r_new")
    new_replica.set_queue_len_response(0)
    router.update_replicas(initial + [new_replica])

    base_obs = _count(fakes["session_observations"], tags)
    base_hits = _count(fakes["session_affinity_hits"], tags)

    for s in sessions:
        await _route_session(router, s)
    dip_obs = _count(fakes["session_observations"], tags) - base_obs
    dip_hits = _count(fakes["session_affinity_hits"], tags) - base_hits

    # All sessions are observed (they were in the LRU before the rebuild).
    assert dip_obs == len(sessions)
    # The hit rate must dip below 100% — at least one session must have
    # moved to the new replica. Theoretically ~1/(R0+1) sessions move,
    # so we assert hit_rate < 1.0 with a generous bound.
    dip_hit_rate = dip_hits / dip_obs
    assert dip_hit_rate < 1.0, (
        f"Expected the rebuild to remap at least one session, but the "
        f"affinity hit rate stayed at {dip_hit_rate:.3f}."
    )

    # Phase 4: post-rebuild ring is again stable, so the next pass
    # recovers to 100% hit rate -- the "recover" half of the curve.
    base_obs2 = _count(fakes["session_observations"], tags)
    base_hits2 = _count(fakes["session_affinity_hits"], tags)
    for s in sessions:
        await _route_session(router, s)
    recovery_obs = _count(fakes["session_observations"], tags) - base_obs2
    recovery_hits = _count(fakes["session_affinity_hits"], tags) - base_hits2
    assert recovery_obs == len(sessions)
    assert recovery_hits == len(sessions), (
        "Affinity rate did not recover to 100% after a single full pass "
        "post-rebuild; the LRU may not be advancing to the new replica."
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
