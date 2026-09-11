import asyncio
import sys

import pytest

from ray.serve._private.common import (
    DeploymentHandleSource,
    RequestMetadata,
)
from ray.serve._private.request_router import PendingRequest
from ray.serve._private.request_router import request_router as request_router_module
from ray.serve._private.test_utils import (
    FAKE_REPLICA_DEPLOYMENT_ID as DEPLOYMENT_ID,
    FakeRunningReplica,
    MockTimer,
)
from ray.serve._private.utils import generate_request_id
from ray.serve.experimental.round_robin_router import (
    _MAX_ROUND_ROBIN_COUNTER,
    RoundRobinRouter,
)

TIMER = MockTimer()


def _make_request(model_id: str = "") -> PendingRequest:
    return PendingRequest(
        args=[],
        kwargs={},
        metadata=RequestMetadata(
            request_id=generate_request_id(),
            internal_request_id=generate_request_id(),
            multiplexed_model_id=model_id,
        ),
    )


@pytest.fixture
def router(request) -> RoundRobinRouter:
    if not hasattr(request, "param"):
        request.param = {}

    params = dict(request.param)
    use_replica_queue_len_cache = params.pop("use_replica_queue_len_cache", False)

    TIMER.reset()
    request_router = RoundRobinRouter(
        deployment_id=DEPLOYMENT_ID,
        handle_source=DeploymentHandleSource.REPLICA,
        self_actor_id="fake-actor-id",
        self_actor_handle=None,
        use_replica_queue_len_cache=use_replica_queue_len_cache,
        get_curr_time_s=TIMER.time,
        initial_backoff_s=0.001,
        backoff_multiplier=1,
        max_backoff_s=0.001,
    )
    request_router.initialize_state(**params)

    yield request_router

    assert request_router.curr_num_routing_tasks == 0
    assert request_router.num_pending_requests == 0


def _make_replicas(*replica_ids: str, **kwargs):
    replicas = [FakeRunningReplica(replica_id, **kwargs) for replica_id in replica_ids]
    for replica in replicas:
        replica.set_queue_len_response(0)
    return replicas


def _ranked_replica_unique_ids(ranks):
    return [[replica.replica_id.unique_id for replica in rank] for rank in ranks]


def test_initialize_state_ignores_router_kwargs(router):
    router.initialize_state(unused=True)


def test_round_robin_counter_initialized_randomly(router):
    assert 0 <= router._round_robin_counter < _MAX_ROUND_ROBIN_COUNTER


@pytest.mark.asyncio
async def test_choose_replicas_round_robin_order(router):
    replicas = _make_replicas("r2", "r0", "r1")
    router.update_replicas(replicas)
    router._round_robin_counter = 0
    await asyncio.sleep(0)

    assert _ranked_replica_unique_ids(
        await router.choose_replicas(replicas, _make_request())
    ) == [["r2"], ["r0"], ["r1"]]
    assert _ranked_replica_unique_ids(
        await router.choose_replicas(replicas, _make_request())
    ) == [["r0"], ["r1"], ["r2"]]
    assert _ranked_replica_unique_ids(
        await router.choose_replicas(replicas, _make_request())
    ) == [["r1"], ["r2"], ["r0"]]
    assert _ranked_replica_unique_ids(
        await router.choose_replicas(replicas, _make_request())
    ) == [["r2"], ["r0"], ["r1"]]


@pytest.mark.asyncio
async def test_prefers_loaded_model(router):
    replicas = [
        FakeRunningReplica("r0", model_ids={"model-a"}),
        FakeRunningReplica("r1"),
        FakeRunningReplica("r2", model_ids={"model-a"}),
    ]
    for replica in replicas:
        replica.set_queue_len_response(0)
    router.update_replicas(replicas)
    router._multiplexed_round_robin_counters["model-a"] = 0
    await asyncio.sleep(0)

    assert _ranked_replica_unique_ids(
        await router.choose_replicas(replicas, _make_request(model_id="model-a"))
    ) == [["r0"], ["r2"]]
    assert _ranked_replica_unique_ids(
        await router.choose_replicas(replicas, _make_request(model_id="model-a"))
    ) == [["r2"], ["r0"]]
    assert _ranked_replica_unique_ids(
        await router.choose_replicas(replicas, _make_request(model_id="model-a"))
    ) == [["r0"], ["r2"]]


@pytest.mark.asyncio
async def test_round_robins_warm_replicas(router):
    replicas = [
        FakeRunningReplica("r0", model_ids={"model-a"}),
        FakeRunningReplica("r1", model_ids={"model-a"}),
        FakeRunningReplica("r2", model_ids={"model-b"}),
        FakeRunningReplica("r3", model_ids={"model-b"}),
    ]
    for replica in replicas:
        replica.set_queue_len_response(0)
    router.update_replicas(replicas)
    router._multiplexed_round_robin_counters["model-a"] = 0
    router._multiplexed_round_robin_counters["model-b"] = 0
    await asyncio.sleep(0)

    assert _ranked_replica_unique_ids(
        await router.choose_replicas(replicas, _make_request(model_id="model-a"))
    ) == [["r0"], ["r1"]]
    assert _ranked_replica_unique_ids(
        await router.choose_replicas(replicas, _make_request(model_id="model-b"))
    ) == [["r2"], ["r3"]]
    assert _ranked_replica_unique_ids(
        await router.choose_replicas(replicas, _make_request(model_id="model-a"))
    ) == [["r1"], ["r0"]]
    assert _ranked_replica_unique_ids(
        await router.choose_replicas(replicas, _make_request(model_id="model-b"))
    ) == [["r3"], ["r2"]]


@pytest.mark.asyncio
async def test_shared_cursor_for_fallback(router):
    replicas = [
        FakeRunningReplica("r0", model_ids={"model-a"}),
        FakeRunningReplica("r1", model_ids={"model-b"}),
        FakeRunningReplica("r2"),
        FakeRunningReplica("r3"),
    ]
    for replica in replicas:
        replica.set_queue_len_response(0)
    router.update_replicas(replicas)
    router._round_robin_counter = 0
    await asyncio.sleep(0)

    # Models C and D are not loaded. Both fall back to r2 and r3, the replicas
    # with the fewest loaded models, and use the router's shared cursor.
    assert _ranked_replica_unique_ids(
        await router.choose_replicas(replicas, _make_request(model_id="model-c"))
    ) == [["r2"], ["r3"]]
    assert _ranked_replica_unique_ids(
        await router.choose_replicas(replicas, _make_request(model_id="model-d"))
    ) == [["r3"], ["r2"]]


@pytest.mark.asyncio
async def test_shared_cursor_for_overlap(router, monkeypatch):
    current_time = 100.0
    monkeypatch.setattr(request_router_module.time, "time", lambda: current_time)
    monkeypatch.setattr(
        request_router_module,
        "RAY_SERVE_MULTIPLEXED_MODEL_ID_MATCHING_TIMEOUT_S",
        1.0,
    )

    replicas = [
        FakeRunningReplica("r0", model_ids={"model-a"}),
        FakeRunningReplica("r1", model_ids={"model-a"}),
        FakeRunningReplica("r2", model_ids={"model-b", "model-c"}),
        FakeRunningReplica("r3", model_ids={"model-d", "model-e"}),
    ]
    for replica in replicas:
        replica.set_queue_len_response(0)
    router.update_replicas(replicas)
    router._multiplexed_round_robin_counters["model-a"] = 4
    router._round_robin_counter = 0
    await asyncio.sleep(0)

    request = _make_request(model_id="model-a")

    # The warm and fewest-model tiers both contain r0 and r1.
    assert _ranked_replica_unique_ids(
        await router.choose_replicas(replicas, request)
    ) == [["r0"], ["r1"]]

    # A retry is in the fewest-model tier and must use the shared cursor.
    current_time = 100.5
    assert _ranked_replica_unique_ids(
        await router.choose_replicas(replicas, request)
    ) == [["r0"], ["r1"]]


@pytest.mark.asyncio
async def test_prunes_stale_cursors(router):
    replicas = [
        FakeRunningReplica("r0", model_ids={"model-a"}),
        FakeRunningReplica("r1", model_ids={"model-b"}),
    ]
    for replica in replicas:
        replica.set_queue_len_response(0)
    router.update_replicas(replicas)
    router._multiplexed_round_robin_counters = {
        "model-a": 1,
        "model-b": 2,
    }

    updated_replicas = [
        FakeRunningReplica("r0", model_ids={"model-b"}),
        FakeRunningReplica("r1", model_ids={"model-b"}),
    ]
    for replica in updated_replicas:
        replica.set_queue_len_response(0)
    router.update_replicas(updated_replicas)
    await asyncio.sleep(0)

    assert router._multiplexed_round_robin_counters == {"model-b": 2}


@pytest.mark.asyncio
async def test_multiplex_priority_order(router, monkeypatch):
    current_time = 100.0
    monkeypatch.setattr(request_router_module.time, "time", lambda: current_time)
    monkeypatch.setattr(
        request_router_module,
        "RAY_SERVE_MULTIPLEXED_MODEL_ID_MATCHING_TIMEOUT_S",
        1.0,
    )

    replicas = [
        FakeRunningReplica("r0", model_ids={"model-a", "model-x"}),
        FakeRunningReplica("r1", model_ids={"model-a", "model-y"}),
        FakeRunningReplica("r2", model_ids={"model-a", "model-z"}),
        FakeRunningReplica("r3"),
        FakeRunningReplica("r4"),
    ]
    for replica in replicas:
        replica.set_queue_len_response(0)
    router.update_replicas(replicas)
    router._multiplexed_round_robin_counters["model-a"] = 4
    router._round_robin_counter = 5
    await asyncio.sleep(0)

    request = _make_request(model_id="model-a")

    # First try replicas with model A already loaded.
    assert _ranked_replica_unique_ids(
        await router.choose_replicas(replicas, request)
    ) == [["r1"], ["r2"], ["r0"]]

    # Before the matching timeout, retries use the fewest-model replicas.
    current_time = 100.5
    assert _ranked_replica_unique_ids(
        await router.choose_replicas(replicas, request)
    ) == [["r4"], ["r3"]]

    # After the timeout, try those replicas once more, then all replicas.
    # The randomly selected matching timeout is in [1, 2] seconds.
    current_time = 102.5
    assert _ranked_replica_unique_ids(
        await router.choose_replicas(replicas, request)
    ) == [["r3"], ["r4"]]
    assert _ranked_replica_unique_ids(
        await router.choose_replicas(replicas, request)
    ) == [["r2"], ["r3"], ["r4"], ["r0"], ["r1"]]


@pytest.mark.parametrize(
    "router", [{"use_replica_queue_len_cache": True}], indirect=True
)
@pytest.mark.asyncio
async def test_choose_replicas_includes_cached_full_replica_first(router):
    replicas = [
        FakeRunningReplica("r0", max_ongoing_requests=1),
        FakeRunningReplica("r1", max_ongoing_requests=1),
        FakeRunningReplica("r2", max_ongoing_requests=1),
    ]
    for replica in replicas:
        replica.set_queue_len_response(0)
    router.update_replicas(replicas)
    router.replica_queue_len_cache.update(replicas[0].replica_id, 1)
    router._round_robin_counter = 0
    await asyncio.sleep(0)

    # Selection should preserve strict round-robin order even if fulfillment
    # later rejects the cached-full replica and advances to the next rank.
    assert _ranked_replica_unique_ids(
        await router.choose_replicas(replicas, _make_request())
    ) == [["r0"], ["r1"], ["r2"]]


@pytest.mark.asyncio
async def test_choose_replica_falls_back_when_current_candidate_full(router):
    replicas = [
        FakeRunningReplica("r0", max_ongoing_requests=1),
        FakeRunningReplica("r1"),
        FakeRunningReplica("r2"),
    ]
    replicas[0].set_queue_len_response(1)
    replicas[1].set_queue_len_response(0)
    replicas[2].set_queue_len_response(0)
    router.update_replicas(replicas)
    router._round_robin_counter = 0
    await asyncio.sleep(0)

    chosen = await router._choose_replica_for_request(_make_request())
    assert chosen.replica_id.unique_id == "r1"
    await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_choose_replicas_empty_candidates(router):
    counter = router._round_robin_counter
    assert await router.choose_replicas([], _make_request()) == []
    assert router._round_robin_counter == counter


@pytest.mark.asyncio
async def test_choose_replicas_sets_should_backoff(router):
    # Without should_backoff, the base class tight-loops choose_replicas when
    # every replica is at capacity (see _choose_replicas_with_backoff).
    replicas = _make_replicas("r0", "r1")
    router.update_replicas(replicas)
    await asyncio.sleep(0)

    request = _make_request()
    assert request.routing_context.should_backoff is False
    await router.choose_replicas(replicas, request)
    assert request.routing_context.should_backoff is True


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
