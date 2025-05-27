import os
import asyncio
from types import SimpleNamespace
from typing import Optional, Set, List

import pytest


from ray._private.test_utils import get_or_create_event_loop
from ray.serve._private.common import (
    DeploymentHandleSource,
    DeploymentID,
    ReplicaID,
    RequestMetadata,
)
from ray.serve._private.replica_scheduler.prefix_aware_scheduler import (
    PrefixAwareReplicaScheduler,
)
from ray.serve._private.replica_scheduler import PendingRequest, RunningReplica
from ray.serve._private.test_utils import MockTimer
from ray.serve._private.utils import generate_request_id

# === Constants & test‐utility classes copied from the Po2 tests ===

TIMER = MockTimer()
DEFAULT_MAX_ONGOING_REQUESTS = 10
SCHEDULER_NODE_ID = "scheduler_node_id"
SCHEDULER_AZ = "scheduler_az"


class FakeRunningReplica(RunningReplica):
    def __init__(
        self,
        replica_unique_id: str,
        *,
        node_id: str = "",
        availability_zone: Optional[str] = None,
        reset_after_response: bool = False,
        model_ids: Optional[Set[str]] = None,
        sleep_time_s: float = 0.0,
        max_ongoing_requests: int = DEFAULT_MAX_ONGOING_REQUESTS,
    ):
        self._replica_id = ReplicaID(
            unique_id=replica_unique_id,
            deployment_id=DeploymentID(name="TEST_DEPLOYMENT"),
        )
        self._node_id = node_id
        self._availability_zone = availability_zone
        self._queue_len = 0
        self._max_ongoing_requests = max_ongoing_requests
        self._has_queue_len_response = asyncio.Event()
        self._reset_after_response = reset_after_response
        self._model_ids = model_ids or set()
        self._sleep_time_s = sleep_time_s

        self.get_queue_len_was_cancelled = False
        self.queue_len_deadline_history: List[float] = []
        self.num_get_queue_len_calls = 0

    @property
    def replica_id(self) -> ReplicaID:
        return self._replica_id

    @property
    def node_id(self) -> str:
        return self._node_id

    @property
    def availability_zone(self) -> Optional[str]:
        return self._availability_zone

    @property
    def multiplexed_model_ids(self) -> Set[str]:
        return self._model_ids

    @property
    def max_ongoing_requests(self) -> int:
        return self._max_ongoing_requests

    def set_queue_len_response(
        self, queue_len: int, exception: Optional[Exception] = None
    ):
        self._queue_len = queue_len
        self._exception = exception
        self._has_queue_len_response.set()

    async def get_queue_len(self, *, deadline_s: float) -> int:
        self.num_get_queue_len_calls += 1
        self.queue_len_deadline_history.append(deadline_s)
        try:
            await self._has_queue_len_response.wait()
            if self._sleep_time_s > 0:
                await asyncio.sleep(self._sleep_time_s)
            if self._reset_after_response:
                self._has_queue_len_response.clear()
            if getattr(self, "_exception", None):
                raise self._exception
            return self._queue_len
        except asyncio.CancelledError:
            self.get_queue_len_was_cancelled = True
            raise

    def send_request(self, pr: PendingRequest) -> None:
        raise NotImplementedError()

    def send_request_with_rejection(self, pr: PendingRequest) -> None:
        raise NotImplementedError()


def fake_pending_request(
    *, created_at: Optional[float] = None, model_id: str = ""
) -> PendingRequest:
    meta = RequestMetadata(
        request_id=generate_request_id(),
        internal_request_id=generate_request_id(),
        multiplexed_model_id=model_id,
    )
    return PendingRequest(args=[], kwargs={}, metadata=meta, created_at=created_at)


# === Scheduler fixture ===


@pytest.fixture
def prefix_scheduler(request) -> PrefixAwareReplicaScheduler:
    """Construct a PrefixAwareReplicaScheduler on its own loop."""
    # Ensure we get a fresh constant reload as in the Po2 tests.
    os.environ["RAY_SERVE_MULTIPLEXED_MODEL_ID_MATCHING_TIMEOUT_S"] = "0.01"
    import importlib

    import ray.serve._private.constants

    import ray.serve._private.replica_scheduler.replica_scheduler

    importlib.reload(ray.serve._private.constants)
    importlib.reload(ray.serve._private.replica_scheduler.replica_scheduler)

    TIMER.reset()

    async def _construct(loop: asyncio.AbstractEventLoop):
        scheduler = PrefixAwareReplicaScheduler(
            deployment_id=DeploymentID(name="TEST_DEPLOYMENT"),
            handle_source=request.param.get(
                "handle_source", DeploymentHandleSource.REPLICA
            ),
            prefer_local_node=request.param.get("prefer_local_node", False),
            prefer_local_az=request.param.get("prefer_local_az", False),
            self_node_id=SCHEDULER_NODE_ID,
            self_actor_id="fake-actor-id",
            self_actor_handle=None,
            self_availability_zone=request.param.get("az", None),
            use_replica_queue_len_cache=request.param.get(
                "use_replica_queue_len_cache", False
            ),
            imbalanced_threshold=request.param.get("imbalanced_threshold", 10),
            match_rate_threshold=request.param.get("match_rate_threshold", 0.1),
            get_curr_time_s=TIMER.time,
        )
        # zero backoff for determinism
        scheduler.backoff_sequence_s = [0.0] * 10
        return scheduler

    loop = get_or_create_event_loop()
    s = asyncio.new_event_loop().run_until_complete(_construct(loop))
    yield s
    # After each test, no stray tasks or pending requests should remain:
    assert s.curr_num_scheduling_tasks == 0
    assert s.num_pending_requests == 0


# === 1️⃣ Fallback to Po2 when no prefix logic applies ===


@pytest.mark.asyncio
@pytest.mark.parametrize("prefix_scheduler", [{}], indirect=True)
async def test_fallback_on_empty_args(prefix_scheduler):
    s = prefix_scheduler
    loop = get_or_create_event_loop()

    r1 = FakeRunningReplica("r1")
    r1.set_queue_len_response(0)
    r2 = FakeRunningReplica("r2")
    r2.set_queue_len_response(1)
    s.update_replicas([r1, r2])

    # args=[] → no prompt/messages → should behave exactly like Po2: pick the least busy
    results = [
        await s.choose_replica_for_request(fake_pending_request()) for _ in range(10)
    ]
    assert all(replica == r1 for replica in results)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "prefix_scheduler", [{"imbalanced_threshold": 1}], indirect=True
)
async def test_fallback_on_imbalanced_load(prefix_scheduler):
    s = prefix_scheduler
    loop = get_or_create_event_loop()

    # one busy, one free → difference > threshold=1 → must fallback to Po2
    r1 = FakeRunningReplica("r1")
    r1.set_queue_len_response(0)
    r2 = FakeRunningReplica("r2")
    r2.set_queue_len_response(5)
    s.update_replicas([r1, r2])

    results = [
        await s.choose_replica_for_request(fake_pending_request()) for _ in range(10)
    ]
    assert all(replica == r1 for replica in results)


# === 2️⃣ Core prefix‐aware behaviors ===


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "prefix_scheduler", [{"imbalanced_threshold": 100}], indirect=True
)
async def test_highest_prefix_match_rate(prefix_scheduler):
    s = prefix_scheduler
    loop = get_or_create_event_loop()

    # Stub out the tree actor so that only the second replica "B" matches the prefix.
    async def fake_prefix_match(text: str, candidates: List[str]):
        # pretend we matched 5 chars out of len(text)=11 → 0.45 > default 0.1
        return ("Hello", [candidates[1]])

    s._tree_actor = SimpleNamespace()
    s._tree_actor.prefix_match = SimpleNamespace()
    s._tree_actor.prefix_match.remote = fake_prefix_match

    # two replicas with identical queue lengths
    rA = FakeRunningReplica("A")
    rA.set_queue_len_response(0)
    rB = FakeRunningReplica("B")
    rB.set_queue_len_response(0)
    s.update_replicas([rA, rB])

    # craft a request whose .args[0].prompt triggers our fake_prefix_match
    req = fake_pending_request()
    req.args = [SimpleNamespace(prompt="Hello world")]
    # every single scheduling should pick B
    results = [await s.choose_replica_for_request(req) for _ in range(10)]
    assert all(r == rB for r in results)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "prefix_scheduler",
    [{"imbalanced_threshold": 100, "match_rate_threshold": 0.8}],
    indirect=True,
)
async def test_fallback_to_smallest_tree_on_low_match_rate(prefix_scheduler):
    s = prefix_scheduler
    loop = get_or_create_event_loop()

    # match_rate = 2/11 < 0.8 → trigger smallest‐tenants logic
    async def fake_prefix_match(text: str, candidates: List[str]):
        return ("He", candidates)  # matched_text len=2

    # pick always the first replica as "smallest"
    r1_str = ReplicaID("r1", DeploymentID("TEST_DEPLOYMENT")).to_full_id_str()

    async def fake_smallest():
        return [r1_str]

    s._tree_actor = SimpleNamespace()
    s._tree_actor.prefix_match = SimpleNamespace()
    s._tree_actor.prefix_match.remote = fake_prefix_match
    s._tree_actor.get_smallest_tenants = SimpleNamespace()
    s._tree_actor.get_smallest_tenants.remote = fake_smallest

    r1 = FakeRunningReplica("r1")
    r1.set_queue_len_response(0)
    r2 = FakeRunningReplica("r2")
    r2.set_queue_len_response(0)
    s.update_replicas([r1, r2])

    req = fake_pending_request()
    req.args = [SimpleNamespace(prompt="Hello world")]
    results = [await s.choose_replica_for_request(req) for _ in range(10)]
    assert all(r == r1 for r in results)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "prefix_scheduler", [{"imbalanced_threshold": 100}], indirect=True
)
async def test_fallback_to_po2_when_no_tenants_found(prefix_scheduler):
    s = prefix_scheduler
    loop = get_or_create_event_loop()

    # no match at all
    async def fake_prefix_match(text: str, candidates: List[str]):
        return ("", [])

    # return [] / None from smallest too → back to Po2
    async def fake_smallest():
        return []

    s._tree_actor = SimpleNamespace()
    s._tree_actor.prefix_match = SimpleNamespace()
    s._tree_actor.prefix_match.remote = fake_prefix_match
    s._tree_actor.get_smallest_tenants = SimpleNamespace()
    s._tree_actor.get_smallest_tenants.remote = fake_smallest

    r1 = FakeRunningReplica("r1")
    r1.set_queue_len_response(0)
    r2 = FakeRunningReplica("r2")
    r2.set_queue_len_response(0)
    s.update_replicas([r1, r2])

    req = fake_pending_request()
    req.args = [SimpleNamespace(prompt="Hello")]
    # with pure Po2 on two equally‐loaded, both should appear over many trials
    chosen = {await s.choose_replica_for_request(req) for _ in range(50)}
    assert chosen == {r1, r2}
