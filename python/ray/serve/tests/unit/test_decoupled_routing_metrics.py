"""Unit tests for Phase 2 decoupled routing primitive metrics.

Tests for serve_selection_dispatch_gap_ms (Histogram) and
serve_selections_released_without_dispatch (Counter) added to AsyncioRouter.

Issue: https://github.com/ray-project/ray/issues/62163
"""
import asyncio
import sys
from typing import Dict, Optional, Tuple, Union
from unittest.mock import Mock

import pytest

from ray._common.utils import get_or_create_event_loop  # noqa: E402
from ray.serve._private.common import (
    DeploymentHandleSource,
    DeploymentID,
    ReplicaID,
    RequestMetadata,
    RunningReplicaInfo,
)
from ray.serve._private.replica_result import ReplicaResult
from ray.serve._private.request_router import (
    PendingRequest,
    ReplicaSelection,
    RequestRouter,
    RunningReplica,
)
from ray.serve._private.request_router.common import ReplicaQueueLengthCache
from ray.serve._private.router import AsyncioRouter

# ---------------------------------------------------------------------------
# Fake metrics classes (defined locally to avoid importing from test_utils,
# which has many heavy dependencies).
# ---------------------------------------------------------------------------


class FakeCounter:
    """Fake Counter for unit tests without Ray."""

    def __init__(self, name: str = None, tag_keys: Tuple[str, ...] = None):
        self.name = name
        self.counts: Dict = {}
        self.tags = tag_keys or ()
        self.default_tags: Dict[str, str] = {}

    def set_default_tags(self, tags: Dict[str, str]):
        for key in tags:
            assert key in self.tags
        self.default_tags = tags.copy()

    def inc(self, value: Union[int, float] = 1.0, tags: Dict[str, str] = None):
        merged_tags = self.default_tags.copy()
        merged_tags.update(tags or {})
        assert set(merged_tags.keys()) == set(self.tags)

        d = self.counts
        for tag in self.tags[:-1]:
            tag_value = merged_tags[tag]
            if tag_value not in d:
                d[tag_value] = {}
            d = d[tag_value]

        key = merged_tags[self.tags[-1]]
        d[key] = d.get(key, 0) + value

    def get_count(self, tags: Dict[str, str]) -> Optional[int]:
        value = self.counts
        for tag in self.tags:
            tag_value = tags[tag]
            value = value.get(tag_value)
            if value is None:
                return None
        return value


class FakeHistogram:
    """Fake Histogram for unit tests without Ray."""

    def __init__(self, name: str = None, tag_keys: Tuple[str, ...] = None):
        self.name = name
        self.observations: list = []
        self.tags = tag_keys or ()
        self.default_tags: Dict[str, str] = {}

    def set_default_tags(self, tags: Dict[str, str]):
        for key in tags:
            assert key in self.tags
        self.default_tags = tags.copy()

    def observe(self, value: Union[int, float], tags: Dict[str, str] = None):
        merged_tags = self.default_tags.copy()
        merged_tags.update(tags or {})
        assert set(merged_tags.keys()) == set(self.tags)
        self.observations.append((value, merged_tags))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class FakeReplicaResult(ReplicaResult):
    def __init__(self, replica_id: ReplicaID, is_generator_object: bool = False):
        self._replica_id = replica_id
        self._is_generator_object = is_generator_object
        self._done_callbacks = []
        self.cancelled = False

    async def get_rejection_response(self):
        return None

    def get(self, timeout_s: Optional[float]):
        raise NotImplementedError

    async def get_async(self):
        raise NotImplementedError

    def __next__(self):
        raise NotImplementedError

    async def __anext__(self):
        raise NotImplementedError

    def add_done_callback(self, callback):
        self._done_callbacks.append(callback)

    def cancel(self):
        self.cancelled = True

    def to_object_ref(self, timeout_s: Optional[float]):
        raise NotImplementedError

    async def to_object_ref_async(self):
        raise NotImplementedError

    def to_object_ref_gen(self):
        raise NotImplementedError


class FakeReplica(RunningReplica):
    def __init__(self, replica_id: ReplicaID, *, is_cross_language: bool = False):
        self._replica_id = replica_id
        self._is_cross_language = is_cross_language

    @property
    def replica_id(self) -> ReplicaID:
        return self._replica_id

    @property
    def actor_id(self):
        return None

    @property
    def is_cross_language(self) -> bool:
        return self._is_cross_language

    def get_queue_len(self, *, deadline_s: float) -> int:
        raise NotImplementedError

    def try_send_request(
        self, pr: PendingRequest, with_rejection: bool
    ) -> FakeReplicaResult:
        return FakeReplicaResult(self._replica_id)


class FakeRequestRouter(RequestRouter):
    """Minimal RequestRouter that always returns a fixed replica."""

    def __init__(self, use_queue_len_cache: bool = False):
        self._replica_to_return: Optional[FakeReplica] = None
        self._replica_queue_len_cache = ReplicaQueueLengthCache()
        self._use_replica_queue_len_cache = use_queue_len_cache
        self._block_requests = False
        self._blocked_events: list = []

    def create_replica_wrapper(self, replica_info: RunningReplicaInfo):
        return FakeReplica(replica_info.replica_id)

    @property
    def replica_queue_len_cache(self) -> ReplicaQueueLengthCache:
        return self._replica_queue_len_cache

    @property
    def curr_replicas(self):
        if self._replica_to_return is not None:
            return {self._replica_to_return.replica_id: self._replica_to_return}
        return {}

    def update_replicas(self, replicas):
        pass

    def on_replica_actor_died(self, replica_id: ReplicaID):
        pass

    def on_new_queue_len_info(self, replica_id, queue_len_info):
        pass

    def on_send_request(self, replica_id: ReplicaID):
        if self._use_replica_queue_len_cache:
            current = self._replica_queue_len_cache.get(replica_id) or 0
            self._replica_queue_len_cache.update(replica_id, current + 1)

    def on_replica_actor_unavailable(self, replica_id: ReplicaID):
        pass

    def on_request_routed(self, pending_request, replica_id, result):
        pass

    def on_request_completed(self, replica_id, internal_request_id):
        pass

    async def _choose_replica_for_request(
        self, pr: PendingRequest, *, is_retry: bool = False
    ) -> FakeReplica:
        if self._block_requests:
            event = asyncio.Event()
            self._blocked_events.append(event)
            await event.wait()
        assert self._replica_to_return is not None, "Set a replica to return."
        return self._replica_to_return

    async def choose_replicas(self, candidate_replicas, pending_request=None):
        pass

    def set_replica(self, replica: FakeReplica):
        self._replica_to_return = replica


def _make_deployment_id(name: str = "test-dep", app: str = "test-app") -> DeploymentID:
    return DeploymentID(name=name, app_name=app)


def _make_pending_request() -> PendingRequest:
    return PendingRequest(
        args=[],
        kwargs={},
        metadata=RequestMetadata(
            request_id="req-1",
            internal_request_id="internal-req-1",
        ),
    )


@pytest.fixture
async def router_with_fake_metrics() -> Tuple[
    AsyncioRouter, FakeRequestRouter, FakeHistogram, FakeCounter
]:
    """Fixture yielding (router, fake_request_router, fake_histogram, fake_counter).

    The histogram and counter replace the real Ray metrics on the router so we
    can assert observations without a running Ray cluster.
    """
    deployment_id = _make_deployment_id()
    fake_request_router = FakeRequestRouter()

    router = AsyncioRouter(
        controller_handle=Mock(),
        deployment_id=deployment_id,
        handle_id="test-handle",
        self_actor_id="test-actor",
        handle_source=DeploymentHandleSource.UNKNOWN,
        event_loop=get_or_create_event_loop(),
        enable_strict_max_ongoing_requests=False,
        request_router=fake_request_router,
        node_id="test-node",
        availability_zone="test-az",
        prefer_local_node_routing=False,
        _request_router_initialized_event=asyncio.Event(),
    )

    # Inject fake metrics so we can assert on them without Ray running.
    fake_histogram = FakeHistogram(tag_keys=("deployment", "application"))
    fake_histogram.set_default_tags(
        {"deployment": deployment_id.name, "application": deployment_id.app_name}
    )
    fake_counter = FakeCounter(tag_keys=("deployment", "application"))
    fake_counter.set_default_tags(
        {"deployment": deployment_id.name, "application": deployment_id.app_name}
    )
    router._selection_dispatch_gap_ms = fake_histogram
    router._selections_released_without_dispatch = fake_counter

    yield router, fake_request_router, fake_histogram, fake_counter

    await router.shutdown()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestSelectionDispatchGapMetric:
    """serve_selection_dispatch_gap_ms is observed on successful dispatch."""

    async def test_gap_is_positive_on_dispatch(self, router_with_fake_metrics):
        router, fake_rr, histogram, counter = router_with_fake_metrics

        replica = FakeReplica(
            ReplicaID(unique_id="r1", deployment_id=_make_deployment_id())
        )
        fake_rr.set_replica(replica)

        pr = _make_pending_request()
        async with router.choose_replica(pr) as selection:
            router.dispatch(selection, pr)

        assert len(histogram.observations) == 1
        gap_ms, _ = histogram.observations[0]
        assert gap_ms >= 0, "Gap must be non-negative"

    async def test_gap_not_observed_without_dispatch(self, router_with_fake_metrics):
        """Histogram is NOT observed when dispatch() is skipped."""
        router, fake_rr, histogram, counter = router_with_fake_metrics

        replica = FakeReplica(
            ReplicaID(unique_id="r1", deployment_id=_make_deployment_id())
        )
        fake_rr.set_replica(replica)

        pr = _make_pending_request()
        # Exit the context without calling dispatch().
        async with router.choose_replica(pr) as _:
            pass

        assert len(histogram.observations) == 0

    async def test_gap_tags_are_correct(self, router_with_fake_metrics):
        """Histogram observation is tagged with deployment and application."""
        router, fake_rr, histogram, counter = router_with_fake_metrics

        replica = FakeReplica(
            ReplicaID(unique_id="r1", deployment_id=_make_deployment_id())
        )
        fake_rr.set_replica(replica)

        pr = _make_pending_request()
        async with router.choose_replica(pr) as selection:
            router.dispatch(selection, pr)

        assert len(histogram.observations) == 1
        _, tags = histogram.observations[0]
        assert tags["deployment"] == "test-dep"
        assert tags["application"] == "test-app"


@pytest.mark.asyncio
class TestSelectionsReleasedWithoutDispatch:
    """serve_selections_released_without_dispatch increments correctly."""

    async def test_counter_increments_when_no_dispatch(self, router_with_fake_metrics):
        router, fake_rr, histogram, counter = router_with_fake_metrics

        replica = FakeReplica(
            ReplicaID(unique_id="r1", deployment_id=_make_deployment_id())
        )
        fake_rr.set_replica(replica)

        pr = _make_pending_request()
        async with router.choose_replica(pr) as _:
            pass  # Deliberately skip dispatch().

        tags = {"deployment": "test-dep", "application": "test-app"}
        assert counter.get_count(tags) == 1

    async def test_counter_increments_when_exception_raised(
        self, router_with_fake_metrics
    ):
        """Counter increments even when the context exits via an exception."""
        router, fake_rr, histogram, counter = router_with_fake_metrics

        replica = FakeReplica(
            ReplicaID(unique_id="r1", deployment_id=_make_deployment_id())
        )
        fake_rr.set_replica(replica)

        pr = _make_pending_request()
        with pytest.raises(RuntimeError, match="abort"):
            async with router.choose_replica(pr) as _:
                raise RuntimeError("abort")

        tags = {"deployment": "test-dep", "application": "test-app"}
        assert counter.get_count(tags) == 1

    async def test_counter_not_incremented_on_dispatch(self, router_with_fake_metrics):
        """Counter stays at zero when dispatch() is called successfully."""
        router, fake_rr, histogram, counter = router_with_fake_metrics

        replica = FakeReplica(
            ReplicaID(unique_id="r1", deployment_id=_make_deployment_id())
        )
        fake_rr.set_replica(replica)

        pr = _make_pending_request()
        async with router.choose_replica(pr) as selection:
            router.dispatch(selection, pr)

        tags = {"deployment": "test-dep", "application": "test-app"}
        assert counter.get_count(tags) is None  # Never incremented.

    async def test_counter_tags_are_correct(self, router_with_fake_metrics):
        """Counter increment is tagged with deployment and application."""
        router, fake_rr, histogram, counter = router_with_fake_metrics

        replica = FakeReplica(
            ReplicaID(unique_id="r1", deployment_id=_make_deployment_id())
        )
        fake_rr.set_replica(replica)

        pr = _make_pending_request()
        async with router.choose_replica(pr) as _:
            pass

        assert (
            counter.get_count({"deployment": "test-dep", "application": "test-app"})
            == 1
        )
        # Wrong tag values should not match.
        assert (
            counter.get_count({"deployment": "other", "application": "test-app"})
            is None
        )

    async def test_counter_accumulates_across_multiple_aborts(
        self, router_with_fake_metrics
    ):
        """Counter accumulates when multiple selections are released without dispatch."""
        router, fake_rr, histogram, counter = router_with_fake_metrics

        replica = FakeReplica(
            ReplicaID(unique_id="r1", deployment_id=_make_deployment_id())
        )
        fake_rr.set_replica(replica)

        tags = {"deployment": "test-dep", "application": "test-app"}

        for _ in range(3):
            pr = _make_pending_request()
            async with router.choose_replica(pr) as _:
                pass

        assert counter.get_count(tags) == 3


@pytest.mark.asyncio
class TestReplicaSelectionDataclass:
    """Unit tests for the ReplicaSelection dataclass itself."""

    def test_dispatched_starts_false(self):
        replica = FakeReplica(
            ReplicaID(unique_id="r1", deployment_id=_make_deployment_id())
        )
        sel = ReplicaSelection(replica=replica)
        assert sel._dispatched is False

    def test_mark_dispatched_sets_flag(self):
        replica = FakeReplica(
            ReplicaID(unique_id="r1", deployment_id=_make_deployment_id())
        )
        sel = ReplicaSelection(replica=replica)
        sel._mark_dispatched()
        assert sel._dispatched is True

    def test_selection_start_time_is_set(self):
        import time

        before = time.monotonic()
        replica = FakeReplica(
            ReplicaID(unique_id="r1", deployment_id=_make_deployment_id())
        )
        sel = ReplicaSelection(replica=replica)
        after = time.monotonic()
        assert before <= sel.selection_start_time <= after

    def test_release_slot_calls_callback(self):
        called = []
        replica = FakeReplica(
            ReplicaID(unique_id="r1", deployment_id=_make_deployment_id())
        )
        sel = ReplicaSelection(replica=replica)
        sel._release_callback = lambda: called.append(True)
        sel._release_slot()
        assert called == [True]

    def test_release_slot_noop_when_no_callback(self):
        replica = FakeReplica(
            ReplicaID(unique_id="r1", deployment_id=_make_deployment_id())
        )
        sel = ReplicaSelection(replica=replica)
        # Should not raise.
        sel._release_slot()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
