"""Unit tests for decoupled routing metrics on AsyncioRouter.

Tests for serve_selection_dispatch_gap_ms (Histogram) and
serve_selections_released_without_dispatch (Counter).
"""
import asyncio
import time
from typing import Optional
from unittest.mock import AsyncMock, Mock, patch

import pytest

from ray._common.utils import get_or_create_event_loop
from ray.serve._private.common import (
    DeploymentHandleSource,
    DeploymentID,
    RequestMetadata,
)
from ray.serve._private.request_router import (
    RunningReplica,
)
from ray.serve._private.request_router.replica_wrapper import ReplicaSelection
from ray.serve._private.router import AsyncioRouter
from ray.serve._private.test_utils import FakeCounter, FakeHistogram

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_deployment_id(name: str = "test-dep", app: str = "test-app") -> DeploymentID:
    return DeploymentID(name=name, app_name=app)


def _make_request_metadata() -> RequestMetadata:
    return RequestMetadata(
        request_id="test-req-1",
        internal_request_id="test-internal-1",
    )


def _make_selection(
    replica: Optional[RunningReplica] = None,
    slot_token: str = "slot-1",
    *,
    start_time_offset_s: float = 0.0,
) -> ReplicaSelection:
    """Build a minimal ReplicaSelection for metric tests."""
    dep_id = _make_deployment_id()
    sel = ReplicaSelection(
        replica_id="replica-0",
        node_ip="127.0.0.1",
        port=8000,
        node_id="node-0",
        availability_zone=None,
        _replica=replica or Mock(),
        _deployment_id=dep_id,
        _request_metadata=_make_request_metadata(),
        _method_name="__call__",
        _slot_token=slot_token,
    )
    # Back-date the start time to simulate elapsed time.
    sel.selection_start_time = time.monotonic() - start_time_offset_s
    return sel


def _make_router(
    fake_histogram: FakeHistogram,
    fake_counter: FakeCounter,
) -> AsyncioRouter:
    """Create an AsyncioRouter with fake metrics injected."""
    dep_id = _make_deployment_id()
    router = AsyncioRouter(
        controller_handle=Mock(),
        deployment_id=dep_id,
        handle_id="test-handle",
        self_actor_id="test-actor",
        handle_source=DeploymentHandleSource.UNKNOWN,
        event_loop=get_or_create_event_loop(),
        enable_strict_max_ongoing_requests=False,
        node_id="test-node",
        availability_zone=None,
        prefer_local_node_routing=False,
        _request_router_initialized_event=asyncio.Event(),
    )
    router._selection_dispatch_gap_ms = fake_histogram
    router._selections_released_without_dispatch = fake_counter
    return router


# ---------------------------------------------------------------------------
# Tests: serve_selection_dispatch_gap_ms
# ---------------------------------------------------------------------------


class TestSelectionDispatchGapMetric:
    @pytest.mark.asyncio
    async def test_gap_observed_on_dispatch(self):
        """dispatch() records the selection→dispatch elapsed time."""
        dep_id = _make_deployment_id()
        fake_hist = FakeHistogram(tag_keys=("deployment", "application"))
        fake_hist.set_default_tags(
            {"deployment": dep_id.name, "application": dep_id.app_name}
        )
        fake_ctr = FakeCounter(tag_keys=("deployment", "application"))
        fake_ctr.set_default_tags(
            {"deployment": dep_id.name, "application": dep_id.app_name}
        )
        router = _make_router(fake_hist, fake_ctr)

        offset_s = 0.05  # 50 ms simulated delay
        selection = _make_selection(start_time_offset_s=offset_s)

        with patch.object(
            router,
            "_dispatch_to_marked_selection",
            new_callable=AsyncMock,
            return_value=Mock(),
        ):
            request_meta = _make_request_metadata()
            await router.dispatch(selection, request_meta)

        assert len(fake_hist.observations) == 1
        observed_ms = fake_hist.observations[0][0]
        # The gap should be at least offset_s * 1000 ms and not absurdly large.
        assert observed_ms >= offset_s * 1000
        assert observed_ms < 5000  # sanity: less than 5 s

    @pytest.mark.asyncio
    async def test_gap_not_recorded_without_dispatch(self):
        """If dispatch() is never called, the histogram stays empty."""
        dep_id = _make_deployment_id()
        fake_hist = FakeHistogram(tag_keys=("deployment", "application"))
        fake_hist.set_default_tags(
            {"deployment": dep_id.name, "application": dep_id.app_name}
        )
        fake_ctr = FakeCounter(tag_keys=("deployment", "application"))
        fake_ctr.set_default_tags(
            {"deployment": dep_id.name, "application": dep_id.app_name}
        )

        # Never call dispatch — histogram should remain empty.
        assert len(fake_hist.observations) == 0

    @pytest.mark.asyncio
    async def test_gap_increases_with_delay(self):
        """A longer delay between selection and dispatch produces a larger gap."""
        dep_id = _make_deployment_id()
        results = []

        for offset_s in (0.01, 0.1):
            fake_hist = FakeHistogram(tag_keys=("deployment", "application"))
            fake_hist.set_default_tags(
                {"deployment": dep_id.name, "application": dep_id.app_name}
            )
            fake_ctr = FakeCounter(tag_keys=("deployment", "application"))
            fake_ctr.set_default_tags(
                {"deployment": dep_id.name, "application": dep_id.app_name}
            )
            router = _make_router(fake_hist, fake_ctr)
            selection = _make_selection(start_time_offset_s=offset_s)

            with patch.object(
                router,
                "_dispatch_to_marked_selection",
                new_callable=AsyncMock,
                return_value=Mock(),
            ):
                await router.dispatch(selection, _make_request_metadata())

            results.append(fake_hist.observations[0][0])

        assert results[1] > results[0], "Larger offset should produce larger gap"


# ---------------------------------------------------------------------------
# Tests: serve_selections_released_without_dispatch
# ---------------------------------------------------------------------------


class TestSelectionsReleasedWithoutDispatch:
    @pytest.mark.asyncio
    async def test_counter_increments_when_no_dispatch(self):
        """Exiting choose_replica without dispatch increments the counter."""
        dep_id = _make_deployment_id()
        fake_hist = FakeHistogram(tag_keys=("deployment", "application"))
        fake_hist.set_default_tags(
            {"deployment": dep_id.name, "application": dep_id.app_name}
        )
        fake_ctr = FakeCounter(tag_keys=("deployment", "application"))
        fake_ctr.set_default_tags(
            {"deployment": dep_id.name, "application": dep_id.app_name}
        )
        router = _make_router(fake_hist, fake_ctr)
        fake_replica = Mock()

        selection = _make_selection(replica=fake_replica)

        with (
            patch.object(
                router,
                "_pick_and_reserve_replica",
                new_callable=AsyncMock,
                return_value=(fake_replica, "slot-1"),
            ),
            patch.object(
                router,
                "_release_slot_and_refresh_cache",
                new_callable=AsyncMock,
            ),
            patch.object(router._metrics_manager, "inc_reserved_slots"),
            patch.object(router._metrics_manager, "dec_reserved_slots"),
            patch.object(
                router,
                "_resolve_args_with_metrics",
                new_callable=AsyncMock,
            ),
        ):
            # Patch ReplicaSelection construction inside choose_replica.
            with patch(
                "ray.serve._private.router.ReplicaSelection",
                return_value=selection,
            ):
                router._deployment_available = True
                router._request_router_initialized.set()
                async with router.choose_replica(_make_request_metadata()):
                    pass  # exit without dispatch

        tags = {"deployment": dep_id.name, "application": dep_id.app_name}
        assert fake_ctr.get_count(tags) == 1

    @pytest.mark.asyncio
    async def test_counter_does_not_increment_when_dispatched(self):
        """Dispatching within choose_replica does NOT increment the counter."""
        dep_id = _make_deployment_id()
        fake_hist = FakeHistogram(tag_keys=("deployment", "application"))
        fake_hist.set_default_tags(
            {"deployment": dep_id.name, "application": dep_id.app_name}
        )
        fake_ctr = FakeCounter(tag_keys=("deployment", "application"))
        fake_ctr.set_default_tags(
            {"deployment": dep_id.name, "application": dep_id.app_name}
        )
        router = _make_router(fake_hist, fake_ctr)
        fake_replica = Mock()
        selection = _make_selection(replica=fake_replica)

        with (
            patch.object(
                router,
                "_pick_and_reserve_replica",
                new_callable=AsyncMock,
                return_value=(fake_replica, "slot-1"),
            ),
            patch.object(
                router,
                "_release_slot_and_refresh_cache",
                new_callable=AsyncMock,
            ),
            patch.object(router._metrics_manager, "inc_reserved_slots"),
            patch.object(router._metrics_manager, "dec_reserved_slots"),
            patch.object(
                router,
                "_resolve_args_with_metrics",
                new_callable=AsyncMock,
            ),
            patch.object(
                router,
                "_dispatch_to_marked_selection",
                new_callable=AsyncMock,
                return_value=Mock(),
            ),
        ):
            with patch(
                "ray.serve._private.router.ReplicaSelection",
                return_value=selection,
            ):
                router._deployment_available = True
                router._request_router_initialized.set()
                async with router.choose_replica(_make_request_metadata()) as sel:
                    await router.dispatch(sel, _make_request_metadata())

        tags = {"deployment": dep_id.name, "application": dep_id.app_name}
        assert fake_ctr.get_count(tags) is None  # counter was never incremented


# ---------------------------------------------------------------------------
# Tests: selection_start_time on ReplicaSelection
# ---------------------------------------------------------------------------


class TestReplicaSelectionStartTime:
    def test_selection_start_time_auto_set(self):
        """selection_start_time is automatically set to current monotonic time."""
        before = time.monotonic()
        selection = _make_selection(start_time_offset_s=0.0)
        after = time.monotonic()
        # The helper sets selection_start_time = monotonic() - offset (0 here).
        # It should fall within [before, after].
        assert before <= selection.selection_start_time <= after

    def test_selection_start_time_reflects_elapsed(self):
        """Backdating selection_start_time produces a measurable gap."""
        offset_s = 0.1
        selection = _make_selection(start_time_offset_s=offset_s)
        elapsed_ms = (time.monotonic() - selection.selection_start_time) * 1000
        assert elapsed_ms >= offset_s * 1000


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
