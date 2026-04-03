import asyncio
import sys
from typing import List

import pytest

from ray.serve.experimental.capacity_queue import (
    CapacityQueue,
    CapacityQueueStats,
    ReplicaCapacityInfo,
)


def _get_raw_class():
    """Get the underlying class without the @ray.remote decorator."""
    return CapacityQueue.__ray_actor_class__


def _create_queue(acquire_timeout_s=30.0):
    """Create a CapacityQueue instance for local testing (no Ray)."""
    raw_class = _get_raw_class()
    return raw_class(acquire_timeout_s=acquire_timeout_s, _enable_long_poll=False)


class TestCapacityQueueStats:
    """Tests for CapacityQueueStats dataclass."""

    def test_stats_fields(self):
        stats = CapacityQueueStats(
            queue_size=10,
            num_waiters=2,
            num_replicas=3,
            total_capacity=15,
            total_in_flight=5,
            total_acquires=100,
            total_releases=95,
            total_timeouts=1,
            max_waiters_seen=5,
        )

        assert stats.queue_size == 10
        assert stats.num_waiters == 2
        assert stats.num_replicas == 3
        assert stats.total_capacity == 15
        assert stats.total_in_flight == 5
        assert stats.total_acquires == 100
        assert stats.total_releases == 95
        assert stats.total_timeouts == 1
        assert stats.max_waiters_seen == 5


class TestReplicaCapacityInfo:
    """Tests for ReplicaCapacityInfo dataclass."""

    def test_default_values(self):
        info = ReplicaCapacityInfo(replica_id="replica1", max_capacity=5)
        assert info.replica_id == "replica1"
        assert info.max_capacity == 5
        assert info.in_flight == 0


class TestCapacityQueueLocal:
    """Tests for CapacityQueue without Ray (local logic only)."""

    def test_initialization(self):
        queue = _create_queue(acquire_timeout_s=10.0)
        assert queue._acquire_timeout_s == 10.0
        assert queue.get_queue_length() == 0
        assert len(queue._waiters) == 0
        assert len(queue._replicas) == 0

    def test_register_replica(self):
        queue = _create_queue()
        queue.register_replica("replica1", 5)
        assert queue.get_queue_length() == 5
        assert "replica1" in queue._replicas
        assert queue._replicas["replica1"].max_capacity == 5
        assert queue._replicas["replica1"].in_flight == 0

    def test_register_replica_duplicate(self):
        queue = _create_queue()
        queue.register_replica("replica1", 5)
        queue.register_replica("replica1", 10)  # Should be ignored
        assert queue.get_queue_length() == 5
        assert queue._replicas["replica1"].max_capacity == 5

    def test_register_multiple_replicas(self):
        queue = _create_queue()
        queue.register_replica("replica1", 3)
        queue.register_replica("replica2", 5)
        queue.register_replica("replica3", 2)
        assert queue.get_queue_length() == 10
        assert len(queue._replicas) == 3

    def test_unregister_replica(self):
        queue = _create_queue()
        queue.register_replica("replica1", 5)
        queue.register_replica("replica2", 3)
        queue.unregister_replica("replica1")
        assert queue.get_queue_length() == 3
        assert "replica1" not in queue._replicas
        assert "replica2" in queue._replicas

    def test_unregister_nonexistent_replica(self):
        queue = _create_queue()
        queue.register_replica("replica1", 5)
        queue.unregister_replica("nonexistent")  # Should not raise
        assert queue.get_queue_length() == 5

    def test_release_to_unregistered_replica(self):
        queue = _create_queue()
        queue.register_replica("replica1", 5)
        initial_tokens = queue.get_queue_length()
        queue.release("nonexistent")
        assert queue.get_queue_length() == initial_tokens

    def test_get_stats(self):
        queue = _create_queue()
        queue.register_replica("replica1", 5)
        queue.register_replica("replica2", 3)
        stats = queue.get_stats()
        assert stats.queue_size == 8
        assert stats.num_replicas == 2
        assert stats.total_capacity == 8
        assert stats.total_in_flight == 0

    def test_get_registered_replicas(self):
        queue = _create_queue()
        queue.register_replica("replica1", 5)
        queue.register_replica("replica2", 3)
        replicas = queue.get_registered_replicas()
        assert set(replicas) == {"replica1", "replica2"}


class TestCapacityQueueAsync:
    """Async tests for CapacityQueue."""

    @pytest.mark.asyncio
    async def test_acquire_immediate(self):
        queue = _create_queue()
        queue.register_replica("replica1", 5)
        replica_id = await queue.acquire()
        assert replica_id == "replica1"
        assert queue._replicas["replica1"].in_flight == 1
        assert queue.get_queue_length() == 4

    @pytest.mark.asyncio
    async def test_acquire_and_release(self):
        queue = _create_queue()
        queue.register_replica("replica1", 2)

        r1 = await queue.acquire()
        r2 = await queue.acquire()
        assert queue.get_queue_length() == 0
        assert queue._replicas["replica1"].in_flight == 2

        queue.release(r1)
        assert queue.get_queue_length() == 1
        assert queue._replicas["replica1"].in_flight == 1

        queue.release(r2)
        assert queue.get_queue_length() == 2
        assert queue._replicas["replica1"].in_flight == 0

    @pytest.mark.asyncio
    async def test_acquire_timeout(self):
        queue = _create_queue(acquire_timeout_s=0.1)
        # No replicas registered
        replica_id = await queue.acquire()
        assert replica_id is None
        assert queue._total_timeouts == 1

    @pytest.mark.asyncio
    async def test_acquire_timeout_custom(self):
        queue = _create_queue(acquire_timeout_s=10.0)
        replica_id = await queue.acquire(timeout_s=0.1)
        assert replica_id is None

    @pytest.mark.asyncio
    async def test_acquire_waits_for_release(self):
        queue = _create_queue()
        queue.register_replica("replica1", 1)

        await queue.acquire()
        assert queue.get_queue_length() == 0

        acquire_task = asyncio.create_task(queue.acquire(timeout_s=5.0))
        await asyncio.sleep(0.01)
        assert queue.get_num_waiters() == 1

        queue.release("replica1")
        replica_id = await acquire_task
        assert replica_id == "replica1"
        assert queue.get_num_waiters() == 0

    @pytest.mark.asyncio
    async def test_acquire_waits_for_registration(self):
        queue = _create_queue()

        acquire_task = asyncio.create_task(queue.acquire(timeout_s=5.0))
        await asyncio.sleep(0.01)
        assert queue.get_num_waiters() == 1

        queue.register_replica("replica1", 3)
        replica_id = await acquire_task
        assert replica_id == "replica1"
        assert queue.get_num_waiters() == 0

    @pytest.mark.asyncio
    async def test_acquire_cancellation(self):
        queue = _create_queue()

        acquire_task = asyncio.create_task(queue.acquire(timeout_s=5.0))
        await asyncio.sleep(0.01)
        assert queue.get_num_waiters() == 1

        acquire_task.cancel()
        try:
            await acquire_task
        except asyncio.CancelledError:
            pass

        await asyncio.sleep(0.01)
        assert queue.get_num_waiters() == 0

    @pytest.mark.asyncio
    async def test_least_loaded_distribution(self):
        queue = _create_queue()
        queue.register_replica("replica1", 3)
        queue.register_replica("replica2", 3)

        # Acquire 2 tokens - should distribute evenly
        await queue.acquire()
        await queue.acquire()

        assert queue._replicas["replica1"].in_flight == 1
        assert queue._replicas["replica2"].in_flight == 1

        # Acquire 2 more - again evenly
        await queue.acquire()
        await queue.acquire()

        assert queue._replicas["replica1"].in_flight == 2
        assert queue._replicas["replica2"].in_flight == 2

    @pytest.mark.asyncio
    async def test_max_waiters_tracking(self):
        queue = _create_queue()

        tasks = [asyncio.create_task(queue.acquire(timeout_s=0.2)) for _ in range(5)]
        await asyncio.sleep(0.05)
        await asyncio.gather(*tasks)

        stats = queue.get_stats()
        assert stats.max_waiters_seen >= 5


class TestCapacityQueueStatsTracking:
    """Tests for statistics tracking during operations."""

    @pytest.mark.asyncio
    async def test_total_acquires_increments(self):
        queue = _create_queue(acquire_timeout_s=0.1)
        queue.register_replica("replica1", 2)

        assert queue._total_acquires == 0

        await queue.acquire()
        assert queue._total_acquires == 1

        await queue.acquire()
        assert queue._total_acquires == 2

        # Timeout also counts
        await queue.acquire()
        assert queue._total_acquires == 3

    @pytest.mark.asyncio
    async def test_total_releases_increments(self):
        queue = _create_queue()
        queue.register_replica("replica1", 3)

        assert queue._total_releases == 0

        r1 = await queue.acquire()
        r2 = await queue.acquire()

        queue.release(r1)
        assert queue._total_releases == 1

        queue.release(r2)
        assert queue._total_releases == 2

    @pytest.mark.asyncio
    async def test_in_flight_tracking(self):
        queue = _create_queue()
        queue.register_replica("replica1", 3)

        assert queue._replicas["replica1"].in_flight == 0

        r1 = await queue.acquire()
        assert queue._replicas["replica1"].in_flight == 1

        r2 = await queue.acquire()
        assert queue._replicas["replica1"].in_flight == 2

        queue.release(r1)
        assert queue._replicas["replica1"].in_flight == 1

        queue.release(r2)
        assert queue._replicas["replica1"].in_flight == 0

    @pytest.mark.asyncio
    async def test_stats_reflect_operations(self):
        queue = _create_queue(acquire_timeout_s=0.1)
        queue.register_replica("replica1", 2)
        queue.register_replica("replica2", 1)

        r1 = await queue.acquire()
        await queue.acquire()

        stats = queue.get_stats()
        assert stats.total_capacity == 3
        assert stats.total_in_flight == 2
        assert stats.queue_size == 1
        assert stats.total_acquires == 2

        queue.release(r1)

        stats = queue.get_stats()
        assert stats.total_in_flight == 1
        assert stats.queue_size == 2
        assert stats.total_releases == 1


class TestCapacityQueueEdgeCases:
    """Tests for edge cases and error handling."""

    def test_register_zero_capacity(self):
        queue = _create_queue()
        queue.register_replica("replica1", 0)
        assert "replica1" in queue._replicas
        assert queue._replicas["replica1"].max_capacity == 0
        assert queue.get_queue_length() == 0

    @pytest.mark.asyncio
    async def test_unregister_while_in_flight(self):
        queue = _create_queue()
        queue.register_replica("replica1", 3)

        r1 = await queue.acquire()
        assert queue._replicas["replica1"].in_flight == 1

        queue.unregister_replica("replica1")
        assert "replica1" not in queue._replicas
        assert queue.get_queue_length() == 0

        # Release should be handled gracefully (discarded)
        queue.release(r1)
        assert queue.get_queue_length() == 0

    @pytest.mark.asyncio
    async def test_release_same_token_twice(self):
        """Releasing the same token twice doesn't inflate capacity."""
        queue = _create_queue()
        queue.register_replica("replica1", 1)

        r1 = await queue.acquire()
        assert queue.get_queue_length() == 0

        queue.release(r1)
        assert queue.get_queue_length() == 1

        # Release again - in_flight clamped to 0
        queue.release(r1)
        assert queue.get_queue_length() == 1  # Correctly doesn't inflate

    @pytest.mark.asyncio
    async def test_fifo_waiter_ordering(self):
        """Waiters are fulfilled in FIFO order."""
        queue = _create_queue()

        completion_order: List[int] = []

        async def waiter(waiter_id: int):
            await queue.acquire(timeout_s=5.0)
            completion_order.append(waiter_id)

        tasks = [asyncio.create_task(waiter(i)) for i in range(3)]
        await asyncio.sleep(0.05)
        assert queue.get_num_waiters() == 3

        queue.register_replica("replica1", 3)
        await asyncio.gather(*tasks)

        assert completion_order == [0, 1, 2]

    @pytest.mark.asyncio
    async def test_multiple_replicas_least_loaded(self):
        """Least-loaded replica is selected."""
        queue = _create_queue()
        queue.register_replica("replica1", 2)
        queue.register_replica("replica2", 2)

        r1 = await queue.acquire()
        r2 = await queue.acquire()

        # Should be different replicas (load balanced)
        assert r1 != r2

        # Both should have 1 in-flight
        assert queue._replicas["replica1"].in_flight == 1
        assert queue._replicas["replica2"].in_flight == 1

        # Acquire two more
        await queue.acquire()
        await queue.acquire()

        assert queue._replicas["replica1"].in_flight == 2
        assert queue._replicas["replica2"].in_flight == 2

        # Clean up
        queue.release(r1)
        queue.release(r2)

    @pytest.mark.asyncio
    async def test_waiter_timeout_cleanup(self):
        """Timed-out waiters are properly cleaned up."""
        queue = _create_queue(acquire_timeout_s=0.1)

        tasks = [asyncio.create_task(queue.acquire()) for _ in range(3)]
        results = await asyncio.gather(*tasks)

        assert all(r is None for r in results)
        assert queue.get_num_waiters() == 0
        assert queue._total_timeouts == 3


class TestReplicaLifecycleOnDeploymentTargetUpdate:
    """Tests that the queue reflects replica changes from the controller.

    The controller sends deployment target updates (via long poll) when replicas
    are added or removed.  The queue must unregister removed replicas and
    register new ones so that tokens are only issued for live replicas.
    """

    @staticmethod
    def _make_target_info(replica_specs):
        """Build a DeploymentTargetInfo from a list of (unique_id, capacity) tuples."""
        from ray.serve._private.common import (
            DeploymentID,
            DeploymentTargetInfo,
            ReplicaID,
            RunningReplicaInfo,
        )

        dep_id = DeploymentID(name="test_deployment", app_name="test_app")
        replicas = []
        for uid, cap in replica_specs:
            replicas.append(
                RunningReplicaInfo(
                    replica_id=ReplicaID(unique_id=uid, deployment_id=dep_id),
                    node_id="node1",
                    node_ip="127.0.0.1",
                    availability_zone=None,
                    actor_name=f"actor_{uid}",
                    max_ongoing_requests=cap,
                    is_cross_language=False,
                )
            )
        return DeploymentTargetInfo(is_available=True, running_replicas=replicas)

    def test_update_adds_new_replicas(self):
        """New replicas in the update are registered in the queue."""
        queue = _create_queue()
        target = self._make_target_info([("r1", 5)])

        queue._update_deployment_targets(target)

        assert set(queue.get_registered_replicas()) == {"r1"}
        assert queue.get_queue_length() == 5

    def test_update_removes_old_replicas(self):
        """Replicas absent from the update are unregistered."""
        queue = _create_queue()
        queue.register_replica("r1", 3)
        queue.register_replica("r2", 2)
        assert len(queue.get_registered_replicas()) == 2

        # Update with only r1 — r2 should be removed
        target = self._make_target_info([("r1", 3)])
        queue._update_deployment_targets(target)

        assert set(queue.get_registered_replicas()) == {"r1"}
        assert queue.get_queue_length() == 3

    def test_update_empty_removes_all(self):
        """An update with no replicas clears the queue."""
        queue = _create_queue()
        queue.register_replica("r1", 3)

        target = self._make_target_info([])
        queue._update_deployment_targets(target)

        assert queue.get_registered_replicas() == []
        assert queue.get_queue_length() == 0

    def test_update_is_idempotent(self):
        """Calling update twice with the same replicas is a no-op."""
        queue = _create_queue()
        target = self._make_target_info([("r1", 5)])

        queue._update_deployment_targets(target)
        queue._update_deployment_targets(target)

        assert set(queue.get_registered_replicas()) == {"r1"}
        assert queue.get_queue_length() == 5

    @pytest.mark.asyncio
    async def test_scale_down_frees_dead_capacity(self):
        """After scale-down, tokens for removed replicas are no longer issued."""
        queue = _create_queue()
        queue.register_replica("r1", 1)
        queue.register_replica("r2", 1)

        # Saturate both replicas
        t1 = await queue.acquire()
        t2 = await queue.acquire()
        assert queue.get_queue_length() == 0

        # Scale down: remove r2
        target = self._make_target_info([("r1", 1)])
        queue._update_deployment_targets(target)

        # Release both tokens — only r1's should restore capacity
        queue.release(t1)
        queue.release(t2)  # r2 is gone, release is discarded

        assert queue.get_queue_length() == 1
        assert set(queue.get_registered_replicas()) == {"r1"}

        # Next acquire must go to r1 (no phantom r2)
        token = await queue.acquire(timeout_s=1.0)
        assert token == "r1"


class TestTokenTTL:
    """Tests for the token TTL auto-reclaim feature."""

    @pytest.mark.asyncio
    async def test_expired_token_reclaimed(self):
        """Tokens exceeding the TTL are automatically reclaimed."""
        queue = _get_raw_class()(
            acquire_timeout_s=30.0,
            token_ttl_s=0.2,
            _enable_long_poll=False,
        )
        queue.register_replica("replica1", 2)

        # Acquire a token (never release it).
        r = await queue.acquire()
        assert r == "replica1"
        assert queue._replicas["replica1"].in_flight == 1
        assert queue.get_queue_length() == 1

        # Wait for the TTL reaper to reclaim it.
        await asyncio.sleep(0.5)

        assert queue._replicas["replica1"].in_flight == 0
        assert queue.get_queue_length() == 2
        assert queue._total_ttl_reclaims == 1

    @pytest.mark.asyncio
    async def test_ttl_does_not_reclaim_released_tokens(self):
        """Properly released tokens are not double-reclaimed by the reaper."""
        queue = _get_raw_class()(
            acquire_timeout_s=30.0,
            token_ttl_s=0.2,
            _enable_long_poll=False,
        )
        queue.register_replica("replica1", 2)

        r = await queue.acquire()
        queue.release(r)

        assert queue._replicas["replica1"].in_flight == 0

        # Wait past TTL — reaper should find nothing to reclaim.
        await asyncio.sleep(0.5)

        assert queue._replicas["replica1"].in_flight == 0
        assert queue.get_queue_length() == 2
        assert queue._total_ttl_reclaims == 0

    @pytest.mark.asyncio
    async def test_ttl_reclaim_wakes_waiters(self):
        """Reclaimed capacity from TTL should wake blocked waiters."""
        queue = _get_raw_class()(
            acquire_timeout_s=30.0,
            token_ttl_s=0.2,
            _enable_long_poll=False,
        )
        queue.register_replica("replica1", 1)

        # Exhaust capacity.
        await queue.acquire()
        assert queue.get_queue_length() == 0

        # This waiter should be woken once the TTL reaper reclaims.
        result = await asyncio.wait_for(queue.acquire(), timeout=2.0)
        assert result == "replica1"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
