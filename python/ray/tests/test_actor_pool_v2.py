"""Integration tests for Actor Pool V2 API (ray.experimental.actor_pool).

Covers pool lifecycle, task submission, load balancing, scaling,
shutdown, and cross-actor retry (task retried on a different actor
when the original actor dies).

This tests the NEW ray.experimental.actor_pool.ActorPool API,
not the existing ray.util.ActorPool (tested in test_actor_pool.py).
"""

import pytest

import ray
from ray.experimental.actor_pool import ActorPool, RetryPolicy


@ray.remote
class SimpleWorker:
    """Simple worker for testing."""

    def __init__(self, name="worker"):
        self.name = name
        self.call_count = 0

    def process(self, x):
        self.call_count += 1
        return x * 2

    def get_name(self):
        return self.name

    def get_call_count(self):
        return self.call_count

    def echo(self, msg):
        return f"{self.name}: {msg}"


@ray.remote
class CrashableWorker:
    """Worker that crashes via os._exit(1) when signaled.

    Used to test cross-actor retry: when this actor dies mid-task, the pool
    retries the task on a different healthy actor.
    """

    def __init__(self, name="worker"):
        self.name = name
        self.call_count = 0
        self._crash_on_next = False

    def set_crash_on_next(self):
        self._crash_on_next = True

    def process(self, x):
        self.call_count += 1
        if self._crash_on_next:
            import os

            os._exit(1)
        return x * 2

    def get_call_count(self):
        return self.call_count


@pytest.fixture
def ray_start():
    """Start Ray for tests."""
    ray.init(num_cpus=4, ignore_reinit_error=True)
    yield
    ray.shutdown()


class TestActorPoolV2Creation:
    """Tests for pool creation and lifecycle."""

    def test_create_pool_with_size(self, ray_start):
        """Create a pool with specified size."""
        pool = ActorPool(SimpleWorker, size=2)

        assert len(pool.actors) == 2
        assert pool._pool_id is not None

        pool.shutdown()

    def test_create_pool_with_actor_args(self, ray_start):
        """Create a pool with actor constructor arguments."""
        pool = ActorPool(
            SimpleWorker,
            size=2,
            actor_args=("custom_name",),
        )

        # Verify actors were created with args
        names = ray.get([a.get_name.remote() for a in pool.actors])
        assert all(name == "custom_name" for name in names)

        pool.shutdown()

    def test_create_pool_with_retry_policy(self, ray_start):
        """Create a pool with custom retry policy."""
        retry = RetryPolicy(
            max_attempts=5,
            backoff_ms=500,
            backoff_multiplier=1.5,
            max_backoff_ms=10000,
        )
        pool = ActorPool(SimpleWorker, size=1, retry=retry)

        assert len(pool.actors) == 1
        pool.shutdown()


class TestActorPoolV2Submit:
    """Tests for task submission."""

    def test_submit_single_task(self, ray_start):
        """Submit a single task to the pool."""
        pool = ActorPool(SimpleWorker, size=2)

        ref = pool.submit("process", 5)
        result = ray.get(ref)

        assert result == 10
        pool.shutdown()

    def test_submit_multiple_tasks(self, ray_start):
        """Submit multiple tasks to the pool."""
        pool = ActorPool(SimpleWorker, size=2)

        refs = [pool.submit("process", i) for i in range(10)]
        results = ray.get(refs)

        assert results == [i * 2 for i in range(10)]
        pool.shutdown()

    def test_map_over_items(self, ray_start):
        """Use map() to process multiple items."""
        pool = ActorPool(SimpleWorker, size=2)

        items = list(range(10))
        refs = pool.map("process", items)
        results = ray.get(refs)

        assert results == [i * 2 for i in range(10)]
        pool.shutdown()

    def test_submit_with_string_args(self, ray_start):
        """Submit tasks with string arguments."""
        pool = ActorPool(SimpleWorker, size=2)

        refs = [pool.submit("echo", f"msg_{i}") for i in range(5)]
        results = ray.get(refs)

        # Results should come from one of the workers
        for i, result in enumerate(results):
            assert f"msg_{i}" in result
            assert "worker" in result

        pool.shutdown()


class TestActorPoolV2Stats:
    """Tests for pool statistics."""

    def test_stats_after_creation(self, ray_start):
        """Check stats immediately after pool creation."""
        pool = ActorPool(SimpleWorker, size=3)

        stats = pool.stats()
        assert stats["num_actors"] == 3
        assert stats["total_tasks_submitted"] == 0

        pool.shutdown()

    def test_stats_after_submission(self, ray_start):
        """Check stats after submitting tasks."""
        pool = ActorPool(SimpleWorker, size=2)

        refs = [pool.submit("process", i) for i in range(5)]
        ray.get(refs)  # Wait for completion

        stats = pool.stats()
        assert stats["num_actors"] == 2
        assert stats["total_tasks_submitted"] == 5

        pool.shutdown()


class TestActorPoolV2Scale:
    """Tests for scaling the pool."""

    def test_scale_up(self, ray_start):
        """Scale up the pool by adding actors."""
        pool = ActorPool(SimpleWorker, size=1)
        assert len(pool.actors) == 1

        pool.scale(2)  # Add 2 more actors
        assert len(pool.actors) == 3

        stats = pool.stats()
        assert stats["num_actors"] == 3

        pool.shutdown()

    def test_scale_down(self, ray_start):
        """Scale down the pool by removing actors."""
        pool = ActorPool(SimpleWorker, size=3)
        assert len(pool.actors) == 3

        pool.scale(-1)  # Remove 1 actor
        assert len(pool.actors) == 2

        stats = pool.stats()
        assert stats["num_actors"] == 2

        pool.shutdown()


class TestActorPoolV2Shutdown:
    """Tests for pool shutdown."""

    def test_shutdown_graceful(self, ray_start):
        """Gracefully shutdown the pool."""
        pool = ActorPool(SimpleWorker, size=2)

        # Submit some work first
        refs = [pool.submit("process", i) for i in range(3)]
        ray.get(refs)

        # Graceful shutdown
        pool.shutdown(force=False)

        # Pool should be empty after shutdown
        assert len(pool.actors) == 0

    def test_shutdown_force(self, ray_start):
        """Force shutdown the pool."""
        pool = ActorPool(SimpleWorker, size=2)

        # Force shutdown
        pool.shutdown(force=True)

        assert len(pool.actors) == 0


class TestActorPoolV2LoadBalancing:
    """Tests for load balancing across actors."""

    def test_tasks_distributed_across_actors(self, ray_start):
        """Verify tasks are distributed across multiple actors."""
        pool = ActorPool(SimpleWorker, size=2)

        # Submit enough tasks that both actors should be used
        refs = [pool.submit("process", i) for i in range(20)]
        ray.get(refs)

        # Check that both actors received some work
        call_counts = ray.get([a.get_call_count.remote() for a in pool.actors])

        # Both actors should have been used (not all work on one actor)
        assert all(count > 0 for count in call_counts)

        pool.shutdown()


class TestActorPoolV2CrossActorRetry:
    """Tests for cross-actor retry when a pool actor dies."""

    def test_retry_on_actor_death(self, ray_start):
        """Crash an actor mid-task, verify the task completes on a survivor."""
        pool = ActorPool(CrashableWorker, size=3, retry=RetryPolicy(max_attempts=3))

        ray.get(pool.actors[0].set_crash_on_next.remote())

        # Each actor gets one task. Actor 0 crashes; pool retries on a survivor.
        refs = [pool.submit("process", i) for i in range(3)]

        results = ray.get(refs, timeout=10)
        assert sorted(results) == [0, 2, 4]

        pool.shutdown()

    def test_retry_multiple_tasks_on_actor_death(self, ray_start):
        """Crash an actor, verify all 10 tasks complete."""
        pool = ActorPool(CrashableWorker, size=3, retry=RetryPolicy(max_attempts=3))

        ray.get(pool.actors[0].set_crash_on_next.remote())

        refs = [pool.submit("process", i) for i in range(10)]

        results = ray.get(refs, timeout=30)
        assert sorted(results) == sorted([i * 2 for i in range(10)])

        pool.shutdown()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-v"]))
