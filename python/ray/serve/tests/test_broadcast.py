import os
import sys

import pytest

from ray import serve


def test_broadcast_basic(serve_instance):
    """Test that broadcast() calls every replica."""

    @serve.deployment(num_replicas=3)
    class D:
        def get_pid(self):
            return os.getpid()

    serve.run(D.bind())
    handle = serve.get_deployment_handle("D", "default")

    # broadcast() internally waits for replicas to be available.
    pids = handle.broadcast("get_pid").results(timeout_s=10)

    assert len(pids) == 3
    # Each replica should have a unique PID.
    assert len(set(pids)) == 3


def test_broadcast_with_args(serve_instance):
    """Test broadcast with positional and keyword arguments."""

    @serve.deployment(num_replicas=2)
    class D:
        def add(self, a, b=0):
            return a + b

    serve.run(D.bind())
    handle = serve.get_deployment_handle("D", "default")

    results = handle.broadcast("add", 1, b=2).results(timeout_s=10)

    assert len(results) == 2
    assert all(r == 3 for r in results)


def test_broadcast_stateful(serve_instance):
    """Test broadcast for state mutation (the cache-reset use case)."""

    @serve.deployment(num_replicas=2)
    class D:
        def __init__(self):
            self.cache = {"key": "value"}

        def reset_cache(self):
            self.cache.clear()
            return "cleared"

        def get_cache_size(self):
            return len(self.cache)

    serve.run(D.bind())
    handle = serve.get_deployment_handle("D", "default")

    # All replicas should start with cache size 1.
    sizes = handle.broadcast("get_cache_size").results(timeout_s=10)
    assert all(s == 1 for s in sizes)

    # Broadcast cache reset.
    results = handle.broadcast("reset_cache").results(timeout_s=10)
    assert all(r == "cleared" for r in results)

    # All replicas should now have empty caches.
    sizes = handle.broadcast("get_cache_size").results(timeout_s=10)
    assert all(s == 0 for s in sizes)


@pytest.mark.asyncio
async def test_broadcast_async(serve_instance):
    """Test the async results path."""

    @serve.deployment(num_replicas=2)
    class D:
        def get_pid(self):
            return os.getpid()

    serve.run(D.bind())
    handle = serve.get_deployment_handle("D", "default")

    pids = await handle.broadcast("get_pid").results_async()

    assert len(pids) == 2
    assert len(set(pids)) == 2


def test_broadcast_single_replica(serve_instance):
    """Test broadcast with a single replica."""

    @serve.deployment(num_replicas=1)
    class D:
        def ping(self):
            return "pong"

    serve.run(D.bind())
    handle = serve.get_deployment_handle("D", "default")

    results = handle.broadcast("ping").results(timeout_s=10)
    assert results == ["pong"]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
