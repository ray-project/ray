import sys
import time

import pytest

import ray
from ray import serve
from ray.llm._internal.serve.utils.broadcast import broadcast


# Define a simple deployment for testing
@serve.deployment(num_replicas=2)
class MockLLMDeployment:
    def __init__(self):
        self.reset_count = 0
        self.id = id(self)

    async def reset_prefix_cache(self):
        self.reset_count += 1
        return self.id, self.reset_count

    async def get_reset_count(self):
        return self.id, self.reset_count

    async def echo(self, msg, repeat=1):
        return f"{self.id}:{msg * repeat}"

    async def self_destruct(self):
        """Kill this replica's actor. Used for testing dead replica handling."""
        import os

        os._exit(1)


@pytest.fixture(scope="module")
def serve_instance():
    # Start ray and serve once for the module
    if not ray.is_initialized():
        ray.init()
    yield
    serve.shutdown()
    ray.shutdown()


@pytest.fixture
def mock_handle(serve_instance, request):
    # Ensure deployment is up and running.
    # serve.run waits for the deployment to be ready by default unless _blocking=False.
    app_name = f"mock-llm-{request.node.name}"
    route_prefix = f"/{app_name}"
    handle = serve.run(
        MockLLMDeployment.bind(), name=app_name, route_prefix=route_prefix
    )
    yield handle
    serve.delete(app_name, _blocking=True)


@pytest.mark.asyncio
async def test_dispatch_basic(mock_handle):
    """Test basic dispatch without combine."""
    # We can use get_reset_count which doesn't modify state
    results = broadcast(mock_handle, "get_reset_count")

    assert len(results) == 2
    # Verify we got unique IDs back
    ids = {r[0] for r in results}
    assert len(ids) == 2


@pytest.mark.asyncio
async def test_dispatch_with_combine(mock_handle):
    """Test dispatch with a combine function."""
    # First, increment count so we have something to sum
    broadcast(mock_handle, "reset_prefix_cache")

    def sum_counts(results):
        # results is list of (id, count)
        return sum(r[1] for r in results)

    # Get counts using dispatch and combine
    total_count = broadcast(mock_handle, "get_reset_count", combine=sum_counts)

    # We have 2 replicas, each should have reset_count=1 after one reset call
    assert total_count == 2
    assert isinstance(total_count, int)


@pytest.mark.asyncio
async def test_dispatch_args_kwargs(mock_handle):
    """Test dispatch passing args and kwargs."""
    results = broadcast(mock_handle, "echo", args=("hello",), kwargs={"repeat": 2})

    assert len(results) == 2
    for r in results:
        # Format is "id:msg"
        msg_part = r.split(":")[1]
        assert msg_part == "hellohello"


@pytest.mark.asyncio
async def test_dispatch_callable_args(mock_handle):
    """Test dispatch with callable args generator."""

    def arg_gen(replica):
        # replica has unique_id or similar
        return (f"msg-{replica.unique_id}",)

    results = broadcast(mock_handle, "echo", args=arg_gen)

    assert len(results) == 2
    msgs = set()
    for r in results:
        msg_part = r.split(":")[1]
        msgs.add(msg_part)

    assert len(msgs) == 2
    for msg in msgs:
        assert msg.startswith("msg-")


@pytest.mark.asyncio
async def test_dispatch_handles_dead_replica(serve_instance, request):
    """Test that dispatch gracefully handles a dead replica.

    This test verifies that if one replica dies, dispatch still completes
    successfully and returns results from the remaining live replicas.
    """
    app_name = f"mock-llm-{request.node.name}"
    route_prefix = f"/{app_name}"

    # Deploy with 2 replicas
    handle = serve.run(
        MockLLMDeployment.bind(), name=app_name, route_prefix=route_prefix
    )

    # First, verify dispatch works with all replicas alive
    results_before = broadcast(handle, "get_reset_count")
    assert len(results_before) == 2, "Should have 2 results from 2 replicas"

    # Kill one replica by calling self_destruct through the handle.
    # This sends an RPC to one replica which will kill itself.
    # We use options to not wait for response since the actor will die.
    try:
        handle.self_destruct.remote()
    except Exception:
        # The call may raise if the actor dies mid-request
        pass

    # Give Serve a moment to detect the dead replica
    time.sleep(2)

    # Dispatch should still work with the remaining replica(s)
    # The dead replica will be skipped (ValueError caught in dispatch)
    results_after = broadcast(handle, "get_reset_count")

    # Should get at least 1 result from the surviving replica
    # (The killed replica may or may not be in the replica set depending
    # on timing of Serve's failure detection)
    assert len(results_after) >= 1, "Should have at least 1 result from live replica"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
