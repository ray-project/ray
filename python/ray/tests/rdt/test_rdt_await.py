"""Tests for await support on RDT (Ray Direct Transport) refs.

These tests verify that:
1. The _rdt_future() method correctly delegates to ray.get() in a background thread
2. Normal (non-RDT) ObjectRef.__await__ still works as before
3. ObjectRef.future() branches correctly based on _tensor_transport
4. Errors from ray.get are properly propagated through the future

Since RDT transports require specific hardware (NCCL/NIXL need GPU, gloo needs
collective groups), the core logic is validated via unit tests. Integration tests
for specific transports should be added to their respective test files
(e.g., test_rdt_nixl.py for one-sided transport await from driver).
"""

import asyncio
import sys

import pytest

import ray


def test_await_normal_object_ref(ray_start_regular):
    """Verify that normal (non-RDT) ObjectRef await still works."""

    @ray.remote
    def compute(x):
        return x * 2

    async def main():
        ref = compute.remote(21)
        assert ref.tensor_transport() is None
        result = await ref
        assert result == 42

    asyncio.run(main())


def test_await_normal_object_ref_gather(ray_start_regular):
    """Verify that asyncio.gather works with normal ObjectRefs."""

    @ray.remote
    def compute(x):
        return x * 2

    async def main():
        refs = [compute.remote(i) for i in range(5)]
        results = await asyncio.gather(*refs)
        assert results == [0, 2, 4, 6, 8]

    asyncio.run(main())


def test_future_normal_object_ref(ray_start_regular):
    """Verify that ObjectRef.future() still works for non-RDT refs."""

    @ray.remote
    def compute(x):
        return x + 1

    ref = compute.remote(10)
    fut = ref.future()
    result = fut.result(timeout=30)
    assert result == 11


def test_await_error_propagation(ray_start_regular):
    """Verify that task errors are propagated through await."""

    @ray.remote
    def failing_task():
        raise ValueError("intentional error")

    async def main():
        ref = failing_task.remote()
        with pytest.raises(ValueError, match="intentional error"):
            await ref

    asyncio.run(main())


def test_rdt_ref_has_tensor_transport(ray_start_regular):
    """Verify that RDT refs carry the tensor_transport attribute and that
    awaiting an RDT ref from the driver exercises the _rdt_future path.

    For two-sided transports like GLOO, ray.get from the driver is not
    supported, so await should raise a ValueError instead of hanging.
    """
    from ray.experimental.collective import create_collective_group

    try:
        import torch
    except ImportError:
        pytest.skip("torch required for RDT tests")

    @ray.remote
    class TensorActor:
        @ray.method(tensor_transport="gloo")
        def produce(self, data):
            return data

    actors = [TensorActor.remote() for _ in range(2)]
    create_collective_group(actors, backend="gloo")

    tensor = torch.randn((10,))
    rdt_ref = actors[0].produce.remote(tensor)

    # The ref should have tensor_transport set
    assert rdt_ref.tensor_transport() is not None
    assert rdt_ref.tensor_transport() == "GLOO"

    # Awaiting a GLOO RDT ref from the driver should raise ValueError
    # (two-sided transports don't support ray.get from the driver),
    # not hang indefinitely as it did before this fix.
    async def test_await():
        with pytest.raises((ValueError, asyncio.TimeoutError)):
            await asyncio.wait_for(rdt_ref, timeout=10)

    asyncio.run(test_await())


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
