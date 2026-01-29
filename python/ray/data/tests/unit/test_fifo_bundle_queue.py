from typing import Any
from uuid import uuid4

import pandas as pd
import pytest

import ray
from ray.data._internal.execution.bundle_queue import FIFOBundleQueue
from ray.data._internal.execution.interfaces import RefBundle
from ray.data.block import BlockAccessor


def _create_bundle(data: Any) -> RefBundle:
    """Create a RefBundle with a single row with the given data using artificial refs."""
    block = pd.DataFrame({"data": [data]})
    # Create artificial object ref without calling ray.put()
    block_ref = ray.ObjectRef(uuid4().hex[:28].encode())
    metadata = BlockAccessor.for_block(block).get_metadata()
    schema = BlockAccessor.for_block(block).schema()
    return RefBundle([(block_ref, metadata)], owns_blocks=False, schema=schema)


def test_fifo_queue_add_and_length():
    """Test adding bundles and checking length."""
    queue = FIFOBundleQueue()
    bundle1 = _create_bundle("data1")
    bundle2 = _create_bundle("data11")

    queue.add(bundle1)
    assert len(queue) == 1

    queue.add(bundle2)
    assert len(queue) == 2


def test_fifo_queue_get_next_fifo_order():
    """Test that bundles are returned in FIFO order."""
    queue = FIFOBundleQueue()
    bundle1 = _create_bundle("data1")
    bundle2 = _create_bundle("data11")
    bundle3 = _create_bundle("data111")

    queue.add(bundle1)
    queue.add(bundle2)
    queue.add(bundle3)

    assert queue.get_next() is bundle1
    assert queue.get_next() is bundle2
    assert queue.get_next() is bundle3


def test_fifo_queue_init_with_bundles():
    """Test initializing queue with a list of bundles."""
    bundle1 = _create_bundle("data1")
    bundle2 = _create_bundle("data11")

    queue = FIFOBundleQueue(bundles=[bundle1, bundle2])

    assert len(queue) == 2
    assert queue.get_next() is bundle1
    assert queue.get_next() is bundle2


def test_fifo_queue_peek_next():
    """Test peeking at the next bundle without removing it."""
    queue = FIFOBundleQueue()
    bundle1 = _create_bundle("data1")
    bundle2 = _create_bundle("data11")

    queue.add(bundle1)
    queue.add(bundle2)

    # Peek should return bundle1 without removing
    assert queue.peek_next() is bundle1
    assert len(queue) == 2

    # Peek again should return the same bundle
    assert queue.peek_next() is bundle1


def test_fifo_queue_peek_next_empty():
    """Test peeking when queue is empty."""
    queue = FIFOBundleQueue()
    assert queue.peek_next() is None


def test_fifo_queue_has_next():
    """Test has_next correctly reflects queue state."""
    queue = FIFOBundleQueue()
    assert not queue.has_next()

    bundle1 = _create_bundle("data1")
    queue.add(bundle1)
    assert queue.has_next()

    queue.get_next()
    assert not queue.has_next()


def test_fifo_queue_get_next_empty_raises():
    """Test that get_next raises when queue is empty."""
    queue = FIFOBundleQueue()

    with pytest.raises(ValueError, match="Popping from empty"):
        queue.get_next()


def test_fifo_queue_clear():
    """Test clearing the queue resets everything."""
    queue = FIFOBundleQueue()
    bundle1 = _create_bundle("data1")
    bundle2 = _create_bundle("data11")

    queue.add(bundle1)
    queue.add(bundle2)

    queue.clear()

    assert len(queue) == 0
    assert queue.estimate_size_bytes() == 0
    assert queue.num_blocks() == 0
    assert not queue.has_next()


def test_fifo_queue_metrics():
    """Test that metrics are tracked correctly."""
    queue = FIFOBundleQueue()
    bundle1 = _create_bundle("data1")
    bundle2 = _create_bundle("data11")

    queue.add(bundle1)
    assert queue.estimate_size_bytes() == bundle1.size_bytes()
    assert queue.num_blocks() == 1

    queue.add(bundle2)
    assert queue.estimate_size_bytes() == bundle1.size_bytes() + bundle2.size_bytes()
    assert queue.num_blocks() == 2

    queue.get_next()
    assert queue.estimate_size_bytes() == bundle2.size_bytes()
    assert queue.num_blocks() == 1


def test_fifo_queue_iter():
    """Test iterating over the queue."""
    queue = FIFOBundleQueue()
    bundle1 = _create_bundle("data1")
    bundle2 = _create_bundle("data11")
    bundle3 = _create_bundle("data111")

    queue.add(bundle1)
    queue.add(bundle2)
    queue.add(bundle3)

    # Iterate without consuming
    bundles = list(queue)
    assert bundles == [bundle1, bundle2, bundle3]
    assert len(queue) == 3  # Queue unchanged


def test_fifo_queue_to_list():
    """Test converting queue to list."""
    queue = FIFOBundleQueue()
    bundle1 = _create_bundle("data1")
    bundle2 = _create_bundle("data11")

    queue.add(bundle1)
    queue.add(bundle2)

    bundles = queue.to_list()
    assert bundles == [bundle1, bundle2]
    assert len(queue) == 2  # Queue unchanged


def test_fifo_queue_finalize_is_noop():
    """Test that finalize does nothing (it's a no-op for FIFO queue)."""
    queue = FIFOBundleQueue()
    bundle1 = _create_bundle("data1")

    queue.add(bundle1)
    queue.finalize()  # Should not raise or change anything

    assert len(queue) == 1
    assert queue.get_next() is bundle1


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
