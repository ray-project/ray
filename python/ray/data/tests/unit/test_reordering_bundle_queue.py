from typing import Any
from uuid import uuid4

import pandas as pd
import pytest

import ray
from ray.data._internal.execution.bundle_queue import ReorderingBundleQueue
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


def test_ordered_queue_add_and_get_in_order():
    """Test adding and getting bundles in sequential order."""
    queue = ReorderingBundleQueue()
    bundle0 = _create_bundle("data1")
    bundle1 = _create_bundle("data11")

    queue.add(bundle0, key=0)
    queue.add(bundle1, key=1)

    assert len(queue) == 2
    assert queue.has_next()

    # Can only get from key 0 until it's finalized
    assert queue.get_next() is bundle0
    # Nothing else to dequeue for 1
    assert not queue.has_next()
    queue.finalize(key=0)

    # Now can get from key 1
    assert queue.get_next() is bundle1
    assert not queue.has_next()
    queue.finalize(key=1)

    assert len(queue) == 0
    assert not queue.has_next()


def test_ordered_queue_add_out_of_order():
    """Test that bundles added out of order are returned in key order."""
    queue = ReorderingBundleQueue()
    bundle0 = _create_bundle("data1")
    bundle1 = _create_bundle("data11")
    bundle2 = _create_bundle("data111")

    # Add in reverse order
    queue.add(bundle2, key=2)
    queue.add(bundle0, key=0)
    queue.add(bundle1, key=1)

    assert len(queue) == 3

    # Should still get in key order
    assert queue.get_next() is bundle0
    queue.finalize(key=0)

    assert queue.get_next() is bundle1
    queue.finalize(key=1)

    assert queue.get_next() is bundle2
    queue.finalize(key=2)


def test_ordered_queue_multiple_bundles_per_key():
    """Test adding multiple bundles for the same key."""
    queue = ReorderingBundleQueue()
    bundle1a = _create_bundle("data1a")
    bundle1b = _create_bundle("data1b")
    bundle2 = _create_bundle("data2")

    queue.add(bundle1a, key=0)
    queue.add(bundle1b, key=0)
    queue.add(bundle2, key=1)

    assert len(queue) == 3

    # Get both bundles from key 0
    assert queue.get_next() is bundle1a
    assert queue.get_next() is bundle1b
    queue.finalize(key=0)

    # Now get from key 1
    assert queue.get_next() is bundle2
    queue.finalize(key=1)


def test_ordered_queue_finalize_before_all_consumed():
    """Test finalizing a key before all its bundles are consumed."""
    queue = ReorderingBundleQueue()
    bundle1a = _create_bundle("data1a")
    bundle1b = _create_bundle("data1b")
    bundle2 = _create_bundle("data2")

    queue.add(bundle1a, key=0)
    queue.add(bundle1b, key=0)
    queue.add(bundle2, key=1)

    # Finalize key 0 before consuming all bundles
    queue.finalize(key=0)

    # Should still be able to get all bundles from key 0
    assert queue.get_next() is bundle1a
    assert queue.get_next() is bundle1b

    # After consuming all, automatically moves to key 1
    assert queue.get_next() is bundle2


def test_ordered_queue_has_next_blocked_by_earlier_key():
    """Test that has_next returns False when current key has no bundles."""
    queue = ReorderingBundleQueue()
    bundle1 = _create_bundle("data11")

    # Add bundle for key 1, but nothing for key 0
    queue.add(bundle1, key=1)

    # has_next should return False because key 0 (current) has no bundles
    assert not queue.has_next()
    assert len(queue) == 1

    # Finalize key 0 (even though it's empty) to move to key 1
    queue.finalize(key=0)

    # Now has_next should return True
    assert queue.has_next()
    assert queue.get_next() is bundle1


def test_ordered_queue_peek_next():
    """Test peeking at the next bundle without removing it."""
    queue = ReorderingBundleQueue()
    bundle0 = _create_bundle("data1")
    bundle1 = _create_bundle("data11")

    queue.add(bundle0, key=0)
    queue.add(bundle1, key=1)

    # Peek should return bundle0 without removing
    assert queue.peek_next() is bundle0
    assert len(queue) == 2

    # Peek again should return the same bundle
    assert queue.peek_next() is bundle0


def test_ordered_queue_peek_next_empty():
    """Test peeking when current key has no bundles."""
    queue = ReorderingBundleQueue()
    bundle1 = _create_bundle("data11")

    queue.add(bundle1, key=1)

    # Current key 0 is empty
    assert queue.peek_next() is None


def test_ordered_queue_out_of_order():
    """Tests that ordered queue works correctly under following conditions"""

    queue = ReorderingBundleQueue()
    bundle0 = _create_bundle("data0")
    bundle1 = _create_bundle("data1")

    # First, add bundle for key=1
    queue.add(bundle1, key=1)
    queue.finalize(key=1)
    # No bundles can be retrieved yet as we're missing bundles for key=0
    assert not queue.has_next()

    # Next, add bundle for key=0
    queue.add(bundle0, key=0)
    assert queue.get_next() is bundle0
    queue.finalize(key=0)

    # Now able to retrieve bundle for key=1
    assert queue.get_next() is bundle1

    # `has_next` should return bundle0 without removing
    assert not queue.has_next()
    assert len(queue) == 0


@pytest.mark.parametrize("target_op", ["get", "peek"])
def test_ordered_queue_getting_stuck(target_op):
    bundle2 = _create_bundle("data2")

    queue = ReorderingBundleQueue()

    # Task 2 produces output and completes (finalizes key)
    queue.add(bundle2, key=2)
    queue.finalize(key=2)

    # _current_key = 0
    # _completed_keys = {2}

    # Task 1 completes with NO output (empty result)
    queue.finalize(key=1)

    # _current_key = 0
    # _completed_keys = {1, 2}

    # Task 0 completes with NO output (empty result)
    #
    # Previously this will trigger moving to the next key, since
    # _current_key == 0 AND _inner[0] is empty
    #   → _move_to_next_key()
    #       → _current_key = 1
    queue.finalize(key=0)

    # Current state:
    #   _current_key = 1
    #   _completed_keys = {0, 1, 2}
    #   _inner = {0: [], 1: [], 2: [bundle_2]}

    # Previously
    #   - `has_next` would return False, (_inner[_current_key] is empty)
    #   - `get_next` will never be invoked (b/c `has_next` returns false)
    #   - `finalize(key=1)` has already been invoked, no pointer advancement will happen
    #
    # This results in the last bundle getting stuck in the queue
    if target_op == "get":
        assert queue.get_next() is bundle2
    elif target_op == "peek":
        assert queue.peek_next() is bundle2
        assert queue.get_next() is bundle2
    else:
        pytest.fail(f"unsupported {target_op}")

    assert len(queue) == 0


def test_ordered_queue_get_next_empty_raises():
    """Test that get_next raises when current key is empty."""
    queue = ReorderingBundleQueue()

    with pytest.raises(ValueError, match="Cannot pop from empty queue"):
        queue.get_next()


def test_ordered_queue_clear():
    """Test clearing the queue resets everything."""
    queue = ReorderingBundleQueue()
    bundle0 = _create_bundle("data1")
    bundle1 = _create_bundle("data11")

    queue.add(bundle0, key=0)
    queue.add(bundle1, key=1)
    queue.finalize(key=0)
    queue.get_next()  # Consume bundle0, moves to key 1

    queue.clear()

    assert len(queue) == 0
    assert queue.estimate_size_bytes() == 0
    assert queue.num_blocks() == 0
    assert not queue.has_next()


def test_ordered_queue_metrics():
    """Test that metrics are tracked correctly."""
    queue = ReorderingBundleQueue()
    bundle0 = _create_bundle("data1")
    bundle1 = _create_bundle("data11")

    queue.add(bundle0, key=0)
    assert queue.estimate_size_bytes() == bundle0.size_bytes()
    assert queue.num_blocks() == 1

    queue.add(bundle1, key=1)
    assert queue.estimate_size_bytes() == bundle0.size_bytes() + bundle1.size_bytes()
    assert queue.num_blocks() == 2

    queue.get_next()
    queue.finalize(key=0)
    assert queue.estimate_size_bytes() == bundle1.size_bytes()
    assert queue.num_blocks() == 1


def test_ordered_queue_finalize_out_of_order():
    """Test that keys can be finalized out of order."""
    queue = ReorderingBundleQueue()
    bundle0 = _create_bundle("data1")
    bundle1 = _create_bundle("data11")
    bundle2 = _create_bundle("data111")

    queue.add(bundle0, key=0)
    queue.add(bundle1, key=1)
    queue.add(bundle2, key=2)

    # Finalize key 2 first, then 1, then 0
    queue.finalize(key=2)
    queue.finalize(key=1)

    # Should still need to consume key 0 first
    assert queue.get_next() is bundle0
    queue.finalize(key=0)

    assert queue.get_next() is bundle1
    assert queue.get_next() is bundle2


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
