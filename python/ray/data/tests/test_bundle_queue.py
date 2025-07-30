from typing import Any

import pyarrow as pa
import pytest

import ray
from ray.data._internal.execution.bundle_queue import create_bundle_queue
from ray.data._internal.execution.interfaces import RefBundle
from ray.data.block import BlockAccessor


def _create_bundle(data: Any) -> RefBundle:
    """Create a RefBundle with a single row with the given data."""
    block = pa.Table.from_pydict({"data": [data]})
    block_ref = ray.put(block)
    metadata = BlockAccessor.for_block(block).get_metadata()
    schema = BlockAccessor.for_block(block).schema()
    return RefBundle([(block_ref, metadata)], owns_blocks=False, schema=schema)


# CVGA-start
def test_add_and_length():
    queue = create_bundle_queue()
    queue.add(_create_bundle("test1"))
    queue.add(_create_bundle("test2"))
    assert len(queue) == 2


def test_get_next():
    queue = create_bundle_queue()
    bundle1 = _create_bundle("test1")
    queue.add(bundle1)
    bundle2 = _create_bundle("test2")
    queue.add(bundle2)

    popped_bundle = queue.get_next()
    assert popped_bundle is bundle1
    assert len(queue) == 1


def test_peek_next():
    queue = create_bundle_queue()
    bundle1 = _create_bundle("test1")
    queue.add(bundle1)
    bundle2 = _create_bundle("test2")
    queue.add(bundle2)

    peeked_bundle = queue.peek_next()
    assert peeked_bundle is bundle1
    assert len(queue) == 2  # Length should remain unchanged


def test_get_next_empty_queue():
    queue = create_bundle_queue()
    with pytest.raises(IndexError):
        queue.get_next()


def test_get_next_does_not_leak_objects():
    queue = create_bundle_queue()
    bundle1 = _create_bundle("test1")
    queue.add(bundle1)
    queue.get_next()
    assert queue.is_empty()


def test_peek_next_empty_queue():
    queue = create_bundle_queue()
    assert queue.peek_next() is None
    assert queue.is_empty()


def test_remove():
    queue = create_bundle_queue()
    bundle1 = _create_bundle("test1")
    bundle2 = _create_bundle("test2")
    queue.add(bundle1)
    queue.add(bundle2)

    queue.remove(bundle1)
    assert len(queue) == 1
    assert queue.peek_next() is bundle2


def test_remove_does_not_leak_objects():
    queue = create_bundle_queue()
    bundle1 = _create_bundle("test1")
    queue.add(bundle1)
    queue.remove(bundle1)
    assert queue.is_empty()


def test_add_and_remove_duplicates():
    queue = create_bundle_queue()
    bundle1 = _create_bundle("test1")
    bundle2 = _create_bundle("test2")
    queue.add(bundle1)
    queue.add(bundle2)
    queue.add(bundle1)

    assert len(queue) == 3
    queue.remove(bundle1)
    assert len(queue) == 2
    assert queue.peek_next() is bundle2


def test_clear():
    queue = create_bundle_queue()
    queue.add(_create_bundle("test1"))
    queue.add(_create_bundle("test2"))
    queue.clear()
    assert len(queue) == 0
    assert queue.estimate_size_bytes() == 0
    assert queue.is_empty()


def test_estimate_size_bytes():
    queue = create_bundle_queue()
    bundle1 = _create_bundle("test1")
    bundle2 = _create_bundle("test2")
    queue.add(bundle1)
    queue.add(bundle2)
    assert queue.estimate_size_bytes() == bundle1.size_bytes() + bundle2.size_bytes()


# CVGA-end

if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
