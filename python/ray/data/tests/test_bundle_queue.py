import threading
import time
from typing import Any
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

import ray
from ray.data._internal.execution.bundle_queue import (
    HashLinkedQueue,
    ObjectStoreAwareBundleQueue,
    create_bundle_queue,
)
from ray.data._internal.execution.interfaces import BlockEntry, RefBundle
from ray.data.block import BlockAccessor
from ray.data.context import DataContext


def _create_bundle(data: Any) -> RefBundle:
    """Create a RefBundle with a single row with the given data."""
    block = pa.Table.from_pydict({"data": [data]})
    block_ref = ray.put(block)
    metadata = BlockAccessor.for_block(block).get_metadata()
    schema = BlockAccessor.for_block(block).schema()
    return RefBundle(
        [BlockEntry(block_ref, metadata)], owns_blocks=False, schema=schema
    )


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
    bundle2 = _create_bundle("test11")
    queue.add(bundle2)

    popped_bundle = queue.get_next()
    assert popped_bundle is bundle1
    assert len(queue) == 1
    assert queue.num_blocks() == 1
    assert queue.num_rows() == 1
    assert queue.estimate_size_bytes() == bundle2.size_bytes()


def test_peek_next():
    queue = create_bundle_queue()
    bundle1 = _create_bundle("test1")
    queue.add(bundle1)
    bundle2 = _create_bundle("test11")
    queue.add(bundle2)

    peeked_bundle = queue.peek_next()
    assert peeked_bundle is bundle1
    assert len(queue) == 2  # Length should remain unchanged
    assert queue.num_blocks() == 2
    assert queue.num_rows() == 2
    assert queue.estimate_size_bytes() == bundle1.size_bytes() + bundle2.size_bytes()


def test_get_next_empty_queue():
    queue = create_bundle_queue()
    with pytest.raises(IndexError):
        queue.get_next()


def test_get_next_does_not_leak_objects():
    queue = create_bundle_queue()
    bundle1 = _create_bundle("test11")
    queue.add(bundle1)
    queue.get_next()
    assert len(queue) == 0
    assert queue.estimate_size_bytes() == 0
    assert queue.num_rows() == 0
    assert queue.num_blocks() == 0


def test_peek_next_empty_queue():
    queue = create_bundle_queue()
    assert queue.peek_next() is None
    assert len(queue) == 0
    assert queue.num_blocks() == 0
    assert queue.estimate_size_bytes() == 0
    assert queue.num_rows() == 0


def test_remove():
    queue = create_bundle_queue()
    bundle1 = _create_bundle("test1")
    bundle2 = _create_bundle("test11")
    queue.add(bundle1)
    queue.add(bundle2)

    queue.remove(bundle1)
    assert len(queue) == 1
    assert queue.num_blocks() == 1
    assert queue.peek_next() is bundle2
    assert queue.estimate_size_bytes() == bundle2.size_bytes()
    assert queue.num_rows() == bundle2.num_rows()


def test_remove_does_not_leak_objects():
    queue = create_bundle_queue()
    bundle1 = _create_bundle("test1")
    queue.add(bundle1)
    queue.remove(bundle1)
    assert len(queue) == 0
    assert queue.num_blocks() == 0
    assert queue.estimate_size_bytes() == 0
    assert queue.num_rows() == 0


def test_add_and_remove_duplicates():
    queue = create_bundle_queue()
    bundle1 = _create_bundle("test1")
    bundle2 = _create_bundle("test11")
    queue.add(bundle1)
    queue.add(bundle2)
    queue.add(bundle1)

    assert len(queue) == 3
    assert queue.num_rows() == 3
    assert queue.num_blocks() == 3
    queue.remove(bundle1)
    assert len(queue) == 2

    assert queue.estimate_size_bytes() == bundle1.size_bytes() + bundle2.size_bytes()
    assert queue.num_rows() == 2
    assert queue.num_blocks() == 2
    assert queue.peek_next() is bundle2


def test_clear():
    queue = create_bundle_queue()
    queue.add(_create_bundle("test1"))
    queue.add(_create_bundle("test11"))
    queue.clear()
    assert len(queue) == 0
    assert queue.estimate_size_bytes() == 0
    assert queue.num_blocks() == 0
    assert queue.num_rows() == 0


# CVGA-end


@pytest.mark.parametrize(
    "env_value, preserve_order, expected_type",
    [
        (None, False, ObjectStoreAwareBundleQueue),
        ("1", False, ObjectStoreAwareBundleQueue),
        ("0", False, HashLinkedQueue),
        (None, True, HashLinkedQueue),
    ],
)
def test_create_bundle_queue(
    env_value, preserve_order, expected_type, monkeypatch, restore_data_context
):
    if env_value is not None:
        monkeypatch.setenv(
            "RAY_DATA_ENABLE_OBJECT_STORE_AWARE_BUNDLE_QUEUES", env_value
        )
    DataContext.get_current().execution_options.preserve_order = preserve_order

    assert isinstance(create_bundle_queue(), expected_type)


def _mock_object_locations(node_ids_by_ref):
    """Patch the local object-location lookup to return the given node IDs per ref,
    with every object reported as 1 MiB so none are treated as inlined."""
    mock = MagicMock(
        side_effect=lambda refs: {
            ref: {"node_ids": list(node_ids_by_ref[ref]), "object_size": 2**20}
            for ref in refs
        }
    )
    return patch.object(ray.experimental, "get_local_object_locations", mock)


def test_rotates_missing_bundles():
    lost = _create_bundle("lost")
    resident1 = _create_bundle("resident1")
    resident2 = _create_bundle("resident2")
    node_ids_by_ref = {
        lost.block_refs[0]: [],
        resident1.block_refs[0]: ["node1"],
        resident2.block_refs[0]: ["node1"],
    }

    queue = ObjectStoreAwareBundleQueue()
    for bundle in (lost, resident1, resident2):
        queue.add(bundle)

    with _mock_object_locations(node_ids_by_ref), patch(
        "ray.data._internal.utils.object_utils.get_drained_nodes", return_value=set()
    ):
        # The lost bundle sits at the front but is rotated behind the resident ones.
        assert queue.peek_next() is resident1
        assert queue.get_next() is resident1
        assert queue.get_next() is resident2
        # Once only lost bundles remain, they are still served rather than starved.
        assert queue.has_next()
        assert queue.get_next() is lost
        assert len(queue) == 0


def test_refreshes_size():
    bundle = _create_bundle("test1")
    # Two replicas of the block across nodes.
    node_ids_by_ref = {bundle.block_refs[0]: ["node1", "node2"]}

    queue = ObjectStoreAwareBundleQueue(update_frequency_s=0)
    queue.add(bundle)

    with _mock_object_locations(node_ids_by_ref):
        assert queue.estimate_size_bytes() == 2 * 2**20

    # Objects lost from the object store no longer count towards the estimate.
    node_ids_by_ref[bundle.block_refs[0]] = []
    with _mock_object_locations(node_ids_by_ref):
        assert queue.estimate_size_bytes() == 0


def test_thread_safety():
    with patch.object(
        ray.experimental, "get_local_object_locations", MagicMock()
    ) as mock_locations, patch(
        "ray.data._internal.utils.object_utils.get_drained_nodes", return_value=set()
    ):
        mock_locations.return_value = {"": {"node_ids": ["node1"], "object_size": 100}}

        queue = ObjectStoreAwareBundleQueue(update_frequency_s=0)
        exceptions = []

        def add_pop_worker():
            try:
                for _ in range(1000):
                    bundle = MagicMock(size_bytes=lambda: 100, num_rows=lambda: 1)
                    queue.add(bundle)
                    time.sleep(0.001)
                    queue.get_next()
            except Exception as e:
                exceptions.append(f"Add/Pop thread: {e}")

        def size_estimation_worker():
            try:
                for _ in range(2000):
                    assert queue.estimate_size_bytes() in (0, 100)
                    time.sleep(0.0005)
            except Exception as e:
                exceptions.append(f"Size thread: {e}")

        threads = [
            threading.Thread(target=add_pop_worker),
            threading.Thread(target=size_estimation_worker),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        assert not exceptions, f"Exceptions occurred: {exceptions}"
        assert len(queue) == 0
        assert queue.estimate_size_bytes() == 0


def test_remove_duplicates():
    bundle = _create_bundle(0)
    queue = ObjectStoreAwareBundleQueue(update_frequency_s=0)

    queue.add(bundle)
    queue.add(bundle)
    # Refreshing sizes between add and remove used to drop the size entry early.
    queue.estimate_size_bytes()
    queue.remove(bundle)
    queue.remove(bundle)

    assert len(queue) == 0
    assert queue.estimate_size_bytes() == 0


def test_size_with_duplicates():
    bundle = _create_bundle(0)
    queue = ObjectStoreAwareBundleQueue(update_frequency_s=0)

    queue.add(bundle)
    initial_estimate = queue.estimate_size_bytes()
    queue.add(bundle)

    # Both entries reference the same objects, so object store usage is unchanged.
    assert queue.estimate_size_bytes() == initial_estimate


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
