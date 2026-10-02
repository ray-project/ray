import threading
import time
from typing import Any
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

import ray
from ray.data._internal.execution.bundle_queue import (
    HashLinkedQueue,
    ResidentFirstBundleQueue,
    create_bundle_queue,
)
from ray.data._internal.execution.interfaces import BlockEntry, RefBundle
from ray.data.block import BlockAccessor


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
        (None, False, ResidentFirstBundleQueue),
        ("1", False, ResidentFirstBundleQueue),
        ("0", False, HashLinkedQueue),
        (None, True, HashLinkedQueue),
    ],
)
def test_create_bundle_queue(env_value, preserve_order, expected_type, monkeypatch):
    if env_value is not None:
        monkeypatch.setenv("RAY_DATA_ENABLE_RESIDENT_FIRST_BUNDLE_QUEUES", env_value)

    assert isinstance(create_bundle_queue(preserve_order=preserve_order), expected_type)


def _mock_object_locations(node_ids_by_ref):
    """Patch the location lookup to return the given node IDs per ref, with every
    object reported as 1 MiB so none are treated as inlined."""
    mock = MagicMock(
        side_effect=lambda refs: {
            ref: {"node_ids": list(node_ids_by_ref[ref]), "object_size": 2**20}
            for ref in refs
        }
    )
    return patch("ray.experimental.locations.get_local_object_locations", mock)


def _patch_node_loss_version(version: int):
    return patch(
        "ray.data._internal.execution.bundle_queue.resident_first.get_node_loss_version",
        return_value=version,
    )


def test_rotates_missing_bundles():
    lost = _create_bundle("lost")
    resident1 = _create_bundle("resident1")
    resident2 = _create_bundle("resident2")
    node_ids_by_ref = {
        lost.block_refs[0]: [],
        resident1.block_refs[0]: ["node1"],
        resident2.block_refs[0]: ["node1"],
    }

    queue = ResidentFirstBundleQueue()
    # Queue more lost instances than there are distinct bundles, so a rotation
    # budget counted in distinct bundles would stop before reaching a resident.
    with _patch_node_loss_version(0):
        for bundle in (lost, lost, lost, lost, resident1, resident2):
            queue.add(bundle)

    # Blocks can only go missing after a node loss, so simulate one.
    with _mock_object_locations(node_ids_by_ref), _patch_node_loss_version(1), patch(
        "ray.data._internal.utils.object_utils.get_lost_node_ids", return_value=set()
    ):
        # The lost bundle sits at the front but is rotated behind the resident ones.
        assert queue.peek_next() is resident1
        assert queue.get_next() is resident1
        assert queue.get_next() is resident2
        # Once only lost bundles remain, they are still served rather than starved.
        assert queue.has_next()
        for _ in range(4):
            assert queue.get_next() is lost
        assert len(queue) == 0


def test_no_lookup_until_node_loss():
    bundle = _create_bundle("resident")
    node_ids_by_ref = {bundle.block_refs[0]: ["node1"]}
    queue = ResidentFirstBundleQueue()

    with _mock_object_locations(node_ids_by_ref) as lookup, patch(
        "ray.data._internal.utils.object_utils.get_lost_node_ids", return_value=set()
    ):
        # No node lost since start: blocks are pinned, nothing to check.
        with _patch_node_loss_version(0):
            queue.add(bundle)
            assert queue.has_resident_next()
            assert queue.peek_next() is bundle
        assert lookup.call_count == 0

        # A node loss bumps the version: verify once, then trust it again.
        with _patch_node_loss_version(1):
            assert queue.has_resident_next()
            assert queue.has_resident_next()
        assert lookup.call_count == 1


def test_bundle_added_after_node_loss_is_verified():
    """A bundle can reach a queue long after its blocks were produced (e.g. via
    an upstream queue), so once any node has been lost an add is not trusted."""
    bundle = _create_bundle("resident")
    node_ids_by_ref = {bundle.block_refs[0]: ["node1"]}
    queue = ResidentFirstBundleQueue()

    with _mock_object_locations(node_ids_by_ref) as lookup, patch(
        "ray.data._internal.utils.object_utils.get_lost_node_ids", return_value=set()
    ), _patch_node_loss_version(1):
        queue.add(bundle)
        assert queue.has_resident_next()
        assert lookup.call_count == 1
        assert queue.has_resident_next()
        assert lookup.call_count == 1


def test_node_loss_tracker_versions():
    from ray.data._internal.utils.cached_ray_internals import _NodeLossTracker

    tracker = _NodeLossTracker()
    # The first observation is the baseline, not a loss.
    assert tracker.refresh(frozenset({"already-dead"})) == 0
    assert tracker.refresh(frozenset({"already-dead"})) == 0
    # Any change to the lost set is one event.
    assert tracker.refresh(frozenset({"already-dead", "n1"})) == 1
    assert tracker.refresh(frozenset({"n1"})) == 2
    assert tracker.refresh(frozenset({"n1"})) == 2


def test_has_resident_next():
    lost = _create_bundle("lost")
    resident = _create_bundle("resident")
    node_ids_by_ref = {lost.block_refs[0]: [], resident.block_refs[0]: ["node1"]}

    queue = ResidentFirstBundleQueue()
    with _patch_node_loss_version(0):
        queue.add(lost)
    # Blocks can only go missing after a node loss, so simulate one.
    with _mock_object_locations(node_ids_by_ref), _patch_node_loss_version(1), patch(
        "ray.data._internal.utils.object_utils.get_lost_node_ids", return_value=set()
    ):
        # A lost bundle counts as present but not resident.
        assert queue.has_next()
        assert not queue.has_resident_next()

        # One resident bundle anywhere in the queue makes the next bundle resident.
        queue.add(resident)
        assert queue.has_resident_next()
        assert queue.get_next() is resident
        assert not queue.has_resident_next()


def test_thread_safety():
    mock_locations = MagicMock(
        return_value={"": {"node_ids": ["node1"], "object_size": 100}}
    )
    with patch(
        "ray.experimental.locations.get_local_object_locations", mock_locations
    ), patch(
        "ray.data._internal.utils.object_utils.get_lost_node_ids", return_value=set()
    ):
        queue = ResidentFirstBundleQueue()
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
    queue = ResidentFirstBundleQueue()

    queue.add(bundle)
    queue.add(bundle)
    queue.estimate_size_bytes()
    queue.remove(bundle)
    queue.remove(bundle)

    assert len(queue) == 0
    assert queue.estimate_size_bytes() == 0


def test_size_with_duplicates():
    bundle = _create_bundle(0)
    queue = ResidentFirstBundleQueue()

    queue.add(bundle)
    initial_estimate = queue.estimate_size_bytes()
    queue.add(bundle)

    # Both entries reference the same objects, so object store usage is unchanged.
    assert queue.estimate_size_bytes() == initial_estimate


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
