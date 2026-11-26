from typing import Any, cast
from uuid import uuid4

import pandas as pd
import pytest

import ray
from ray.data._internal.execution.interfaces import BlockEntry, RefBundle
from ray.data._internal.experimental.execution.bundle_queue import (
    DrainingActorPriorityBundleQueue,
)
from ray.data.block import Block, BlockAccessor
from ray.types import ObjectRef


def _create_bundle(data: Any) -> RefBundle:
    """A RefBundle with a single row, using an artificial (not ray.put) block ref."""
    block = pd.DataFrame({"data": [data]})
    block_ref = cast("ObjectRef[Block]", ray.ObjectRef(uuid4().hex[:28].encode()))
    metadata = BlockAccessor.for_block(block).get_metadata()
    schema = BlockAccessor.for_block(block).schema()
    return RefBundle(
        (BlockEntry(block_ref, metadata),), owns_blocks=False, schema=schema
    )


def test_fifo_within_a_lane():
    queue = DrainingActorPriorityBundleQueue()
    b1, b2 = _create_bundle("a"), _create_bundle("b")
    queue.add(b1, key="x")
    queue.add(b2, key="y")

    assert queue.get_next() is b1
    assert queue.get_next() is b2
    assert not queue.has_next()


def test_new_bundles_from_prioritized_actor_come_first():
    queue = DrainingActorPriorityBundleQueue()
    regular = _create_bundle("regular")
    queue.add(regular, key="live")
    queue.prioritize_actor("drain")
    # Added AFTER prioritize -> routed to the priority lane.
    prio = _create_bundle("prio")
    queue.add(prio, key="drain")

    assert queue.get_next() is prio
    assert queue.get_next() is regular


def test_prioritize_with_empty_queue_still_routes_future_bundles():
    """Prioritizing before anything is queued must still register the actor:
    a draining actor's in-flight task may not have produced outputs yet, and
    the ones it produces later must route to the priority lane."""
    queue = DrainingActorPriorityBundleQueue()
    queue.prioritize_actor("drain")

    regular = _create_bundle("regular")
    queue.add(regular, key="live")
    prio = _create_bundle("prio")
    queue.add(prio, key="drain")

    assert queue.get_next() is prio
    assert queue.get_next() is regular


def test_prioritize_moves_already_queued_bundles():
    queue = DrainingActorPriorityBundleQueue()
    # Interleave two actors' bundles into the regular lane BEFORE draining.
    d1, l1, d2, l2 = (_create_bundle(name) for name in ("d1", "l1", "d2", "l2"))
    queue.add(d1, key="drain")
    queue.add(l1, key="live")
    queue.add(d2, key="drain")
    queue.add(l2, key="live")

    # Draining "drain" pulls its already-queued bundles ahead (relative order kept).
    queue.prioritize_actor("drain")
    assert queue.get_next() is d1
    assert queue.get_next() is d2
    # Then the live actor's bundles, in their original order.
    assert [queue.get_next() for _ in range(2)] == [l1, l2]


def test_deprioritize_stops_prioritizing_new_bundles():
    queue = DrainingActorPriorityBundleQueue()
    queue.prioritize_actor("a")
    assert "a" in queue.priority_actors()
    queue.deprioritize_actor("a")
    assert "a" not in queue.priority_actors()

    first = _create_bundle("first")
    queue.add(first, key="b")
    # "a" is no longer prioritized, so its new bundle stays behind "b"'s.
    a_bundle = _create_bundle("a-late")
    queue.add(a_bundle, key="a")
    assert queue.get_next() is first
    assert queue.get_next() is a_bundle


def test_metrics_track_across_add_and_pop():
    queue = DrainingActorPriorityBundleQueue()
    assert len(queue) == 0
    queue.add(_create_bundle("a"), key="x")
    queue.add(_create_bundle("b"), key="y")
    assert len(queue) == 2
    assert queue.num_blocks() == 2
    queue.get_next()
    assert len(queue) == 1
    queue.get_next()
    assert len(queue) == 0


def test_clear_resets_lanes_and_priority():
    queue = DrainingActorPriorityBundleQueue()
    queue.prioritize_actor("x")
    queue.add(_create_bundle("a"), key="x")
    queue.add(_create_bundle("b"), key="y")
    queue.clear()
    assert not queue.has_next()
    assert len(queue) == 0
    assert queue.priority_actors() == frozenset()


def test_peek_does_not_consume():
    queue = DrainingActorPriorityBundleQueue()
    b1 = _create_bundle("a")
    queue.add(b1, key="x")
    assert queue.peek_next() is b1
    assert len(queue) == 1  # unchanged
    assert queue.get_next() is b1


def test_pop_from_empty_raises():
    queue = DrainingActorPriorityBundleQueue()
    with pytest.raises(ValueError):
        queue.get_next()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
