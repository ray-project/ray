import time
from typing import Any
from unittest.mock import patch

import pyarrow as pa
import pytest

import ray
import ray.experimental
from ray.anyscale.data._internal.location_aware_bundle_queue import (
    LocationAwareBundleQueue,
)
from ray.data._internal.execution.interfaces import RefBundle
from ray.data.block import BlockAccessor


def _create_bundle(data: Any) -> RefBundle:
    """Create a RefBundle with a single row with the given data."""
    block = pa.Table.from_pydict({"data": [data]})
    block_ref = ray.put(block)
    metadata = BlockAccessor.for_block(block).get_metadata()
    return RefBundle([(block_ref, metadata)], owns_blocks=False)


def _free_bundle(bundle: RefBundle):
    for block_ref in bundle.block_refs:
        ray._private.internal_api.free(block_ref, local_only=False)
    # Wait for the free to take effect.
    time.sleep(0.1)


def test_add_and_length_with_free():
    queue = LocationAwareBundleQueue()
    bundle1 = _create_bundle("\x00" * 128 * 1024 * 1024)
    queue.add(bundle1)
    bundle2 = _create_bundle("\x00" * 128 * 1024 * 1024)
    queue.add(bundle2)

    _free_bundle(bundle1)

    # Length should remain unchanged even though the first bundle isn't in object store
    # memory.
    assert len(queue) == 2


def test_pop_with_free():
    queue = LocationAwareBundleQueue()
    bundle1 = _create_bundle("\x00" * 128 * 1024 * 1024)
    queue.add(bundle1)
    bundle2 = _create_bundle("\x00" * 128 * 1024 * 1024)
    queue.add(bundle2)

    _free_bundle(bundle1)

    # The second bundle should be popped because the first bundle isn't in object store
    # memory.
    popped_bundle = queue.pop()
    assert popped_bundle is bundle2
    assert len(queue) == 1


def test_peek_with_free():
    queue = LocationAwareBundleQueue()
    bundle1 = _create_bundle("\x00" * 128 * 1024 * 1024)
    queue.add(bundle1)
    bundle2 = _create_bundle("\x00" * 128 * 1024 * 1024)
    queue.add(bundle2)

    _free_bundle(bundle1)

    # The second bundle should be peeked because the first bundle isn't in object store
    # memory.
    peeked_bundle = queue.peek()
    assert peeked_bundle is bundle2
    assert len(queue) == 2  # Length should remain unchanged


def test_estimate_size_bytes():
    with patch.object(LocationAwareBundleQueue, "UPDATE_FREQUENCY_S", new=0.1):
        queue = LocationAwareBundleQueue()
        bundle = _create_bundle("\x00" * 128 * 1024 * 1024)
        queue.add(bundle)

        _free_bundle(bundle)

        time.sleep(0.1)  # Wait for the internal locations to update.

        assert queue.estimate_size_bytes() == 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
