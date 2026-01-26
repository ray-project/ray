from typing import Any, List

import numpy as np
import pandas as pd
import pytest

import ray
from ray.data._internal.execution.bundle_queue import EstimateSize, RebundleQueue
from ray.data._internal.execution.interfaces import RefBundle
from ray.data._internal.execution.util import make_ref_bundles
from ray.data.block import BlockAccessor
from ray.tests.conftest import *  # noqa


def _get_bundles(bundle: RefBundle):
    output = []
    for block_ref in bundle.block_refs:
        output.append(list(ray.get(block_ref)["id"]))
    return output


def _make_ref_bundles(raw_bundles: List[List[List[Any]]]) -> List[RefBundle]:
    rbs = []
    for raw_bundle in raw_bundles:
        blocks = []
        schema = None
        for raw_block in raw_bundle:
            print(f">>> {raw_block=}")

            block = pd.DataFrame({"id": raw_block})
            blocks.append(
                (ray.put(block), BlockAccessor.for_block(block).get_metadata())
            )
            schema = BlockAccessor.for_block(block).schema()

        rb = RefBundle(blocks=blocks, owns_blocks=True, schema=schema)

        rbs.append(rb)

    return rbs


@pytest.mark.parametrize(
    "target,in_bundles,expected_bundles",
    [
        (
            # Unit target, should leave unchanged.
            1,
            [
                # Input bundles
                [[1]],
                [[2]],
                [[3, 4]],
                [[5]],
            ],
            [
                # Output bundles
                [[1]],
                [[2]],
                [[3, 4]],
                [[5]],
            ],
        ),
        (
            # No target, should leave unchanged.
            None,
            [
                # Input bundles
                [[1]],
                [[2]],
                [[3, 4]],
                [[5]],
            ],
            [
                # Output bundles
                [[1]],
                [[2]],
                [[3, 4]],
                [[5]],
            ],
        ),
        (
            # Proper handling of empty blocks
            2,
            [
                # Input bundles
                [[1]],
                [[]],
                [[]],
                [[2, 3]],
                [[]],
                [[]],
            ],
            [
                # Output bundles
                [[1], [], [], [2, 3]],
                [[], []],
            ],
        ),
        (
            # Test bundling, finalizing, passing, leftovers, etc.
            2,
            [
                # Input bundles
                [[1], [2]],
                [[3, 4, 5]],
                [[6], [7]],
                [[8]],
                [[9, 10], [11]],
            ],
            [[[1], [2]], [[3, 4, 5]], [[6], [7]], [[8], [9, 10], [11]]],
        ),
        (
            # Test bundling, finalizing, passing, leftovers, etc.
            3,
            [
                # Input bundles
                [[1]],
                [[2, 3]],
                [[4, 5, 6, 7]],
                [[8, 9], [10, 11]],
            ],
            [
                # Output bundles
                [[1], [2, 3]],
                [[4, 5, 6, 7]],
                [[8, 9], [10, 11]],
            ],
        ),
    ],
)
def test_block_ref_bundler_basic(target, in_bundles, expected_bundles):
    # Test that the bundler creates the expected output bundles.
    bundler = RebundleQueue(EstimateSize(target))
    bundles = _make_ref_bundles(in_bundles)
    out_bundles = []
    for bundle in bundles:
        bundler.add(bundle)
        while bundler.has_next():
            out_bundle = _get_bundles(bundler.get_next())
            out_bundles.append(out_bundle)

    bundler.finalize()

    if bundler.has_next():
        out_bundle = _get_bundles(bundler.get_next())
        out_bundles.append(out_bundle)

    # Assert expected output
    assert out_bundles == expected_bundles
    # Assert that all bundles have been ingested
    assert bundler.num_blocks() == 0

    for bundle, expected in zip(out_bundles, expected_bundles):
        assert bundle == expected


@pytest.mark.parametrize(
    "target,n,num_bundles,num_out_bundles,out_bundle_size",
    [
        (5, 20, 20, 4, 5),
        (5, 24, 10, 4, 6),
        (8, 16, 4, 2, 8),
    ],
)
def test_block_ref_bundler_uniform(
    target, n, num_bundles, num_out_bundles, out_bundle_size
):
    # Test that the bundler creates the expected number of bundles with the expected
    # size.
    bundler = RebundleQueue(EstimateSize(target))
    data = np.arange(n)
    pre_bundles = [arr.tolist() for arr in np.array_split(data, num_bundles)]
    bundles = make_ref_bundles(pre_bundles)
    out_bundles = []
    for bundle in bundles:
        bundler.add(bundle)
        while bundler.has_next():
            out_bundle = bundler.get_next()
            out_bundles.append(out_bundle)
    bundler.finalize()
    if bundler.has_next():
        out_bundle = bundler.get_next()
        out_bundles.append(out_bundle)
    assert len(out_bundles) == num_out_bundles
    for out_bundle in out_bundles:
        assert out_bundle.num_rows() == out_bundle_size
    flat_out = [
        i
        for bundle in out_bundles
        for block, _ in bundle.blocks
        for i in list(ray.get(block)["id"])
    ]
    assert flat_out == list(range(n))


def test_peek_next():
    """Test that peek_next returns the next bundle without removing it."""
    bundler = RebundleQueue(EstimateSize(2))
    bundles = _make_ref_bundles([[[1]], [[2]], [[3]]])

    # Peek on empty queue returns None
    assert bundler.peek_next() is None

    # Add bundles until we have a ready bundle
    bundler.add(bundles[0])
    assert bundler.peek_next() is None  # Not enough rows yet

    bundler.add(bundles[1])
    assert bundler.has_next()

    # Peek should return the bundle without removing it
    peeked = bundler.peek_next()
    assert peeked is not None
    assert peeked.num_rows() == 2

    # Peek again should return the same bundle
    peeked2 = bundler.peek_next()
    assert peeked2 is peeked

    # Metrics should be unchanged after peek
    initial_rows = bundler.num_rows()
    initial_len = len(bundler)
    bundler.peek_next()
    assert bundler.num_rows() == initial_rows
    assert len(bundler) == initial_len

    # get_next should return the same bundle
    got = bundler.get_next()
    assert got.num_rows() == peeked.num_rows()


def test_clear():
    """Test that clear resets the bundler to empty state."""
    bundler = RebundleQueue(EstimateSize(2))
    bundles = _make_ref_bundles([[[1]], [[2]], [[3]], [[4]]])

    # Add some bundles
    for bundle in bundles:
        bundler.add(bundle)

    # Verify bundler has content
    assert bundler.has_next()
    assert bundler.num_rows() > 0
    assert len(bundler) > 0
    assert bundler.estimate_size_bytes() > 0

    # Clear the bundler
    bundler.clear()

    # Verify bundler is empty
    assert not bundler.has_next()
    assert bundler.num_rows() == 0
    assert len(bundler) == 0
    assert bundler.num_blocks() == 0
    assert bundler.estimate_size_bytes() == 0
    assert bundler.peek_next() is None

    # Verify we can add bundles again after clear
    new_bundles = _make_ref_bundles([[[10]], [[20]]])
    for bundle in new_bundles:
        bundler.add(bundle)

    assert bundler.has_next()
    out = bundler.get_next()
    assert out.num_rows() == 2


def test_add_updates_metrics():
    """Test that add correctly updates queue metrics."""
    bundler = RebundleQueue(EstimateSize(10))  # High target so nothing gets built
    bundles = _make_ref_bundles([[[1, 2]], [[3, 4, 5]]])

    # Initially empty
    assert bundler.num_rows() == 0
    assert bundler.num_blocks() == 0
    assert bundler.estimate_size_bytes() == 0

    # Add first bundle
    bundler.add(bundles[0])
    assert bundler.num_rows() == 2
    assert bundler.num_blocks() == 1
    assert bundler.estimate_size_bytes() == bundles[0].size_bytes()

    # Add second bundle
    bundler.add(bundles[1])
    assert bundler.num_rows() == 5
    assert bundler.num_blocks() == 2
    expected_bytes = bundles[0].size_bytes() + bundles[1].size_bytes()
    assert bundler.estimate_size_bytes() == expected_bytes


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
