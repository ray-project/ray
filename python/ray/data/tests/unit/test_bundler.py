import uuid
from typing import Any, List

import pandas as pd
import pytest

import ray
from ray.data._internal.execution.bundle_queue import (
    EstimateSize,
    ExactMultipleSize,
    RebundleQueue,
)
from ray.data._internal.execution.interfaces.ref_bundle import RefBundle
from ray.data.block import BlockAccessor


def _make_ref_bundles_for_unit_test(raw_bundles: List[List[List[Any]]]) -> tuple:
    """Create RefBundles with fake object refs for unit testing (no Ray required).

    Args:
        raw_bundles: A list of bundles, where each bundle is a list of blocks,
            and each block is a list of values.

    Returns:
        A tuple of (list of RefBundles, block_data_map) where block_data_map
        maps fake object refs to their actual DataFrame data.
    """
    output_bundles = []
    block_data_map = {}

    for raw_bundle in raw_bundles:
        blocks = []
        schema = None
        for raw_block in raw_bundle:
            block = pd.DataFrame({"id": raw_block})
            # Use UUID to generate unique fake object refs
            block_ref = ray.ObjectRef(uuid.uuid4().hex[:28].encode())
            block_data_map[block_ref] = block

            blocks.append((block_ref, BlockAccessor.for_block(block).get_metadata()))
            schema = BlockAccessor.for_block(block).schema()

        output_bundle = RefBundle(blocks=blocks, owns_blocks=True, schema=schema)
        output_bundles.append(output_bundle)

    return output_bundles, block_data_map


def _get_bundle_values(bundle: RefBundle, block_data_map: dict) -> List[List[Any]]:
    """Extract values from a bundle using block_data_map (no ray.get needed)."""
    output = []
    for block_ref in bundle.block_refs:
        output.append(list(block_data_map[block_ref]["id"]))
    return output


@pytest.mark.parametrize(
    "target,in_bundles,expected_row_counts",
    [
        (
            # Target of 2 rows per bundle
            2,
            [[[1]], [[2]], [[3]], [[4]]],
            [2, 2],  # Expected output: 2 bundles of 2 rows each
        ),
        (
            # Target of 3 rows with uneven inputs
            3,
            [[[1, 2]], [[3, 4, 5]], [[6]]],
            [3, 3],  # Expected: [1,2,3] and [4,5,6]
        ),
        (
            # Target of 4 rows with leftover
            4,
            [[[1, 2]], [[3, 4]], [[5, 6, 7]]],
            [4, 3],  # Expected: [1,2,3,4] and [5,6,7]
        ),
        (
            # Larger target with various input sizes
            5,
            [[[1, 2, 3]], [[4, 5, 6, 7]], [[8, 9]], [[10, 11, 12]]],
            [5, 5, 2],  # Expected: [1-5], [6-10], [11-12]
        ),
        (
            # Test with empty blocks
            3,
            [[[1]], [[]], [[2, 3]], [[]], [[4, 5]]],
            [3, 2],  # Expected: [1,2,3] and [4,5]
        ),
        (
            # Test with last block smaller than target num rows per block
            100,
            [[[1]], [[2]], [[3]], [[4]], [[5]]],
            [5],
        ),
    ],
)
def test_streaming_repartition_ref_bundler(target, in_bundles, expected_row_counts):
    """Test RebundleQueue with various input patterns (unit test)."""

    bundler = RebundleQueue(ExactMultipleSize(target))
    bundles, block_data_map = _make_ref_bundles_for_unit_test(in_bundles)
    out_bundles = []

    for bundle in bundles:
        bundler.add(bundle)

    # NOTE: The check for num bundles/blocks is harder to reason about since we rebundle bundles together
    og_total_size_bytes = bundler.estimate_size_bytes()
    og_total_num_rows = bundler.num_rows()
    assert sum(bundle.size_bytes() for bundle in bundles) == og_total_size_bytes
    assert sum(bundle.num_rows() for bundle in bundles) == og_total_num_rows

    all_original_bundles = []
    while bundler.has_next():
        out_bundle, original_bundles = bundler.get_next_with_original()
        out_bundles.append(out_bundle)
        all_original_bundles.extend(original_bundles)

    bundler.finalize()

    while bundler.has_next():
        out_bundle, original_bundles = bundler.get_next_with_original()
        out_bundles.append(out_bundle)
        all_original_bundles.extend(original_bundles)

    # Verify number of output bundles
    assert len(out_bundles) == len(
        expected_row_counts
    ), f"Expected {len(expected_row_counts)} bundles, got {len(out_bundles)}"

    # Verify row counts for each bundle
    for i, (out_bundle, expected_count) in enumerate(
        zip(out_bundles, expected_row_counts)
    ):
        assert (
            out_bundle.num_rows() == expected_count
        ), f"Bundle {i}: expected {expected_count} rows, got {out_bundle.num_rows()}"

    # Verify all bundles have been ingested
    assert bundler.num_blocks() == 0
    assert bundler.num_rows() == 0
    assert len(bundler) == 0
    assert bundler.estimate_size_bytes() == 0

    # Verify all output bundles except the last are exact multiples of target
    for i, out_bundle in enumerate(out_bundles[:-1]):
        assert (
            out_bundle.num_rows() % target == 0
        ), f"Bundle {i} has {out_bundle.num_rows()} rows, not a multiple of {target}"

    # Verify data integrity - all input data is preserved in order (bundler slicing is in order)
    total_input_rows = sum(sum(len(block) for block in bundle) for bundle in in_bundles)
    total_output_rows = sum(bundle.num_rows() for bundle in out_bundles)
    assert total_output_rows == total_input_rows

    # Verify block content - extract all values from output bundles
    output_values = []
    total_num_rows = 0
    total_size_bytes = 0
    for bundle in out_bundles:
        for (block_ref, _), block_slice in zip(bundle.blocks, bundle.slices):
            # Look up the actual block data from our map (no ray.get needed)
            data = block_data_map[block_ref]["id"]
            if block_slice is not None:
                # We apply the slice here manually because this is just for testing bundler
                # and the block slicing is happened in map operator for streaming repartition
                data = data[block_slice.start_offset : block_slice.end_offset]
            output_values.extend(data)
        total_num_rows += bundle.num_rows()
        total_size_bytes += bundle.size_bytes()

    assert og_total_size_bytes == total_size_bytes
    assert og_total_num_rows == total_num_rows

    # Expected values are all input values flattened in order
    expected_values = [
        value for bundle in in_bundles for block in bundle for value in block
    ]
    assert (
        output_values == expected_values
    ), f"Output values {output_values} don't match expected {expected_values}"

    # Verify get_next_with_original tracks all original bundles
    assert len(all_original_bundles) == len(bundles)
    for orig, expected in zip(all_original_bundles, bundles):
        assert orig is expected


def test_peek_next():
    """Test that peek_next returns the next bundle without removing it."""
    bundler = RebundleQueue(ExactMultipleSize(2))
    bundles, _ = _make_ref_bundles_for_unit_test([[[1]], [[2]], [[3]]])

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
    bundler = RebundleQueue(ExactMultipleSize(2))
    bundles, _ = _make_ref_bundles_for_unit_test([[[1]], [[2]], [[3]], [[4]]])

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
    new_bundles, _ = _make_ref_bundles_for_unit_test([[[10]], [[20]]])
    for bundle in new_bundles:
        bundler.add(bundle)

    assert bundler.has_next()
    out = bundler.get_next()
    assert out.num_rows() == 2


def test_add_updates_metrics():
    """Test that add correctly updates queue metrics."""
    bundler = RebundleQueue(ExactMultipleSize(10))  # High target so nothing gets built
    bundles, _ = _make_ref_bundles_for_unit_test([[[1, 2]], [[3, 4, 5]]])

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


# =============================================================================
# Tests for EstimateSize strategy
# =============================================================================


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
def test_estimate_size_bundler_basic(target, in_bundles, expected_bundles):
    """Test RebundleQueue with EstimateSize strategy creates expected output bundles."""
    bundler = RebundleQueue(EstimateSize(target))
    bundles, block_data_map = _make_ref_bundles_for_unit_test(in_bundles)
    out_bundles = []
    for bundle in bundles:
        bundler.add(bundle)
        while bundler.has_next():
            out_bundle = _get_bundle_values(bundler.get_next(), block_data_map)
            out_bundles.append(out_bundle)

    bundler.finalize()

    if bundler.has_next():
        out_bundle = _get_bundle_values(bundler.get_next(), block_data_map)
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
def test_estimate_size_bundler_uniform(
    target, n, num_bundles, num_out_bundles, out_bundle_size
):
    """Test RebundleQueue with EstimateSize creates expected number of bundles."""
    import numpy as np

    bundler = RebundleQueue(EstimateSize(target))
    data = np.arange(n)
    pre_bundles = [arr.tolist() for arr in np.array_split(data, num_bundles)]
    # Convert to expected format: each bundle has one block
    raw_bundles = [[list(arr)] for arr in pre_bundles]
    bundles, block_data_map = _make_ref_bundles_for_unit_test(raw_bundles)

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
        for block_ref in bundle.block_refs
        for i in list(block_data_map[block_ref]["id"])
    ]
    assert flat_out == list(range(n))


def test_estimate_size_peek_next():
    """Test peek_next with EstimateSize strategy."""
    bundler = RebundleQueue(EstimateSize(2))
    bundles, _ = _make_ref_bundles_for_unit_test([[[1]], [[2]], [[3]]])

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

    # get_next should return the same bundle
    got = bundler.get_next()
    assert got.num_rows() == peeked.num_rows()


def test_estimate_size_clear():
    """Test clear with EstimateSize strategy."""
    bundler = RebundleQueue(EstimateSize(2))
    bundles, _ = _make_ref_bundles_for_unit_test([[[1]], [[2]], [[3]], [[4]]])

    # Add some bundles
    for bundle in bundles:
        bundler.add(bundle)

    # Verify bundler has content
    assert bundler.has_next()
    assert bundler.num_rows() > 0

    # Clear the bundler
    bundler.clear()

    # Verify bundler is empty
    assert not bundler.has_next()
    assert bundler.num_rows() == 0
    assert len(bundler) == 0
    assert bundler.num_blocks() == 0

    # Verify we can add bundles again after clear
    new_bundles, _ = _make_ref_bundles_for_unit_test([[[10]], [[20]]])
    for bundle in new_bundles:
        bundler.add(bundle)

    assert bundler.has_next()
    out = bundler.get_next()
    assert out.num_rows() == 2


def test_estimate_size_add_updates_metrics():
    """Test add updates metrics with EstimateSize strategy."""
    bundler = RebundleQueue(EstimateSize(10))  # High target so nothing gets built
    bundles, _ = _make_ref_bundles_for_unit_test([[[1, 2]], [[3, 4, 5]]])

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
