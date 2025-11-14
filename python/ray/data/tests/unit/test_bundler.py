from typing import Any, List

import pandas as pd
import pytest

import ray
from ray.data._internal.execution.interfaces.ref_bundle import RefBundle
from ray.data._internal.streaming_repartition import StreamingRepartitionRefBundler
from ray.data.block import BlockAccessor


def _make_ref_bundles_for_unit_test(raw_bundles: List[List[List[Any]]]) -> tuple:
    output_bundles = []
    block_data_map = {}
    ref_counter = 0

    for raw_bundle in raw_bundles:
        blocks = []
        schema = None
        for raw_block in raw_bundle:
            block = pd.DataFrame({"id": raw_block})
            block_ref = ray.ObjectRef(str(ref_counter).encode().ljust(28, b"0"))
            ref_counter += 1
            block_data_map[block_ref] = block

            blocks.append((block_ref, BlockAccessor.for_block(block).get_metadata()))
            schema = BlockAccessor.for_block(block).schema()

        output_bundle = RefBundle(blocks=blocks, owns_blocks=True, schema=schema)
        output_bundles.append(output_bundle)

    return output_bundles, block_data_map


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
    ],
)
def test_streaming_repartition_ref_bundler(target, in_bundles, expected_row_counts):
    """Test StreamingRepartitionRefBundler with various input patterns (unit test)."""

    bundler = StreamingRepartitionRefBundler(target)
    bundles, block_data_map = _make_ref_bundles_for_unit_test(in_bundles)
    out_bundles = []

    for bundle in bundles:
        bundler.add_bundle(bundle)
        while bundler.has_bundle():
            _, out_bundle = bundler.get_next_bundle()
            out_bundles.append(out_bundle)

    bundler.done_adding_bundles()

    while bundler.has_bundle():
        _, out_bundle = bundler.get_next_bundle()
        out_bundles.append(out_bundle)

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
    for bundle in out_bundles:
        for (block_ref, _), block_slice in zip(bundle.blocks, bundle.slices):
            # Look up the actual block data from our map (no ray.get needed)
            data = block_data_map[block_ref]["id"]
            if block_slice is not None:
                # We apply the slice here manually because this is just for testing bundler
                # and the block slicing is happened in map operator for streaming repartition
                data = data[block_slice.start_offset : block_slice.end_offset]
            output_values.extend(data)

    # Expected values are all input values flattened in order
    expected_values = [
        value for bundle in in_bundles for block in bundle for value in block
    ]
    assert (
        output_values == expected_values
    ), f"Output values {output_values} don't match expected {expected_values}"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
