from typing import Any, List

import numpy as np
import pandas as pd
import pytest

import ray
from ray.data._internal.execution.interfaces import RefBundle
from ray.data._internal.execution.operators.map_operator import BlockRefBundler
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
    bundler = BlockRefBundler(target)
    bundles = _make_ref_bundles(in_bundles)
    out_bundles = []
    for bundle in bundles:
        bundler.add_bundle(bundle)
        while bundler.has_bundle():
            out_bundle = _get_bundles(bundler.get_next_bundle()[1])
            out_bundles.append(out_bundle)

    bundler.done_adding_bundles()

    if bundler.has_bundle():
        out_bundle = _get_bundles(bundler.get_next_bundle()[1])
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
    bundler = BlockRefBundler(target)
    data = np.arange(n)
    pre_bundles = [arr.tolist() for arr in np.array_split(data, num_bundles)]
    bundles = make_ref_bundles(pre_bundles)
    out_bundles = []
    for bundle in bundles:
        bundler.add_bundle(bundle)
        while bundler.has_bundle():
            _, out_bundle = bundler.get_next_bundle()
            out_bundles.append(out_bundle)
    bundler.done_adding_bundles()
    if bundler.has_bundle():
        _, out_bundle = bundler.get_next_bundle()
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


def test_block_ref_bundler_get_next_regression():
    """Test that all remaining bundles are appropriately preserved after `get_next_bundle`."""

    # Create 4 blocks, each with 1 row
    bundler = BlockRefBundler(min_rows_per_bundle=2)
    bundles = _make_ref_bundles([[[1]], [[2]], [[3]], [[4]]])

    # Add all bundles at once
    for b in bundles:
        bundler.add_bundle(b)

    # Buffer now has 4 rows total
    assert bundler.num_blocks() == 4

    # First get_next_bundle should return bundles with 2 rows
    assert bundler.has_bundle()
    _, out_bundle = bundler.get_next_bundle()
    assert out_bundle.num_rows() == 2
    assert _get_bundles(out_bundle) == [[1], [2]]

    # Remainder should have 2 bundles with 2 rows total
    # BUG: Without the break statement, only 1 bundle (1 row) remains
    assert bundler.num_blocks() == 2, (
        f"Expected 2 rows remaining, got {bundler._bundle_buffer_size}. "
        "This indicates the remainder was overwritten in the loop."
    )

    # Second get_next_bundle (after finalization) should return remaining bundles
    bundler.done_adding_bundles()

    assert bundler.has_bundle()
    _, out_bundle = bundler.get_next_bundle()

    assert out_bundle.num_rows() == 2
    assert _get_bundles(out_bundle) == [[3], [4]]

    # Buffer should now be empty
    assert bundler.num_blocks() == 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
