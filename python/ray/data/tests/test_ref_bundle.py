from dataclasses import replace
from unittest.mock import patch

import pytest

from ray import ObjectRef
from ray.data._internal.execution.interfaces import BlockSlice, RefBundle
from ray.data.block import BlockMetadata


def test_get_preferred_locations():

    first_block_ref = ObjectRef(b"1" * 28)
    second_block_ref = ObjectRef(b"2" * 28)
    third_block_ref = ObjectRef(b"3" * 28)

    meta = BlockMetadata(num_rows=None, size_bytes=1, exec_stats=None, input_files=None)

    bundle = RefBundle(
        blocks=[
            (first_block_ref, meta),
            (second_block_ref, meta),
            (third_block_ref, meta),
        ],
        owns_blocks=True,
        schema=None,
    )

    def _get_obj_locs(obj_refs):
        assert obj_refs == [first_block_ref, second_block_ref, third_block_ref]

        return {
            first_block_ref: {
                "object_size": 1024,
                "did_spill": False,
                "node_ids": ["1", "2", "3"],
            },
            second_block_ref: {
                "object_size": 2048,
                "did_spill": False,
                "node_ids": ["2", "3"],
            },
            third_block_ref: {
                "object_size": 4096,
                "did_spill": False,
                "node_ids": ["2"],
            },
        }

    with patch("ray.experimental.get_local_object_locations", _get_obj_locs):
        preferred_object_locs = bundle.get_preferred_object_locations()

    assert {
        "1": 1024,  # first_block_ref]
        "2": 7168,  # first_block_ref, second_block_ref, third_block_ref
        "3": 3072,  # first_block_ref, second_block_ref
    } == preferred_object_locs


def test_ref_bundle_num_rows_size_bytes():

    block_ref_one = ObjectRef(b"1" * 28)
    block_ref_two = ObjectRef(b"2" * 28)

    meta_one = BlockMetadata(
        num_rows=10, size_bytes=100, exec_stats=None, input_files=None
    )
    meta_two = BlockMetadata(
        num_rows=5, size_bytes=50, exec_stats=None, input_files=None
    )
    # Before slice
    bundle = RefBundle(
        blocks=[
            (block_ref_one, meta_one),
            (block_ref_two, meta_two),
        ],
        owns_blocks=True,
        schema=None,
    )
    assert bundle.num_rows() == 15
    assert bundle.size_bytes() == 150
    # After slice
    sliced = replace(
        bundle,
        slices=(
            BlockSlice(start_offset=2, end_offset=6),  # 4 rows
            BlockSlice(start_offset=0, end_offset=2),  # 2 rows
        ),
    )

    assert sliced.num_rows() == 6
    assert sliced.size_bytes() == 60


@pytest.mark.parametrize(
    "start_offset, end_offset",
    [
        (-1, 0),  # Negative start_offset
        (0, 11),  # end_offset > num_rows
        (1, 0),  # start_offset > end_offset
    ],
)
def test_ref_bundle_with_invalid_slices(start_offset, end_offset):
    block_ref = ObjectRef(b"1" * 28)
    metadata = BlockMetadata(
        num_rows=10, size_bytes=100, exec_stats=None, input_files=None
    )
    with pytest.raises(AssertionError):
        RefBundle(
            blocks=[(block_ref, metadata)],
            owns_blocks=True,
            schema=None,
            slices=[
                BlockSlice(start_offset=start_offset, end_offset=end_offset),
            ],
        )


def test_slice_ref_bundle_basic():

    block_ref_one = ObjectRef(b"1" * 28)
    block_ref_two = ObjectRef(b"2" * 28)

    meta_one = BlockMetadata(
        num_rows=6, size_bytes=60, exec_stats=None, input_files=None
    )
    meta_two = BlockMetadata(
        num_rows=4, size_bytes=40, exec_stats=None, input_files=None
    )

    bundle = RefBundle(
        blocks=[
            (block_ref_one, meta_one),
            (block_ref_two, meta_two),
        ],
        owns_blocks=True,
        schema="schema",
    )

    consumed, remaining = bundle.slice(8)

    assert consumed.num_rows() == 8
    assert remaining.num_rows() == 2

    assert consumed.slices == (
        BlockSlice(start_offset=0, end_offset=6),
        BlockSlice(start_offset=0, end_offset=2),
    )
    assert remaining.slices == (
        BlockSlice(start_offset=2, end_offset=4),
    )


def test_slice_ref_bundle_should_raise_error_if_needed_rows_is_not_less_than_num_rows():

    block_ref = ObjectRef(b"1" * 28)
    metadata = BlockMetadata(
        num_rows=5, size_bytes=50, exec_stats=None, input_files=None
    )

    bundle = RefBundle(
        blocks=[(block_ref, metadata)],
        owns_blocks=True,
        schema=None,
    )

    with pytest.raises(AssertionError):
        bundle.slice(5)


def test_slice_ref_bundle_with_existing_slices():

    block_ref_one = ObjectRef(b"1" * 28)
    block_ref_two = ObjectRef(b"2" * 28)

    meta_one = BlockMetadata(
        num_rows=10, size_bytes=100, exec_stats=None, input_files=None
    )
    meta_two = BlockMetadata(
        num_rows=5, size_bytes=50, exec_stats=None, input_files=None
    )

    bundle = RefBundle(
        blocks=[
            (block_ref_one, meta_one),
            (block_ref_two, meta_two),
        ],
        owns_blocks=True,
        schema="schema",
        slices=[
            BlockSlice(start_offset=2, end_offset=10),
            BlockSlice(start_offset=0, end_offset=3),
        ],
    )

    consumed, remaining = bundle.slice(7)

    assert consumed.num_rows() == 7
    assert consumed.slices == (
        BlockSlice(start_offset=2, end_offset=9),
    )
    assert consumed.size_bytes() == 70
    assert remaining.num_rows() == 4
    assert remaining.slices == (
        BlockSlice(start_offset=9, end_offset=10),
        BlockSlice(start_offset=0, end_offset=3),
    )
    assert remaining.size_bytes() == 40


@pytest.mark.parametrize(
    "num_rows,slice_rows",
    [
        (5, 0),  # Zero rows requested
        (5, 5),  # Equal to total (must be less than)
        (5, 6),  # More than available
    ],
)
def test_slice_ref_bundle_invalid_rows(num_rows, slice_rows):
    """Test that slicing with invalid row counts raises appropriate errors."""
    block_ref = ObjectRef(b"1" * 28)
    metadata = BlockMetadata(
        num_rows=num_rows, size_bytes=num_rows * 10, exec_stats=None, input_files=None
    )

    bundle = RefBundle(
        blocks=[(block_ref, metadata)],
        owns_blocks=True,
        schema=None,
    )

    with pytest.raises(AssertionError):
        bundle.slice(slice_rows)


def test_ref_bundle_with_none_slices():
    """Test that None can be used to represent full blocks in slices."""

    block_ref_one = ObjectRef(b"1" * 28)
    block_ref_two = ObjectRef(b"2" * 28)

    meta_one = BlockMetadata(
        num_rows=10, size_bytes=100, exec_stats=None, input_files=None
    )
    meta_two = BlockMetadata(
        num_rows=5, size_bytes=50, exec_stats=None, input_files=None
    )

    # Test with all None slices (representing full blocks)
    bundle = RefBundle(
        blocks=[
            (block_ref_one, meta_one),
            (block_ref_two, meta_two),
        ],
        owns_blocks=True,
        schema=None,
        slices=[None, None],
    )

    assert bundle.num_rows() == 15  # 10 + 5
    assert bundle.size_bytes() == 150  # 100 + 50


def test_ref_bundle_with_mixed_none_and_explicit_slices():
    """Test mixing None and explicit BlockSlice objects."""

    block_ref_one = ObjectRef(b"1" * 28)
    block_ref_two = ObjectRef(b"2" * 28)
    block_ref_three = ObjectRef(b"3" * 28)

    meta_one = BlockMetadata(
        num_rows=10, size_bytes=100, exec_stats=None, input_files=None
    )
    meta_two = BlockMetadata(
        num_rows=8, size_bytes=80, exec_stats=None, input_files=None
    )
    meta_three = BlockMetadata(
        num_rows=6, size_bytes=60, exec_stats=None, input_files=None
    )

    # Mix None (full block) with explicit slices
    bundle = RefBundle(
        blocks=[
            (block_ref_one, meta_one),
            (block_ref_two, meta_two),
            (block_ref_three, meta_three),
        ],
        owns_blocks=True,
        schema=None,
        slices=[
            None,  # Full block: 10 rows, 100 bytes
            BlockSlice(start_offset=2, end_offset=6),  # 4 rows, ~40 bytes
            None,  # Full block: 6 rows, 60 bytes
        ],
    )

    assert bundle.num_rows() == 20  # 10 + 4 + 6
    assert bundle.size_bytes() == 200  # 100 + 40 + 60


def test_slice_ref_bundle_with_none_slices():
    """Test slicing a bundle that has None slices."""

    block_ref_one = ObjectRef(b"1" * 28)
    block_ref_two = ObjectRef(b"2" * 28)

    meta_one = BlockMetadata(
        num_rows=6, size_bytes=60, exec_stats=None, input_files=None
    )
    meta_two = BlockMetadata(
        num_rows=4, size_bytes=40, exec_stats=None, input_files=None
    )

    # Start with None slices (full blocks)
    bundle = RefBundle(
        blocks=[
            (block_ref_one, meta_one),
            (block_ref_two, meta_two),
        ],
        owns_blocks=True,
        schema="schema",
        slices=[None, None],
    )

    # Slice it to get first 8 rows
    consumed, remaining = bundle.slice(8)

    assert consumed.num_rows() == 8
    assert remaining.num_rows() == 2

    # The None slices should be converted to explicit BlockSlice objects
    assert consumed.slices == (
        BlockSlice(start_offset=0, end_offset=6),
        BlockSlice(start_offset=0, end_offset=2),
    )
    assert remaining.slices == (
        BlockSlice(start_offset=2, end_offset=4),
    )


def test_ref_bundle_str():
    """Test the __str__ method returns a readable representation."""
    block_ref_one = ObjectRef(b"1" * 28)
    block_ref_two = ObjectRef(b"2" * 28)
    block_ref_three = ObjectRef(b"3" * 28)

    meta_one = BlockMetadata(
        num_rows=10, size_bytes=100, exec_stats=None, input_files=None
    )
    meta_two = BlockMetadata(
        num_rows=5, size_bytes=50, exec_stats=None, input_files=None
    )
    meta_three = BlockMetadata(
        num_rows=3, size_bytes=30, exec_stats=None, input_files=None
    )
    slice_three = BlockSlice(start_offset=0, end_offset=3)

    bundle = RefBundle(
        blocks=[
            (block_ref_one, meta_one),
            (block_ref_two, meta_two),
            (block_ref_three, meta_three),
        ],
        owns_blocks=True,
        schema="test_schema",
        slices=[None, None, slice_three],
    )

    expected = """RefBundle(3 blocks,
  18 rows,
  schema=test_schema,
  owns_blocks=True,
  blocks=(
    0: 10 rows, 100 bytes, slice=None (full block)
    1: 5 rows, 50 bytes, slice=None (full block)
    2: 3 rows, 30 bytes, slice=BlockSlice(start_offset=0, end_offset=3)
  )
)"""

    assert str(bundle) == expected


def test_merge_ref_bundles():

    block_ref_one = ObjectRef(b"1" * 28)
    block_ref_two = ObjectRef(b"2" * 28)

    metadata_one = BlockMetadata(
        num_rows=10, size_bytes=100, exec_stats=None, input_files=None
    )
    metadata_two = BlockMetadata(
        num_rows=10, size_bytes=10, exec_stats=None, input_files=None
    )

    bundle_one = RefBundle(
        blocks=[(block_ref_one, metadata_one), (block_ref_one, metadata_one)],
        owns_blocks=True,
        schema="schema",
        slices=[
            BlockSlice(start_offset=0, end_offset=1),
            BlockSlice(start_offset=1, end_offset=2),
        ],
    )
    bundle_two = RefBundle(
        blocks=[(block_ref_two, metadata_two), (block_ref_two, metadata_two)],
        owns_blocks=False,
        schema="schema",
        slices=[
            BlockSlice(start_offset=2, end_offset=3),
            BlockSlice(start_offset=3, end_offset=4),
        ],
    )

    merged = RefBundle.merge_ref_bundles([bundle_one, bundle_two])

    assert merged.schema == "schema"
    # The merged bundle should own the blocks if all input bundles own their blocks.
    # Since bundle_two doesn't own its blocks, the merged bundle should not own its
    # blocks.
    assert merged.owns_blocks is False
    assert len(merged.blocks) == 4
    assert merged.slices == (
        BlockSlice(start_offset=0, end_offset=1),
        BlockSlice(start_offset=1, end_offset=2),
        BlockSlice(start_offset=2, end_offset=3),
        BlockSlice(start_offset=3, end_offset=4),
    )


def test_ref_bundle_eq_and_hash():
    """Tests that `__eq__` and `__hash__` use field-based comparison, so that
    copies (e.g. from dataclasses.replace) are equal to the original and
    usable as dict keys."""

    from dataclasses import replace

    ref_a = ObjectRef(b"1" * 28)
    meta = BlockMetadata(num_rows=10, size_bytes=100, exec_stats=None, input_files=None)

    bundle = RefBundle(
        blocks=[(ref_a, meta)],
        owns_blocks=True,
        schema="schema",
        slices=[BlockSlice(start_offset=0, end_offset=5)],
        output_split_idx=1,
    )

    # A dataclasses.replace copy should be equal and have the same hash
    copy = replace(bundle)
    assert copy is not bundle
    assert copy == bundle
    assert hash(copy) == hash(bundle)

    # Equal bundles work as dict keys
    d = {bundle: "value"}
    assert d[copy] == "value"

    # Differing any field should break equality
    diff_bundle_1 = replace(bundle, owns_blocks=False)
    assert bundle != diff_bundle_1 and hash(bundle) != hash(diff_bundle_1)

    diff_bundle_2 = replace(bundle, output_split_idx=2)
    assert bundle != diff_bundle_2 and hash(bundle) != hash(diff_bundle_2)

    diff_bundle_3 = replace(bundle, schema="other")
    assert bundle != diff_bundle_3 and hash(bundle) != hash(diff_bundle_3)

    diff_bundle_4 = replace(bundle, slices=(None,))
    assert bundle != diff_bundle_4 and hash(bundle) != hash(diff_bundle_4)

    # Differing blocks or meta should not match too
    ref_b = ObjectRef(b"2" * 28)

    deff_block_ref_bundle = RefBundle(
        blocks=[(ref_b, meta)],
        owns_blocks=True,
        schema="schema",
        slices=[BlockSlice(start_offset=0, end_offset=5)],
        output_split_idx=1,
    )

    assert bundle != deff_block_ref_bundle

    diff_meta = replace(meta, num_rows=11)

    deff_block_meta_bundle = RefBundle(
        blocks=[(ref_a, diff_meta)],
        owns_blocks=True,
        schema="schema",
        slices=[BlockSlice(start_offset=0, end_offset=5)],
        output_split_idx=1,
    )

    assert bundle != deff_block_meta_bundle


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
