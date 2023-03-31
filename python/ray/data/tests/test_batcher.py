import pytest

import pyarrow as pa
import pandas as pd

from ray.data._internal.batcher import Batcher, ShufflingBatcher


def gen_block(num_rows: int, block_format: str = "pyarrow"):
    if block_format == "pyarrow":
        return pa.table({"foo": [1] * num_rows})
    elif block_format == "pandas":
        return pd.DataFrame({"foo": [1] * num_rows})
    elif block_format == "simple":
        return [1] * num_rows
    else:
        raise ValueError("No such block format: ", block_format)


@pytest.mark.parametrize("block_format", ["pyarrow", "pandas", "simple"])
@pytest.mark.parametrize("ensure_copy", [False, True])
def test_batcher(block_format, ensure_copy):
    # Test Batcher's batching logic, including when copies are made.
    batch_size = 5
    batcher = Batcher(batch_size=batch_size, ensure_copy=ensure_copy)

    def add_and_check(num_rows, expect_has_batch=False):
        block = gen_block(num_rows, block_format)
        assert batcher.can_add(block)

        before_metrics = batcher.get_metrics()
        before_num_rows_sliced = before_metrics.num_rows_sliced
        before_num_rows_concatenated = before_metrics.num_rows_concatenated
        before_num_rows_copied = before_metrics.num_rows_copied

        batcher.add(block)
        assert not expect_has_batch or batcher.has_batch()

        # Check that no slices, concatenations, or copies take place on block add.
        after_metrics = batcher.get_metrics()
        num_rows_sliced = after_metrics.num_rows_sliced - before_num_rows_sliced
        num_rows_concatenated = (
            after_metrics.num_rows_concatenated - before_num_rows_concatenated
        )
        num_rows_copied = after_metrics.num_rows_copied - before_num_rows_copied
        assert num_rows_sliced == 0
        assert num_rows_concatenated == 0
        assert num_rows_copied == 0

    def next_and_check(
        expected_batch_size=batch_size,
        should_have_batch_after=True,
        expected_num_rows_sliced=0,
        expected_num_rows_concatenated=0,
        expected_num_rows_copied=0,
    ):
        if expected_batch_size == batch_size:
            assert batcher.has_batch()
        else:
            assert batcher.has_any()
            assert batcher._done_adding
        before_metrics = batcher.get_metrics()
        before_num_rows_sliced = before_metrics.num_rows_sliced
        before_num_rows_concatenated = before_metrics.num_rows_concatenated
        before_num_rows_copied = before_metrics.num_rows_copied

        batch = batcher.next_batch()

        assert len(batch) == expected_batch_size

        if should_have_batch_after:
            assert batcher.has_batch()
        else:
            assert not batcher.has_batch()
        after_metrics = batcher.get_metrics()
        num_rows_sliced = after_metrics.num_rows_sliced - before_num_rows_sliced
        num_rows_concatenated = (
            after_metrics.num_rows_concatenated - before_num_rows_concatenated
        )
        num_rows_copied = after_metrics.num_rows_copied - before_num_rows_copied
        assert num_rows_sliced == expected_num_rows_sliced
        assert num_rows_concatenated == expected_num_rows_concatenated
        assert num_rows_copied == expected_num_rows_copied

    # The following adds blocks of sizes 2, 2, 3, 6, ..., 1, 4 to the Batcher, with
    # nexting interleaved at the ... point and with a batch_size=5 configured.

    # First two blocks do not yet make a batch.
    add_and_check(2)
    add_and_check(2)
    # This block puts us over the batch threshold.
    add_and_check(3, True)
    # Add another block.
    add_and_check(6, True)

    # Should have 13 rows (2 + 2 + 3 + 6) in the buffer at this point.
    # Get first batch, with more than a batch remaining.
    if block_format == "simple":
        # First 3 blocks are all sliced.
        expected_num_rows_sliced = 7
        # First 2 blocks and slice of third block are concatenated to create a batch.
        expected_num_rows_concatenated = batch_size
        # Copies:
        # - Slice copies.
        # - Add block (concatenation) copies.
        # - Final build copy.
        expected_num_rows_copied = (
            expected_num_rows_sliced + expected_num_rows_concatenated + batch_size
        )
    elif block_format == "pandas":
        # No extra slice/copy for ensure_copy=True, since Pandas concatenation will
        # already produce a copy.
        # First 3 blocks are all sliced.
        expected_num_rows_sliced = 7
        # First 2 blocks and slice of third block are concatenated to create a batch.
        expected_num_rows_concatenated = batch_size
        # Concatenation to create the batch.
        expected_num_rows_copied = expected_num_rows_concatenated
    else:
        # First 3 blocks are all sliced.
        expected_num_rows_sliced = 7
        if ensure_copy:
            # If ensure_copy, we slice-copy the batch before returning it.
            expected_num_rows_sliced += batch_size
        # First 2 blocks and slice of third block are concatenated to create a batch.
        expected_num_rows_concatenated = batch_size
        # Only copied if ensure_copy=True.
        expected_num_rows_copied = batch_size if ensure_copy else 0

    next_and_check(
        batch_size,
        True,
        expected_num_rows_sliced=expected_num_rows_sliced,
        expected_num_rows_concatenated=expected_num_rows_concatenated,
        expected_num_rows_copied=expected_num_rows_copied,
    )

    # Should have 8 rows (2 + 6) in the buffer at this point.
    # Get second batch, with no full batch remaining.
    if block_format == "simple":
        # Both (two) blocks are sliced.
        expected_num_rows_sliced = 8
        # First block and slice of second block are concatenated to create a batch.
        expected_num_rows_concatenated = batch_size
        # Copies:
        # - Slice copies.
        # - Add block (concatenation) copies.
        # - Final build copy.
        expected_num_rows_copied = (
            expected_num_rows_sliced + expected_num_rows_concatenated + batch_size
        )
    elif block_format == "pandas":
        # No extra slice/copy for ensure_copy=True, since Pandas concatenation will
        # already produce a copy.
        # Both blocks are all sliced.
        expected_num_rows_sliced = 8
        # First block and slice of second block are concatenated to create a batch.
        expected_num_rows_concatenated = batch_size
        # Concatenation to create the batch.
        expected_num_rows_copied = expected_num_rows_concatenated
    else:
        assert block_format == "pyarrow"
        # If ensure_copy=True, there will be an extra slice-copy.
        # Both blocks are all sliced.
        expected_num_rows_sliced = 8
        if ensure_copy:
            # If ensure_copy, we slice-copy the batch before returning it.
            expected_num_rows_sliced += batch_size
        # First block and slice of second block are concatenated to create a batch.
        expected_num_rows_concatenated = batch_size
        # Only copied if ensure_copy=True.
        expected_num_rows_copied = batch_size if ensure_copy else 0

    next_and_check(
        batch_size,
        False,
        expected_num_rows_sliced=expected_num_rows_sliced,
        expected_num_rows_concatenated=expected_num_rows_concatenated,
        expected_num_rows_copied=expected_num_rows_copied,
    )

    assert not batcher.has_batch()

    # Interleave block add with nexting.
    # NOTE: 3 rows carried over from last block adding run.
    add_and_check(1)
    add_and_check(4, True)

    # Indicate to the batcher that we're done adding blocks.
    batcher.done_adding()

    # Should have 8 rows (3 + 1 + 4) in the buffer at this point.
    # Get first batch, with no full batch remaining.
    if block_format == "simple":
        # All three blocks are fully sliced.
        expected_num_rows_sliced = 8
        # First 2 blocks and slice of third block are concatenated to create a batch.
        expected_num_rows_concatenated = batch_size
        # Copies:
        # - Slice copies.
        # - Add block (concatenation) copies.
        # - Final build copy.
        expected_num_rows_copied = (
            expected_num_rows_sliced + expected_num_rows_concatenated + batch_size
        )
    elif block_format == "pandas":
        # No extra slice/copy for ensure_copy=True, since Pandas concatenation will
        # already produce a copy.
        # First 3 blocks are all sliced.
        expected_num_rows_sliced = 8
        # First 2 blocks and slice of third block are concatenated to create a batch.
        expected_num_rows_concatenated = batch_size
        # Concatenation to create the batch.
        expected_num_rows_copied = expected_num_rows_concatenated
    else:
        assert block_format == "pyarrow"
        # If ensure_copy=True, there will be an extra slice-copy.
        # First 3 blocks are all sliced.
        expected_num_rows_sliced = 8
        if ensure_copy:
            # If ensure_copy, we slice-copy the batch before returning it.
            expected_num_rows_sliced += batch_size
        # First 2 blocks and slice of third block are concatenated to create a batch.
        expected_num_rows_concatenated = batch_size
        # Only copied if ensure_copy=True.
        expected_num_rows_copied = batch_size if ensure_copy else 0

    next_and_check(
        batch_size,
        False,
        expected_num_rows_sliced=expected_num_rows_sliced,
        expected_num_rows_concatenated=expected_num_rows_concatenated,
        expected_num_rows_copied=expected_num_rows_copied,
    )

    # Should have 3 rows in the buffer at this point.
    # Get remainder batch.
    if block_format == "simple":
        # Single block sliced.
        expected_num_rows_sliced = 3
        # Single block concatenated.
        expected_num_rows_concatenated = 3
        # Copies:
        # - Slice copies.
        # - Add block (concatenation) copies.
        # - Final build copy.
        expected_num_rows_copied = (
            expected_num_rows_sliced + expected_num_rows_concatenated + 3
        )
    else:
        if ensure_copy:
            # Ensuring copy will force an extra slice-copy.
            expected_num_rows_sliced = 6
            expected_num_rows_copied = 3
        else:
            expected_num_rows_sliced = 3
            expected_num_rows_copied = 0
        # No concatenation for a single remainder block.
        expected_num_rows_concatenated = 0

    next_and_check(
        3,
        False,
        expected_num_rows_sliced=expected_num_rows_sliced,
        expected_num_rows_concatenated=expected_num_rows_concatenated,
        expected_num_rows_copied=expected_num_rows_copied,
    )

    # Batcher should have no rows in its buffer at this point.
    assert not batcher.has_any()


def test_shuffling_batcher():
    batch_size = 5
    buffer_size = 20

    with pytest.raises(
        ValueError, match="Must specify a batch_size if using a local shuffle."
    ):
        ShufflingBatcher(batch_size=None, shuffle_buffer_min_size=buffer_size)

    # Should not raise error.
    ShufflingBatcher(batch_size=batch_size, shuffle_buffer_min_size=batch_size - 1)

    batcher = ShufflingBatcher(
        batch_size=batch_size,
        shuffle_buffer_min_size=buffer_size,
    )

    def add_and_check(num_rows, expect_has_batch=False, no_nexting_yet=True):
        block = gen_block(num_rows)
        assert batcher.can_add(block)
        batcher.add(block)
        assert not expect_has_batch or batcher.has_batch()

        if no_nexting_yet:
            # Check that no shuffle buffer has been materialized yet.
            assert batcher._shuffle_buffer is None
            assert batcher._shuffle_indices is None
            assert batcher._batch_head == 0

    def next_and_check(
        current_cursor,
        should_batch_be_full=True,
        should_have_batch_after=True,
        new_data_added=False,
    ):
        if should_batch_be_full:
            assert batcher.has_batch()
        else:
            assert batcher.has_any()
            assert batcher._done_adding
        if new_data_added:
            # If new data was added, the builder should be non-empty.
            assert batcher._builder.num_rows() > 0
        # Store the old shuffle indices for comparison in post.
        old_shuffle_indices = batcher._shuffle_indices

        batch = batcher.next_batch()

        if should_batch_be_full:
            assert len(batch) == batch_size

        # Check that shuffle buffer has been materialized and its state is as expected.
        assert batcher._shuffle_buffer is not None
        # Builder should always be empty after consuming a batch since the shuffle
        # buffer should always be materialized.
        assert batcher._builder.num_rows() == 0
        assert len(
            batcher._shuffle_indices
        ) == batcher._buffer_size() + current_cursor + len(batch)
        if new_data_added:
            # If new data was added, confirm that the old shuffle indices were
            # invalidated.
            assert batcher._shuffle_indices != old_shuffle_indices
        assert batcher._batch_head == current_cursor + len(batch)

        if should_have_batch_after:
            assert batcher.has_batch()
        else:
            assert not batcher.has_batch()

    # Add less than a batch.
    # Buffer not full and no batch slack.
    add_and_check(3)

    # Add to more than a batch (total=10).
    # Buffer not full and no batch slack.
    add_and_check(7)

    # Fill up to buffer (total=20).
    # Buffer is full but no batch slack.
    add_and_check(10)

    # Fill past buffer but not to full batch (total=22)
    # Buffer is over-full but no batch slack.
    add_and_check(2)

    # Fill past buffer to full batch (total=25).
    add_and_check(3, expect_has_batch=True)

    # Consume only available batch.
    next_and_check(0, should_have_batch_after=False, new_data_added=True)

    # Add 4 batches-worth to the already-full buffer.
    add_and_check(20, no_nexting_yet=False)

    # Consume 4 batches from the buffer.
    next_and_check(0, new_data_added=True)
    next_and_check(batch_size)
    next_and_check(2 * batch_size)
    next_and_check(3 * batch_size, should_have_batch_after=False)

    # Add a full batch + a partial batch to the buffer.
    add_and_check(8, no_nexting_yet=False)
    next_and_check(0, should_have_batch_after=False, new_data_added=True)

    # Indicate to the batcher that we're done adding blocks.
    batcher.done_adding()

    # Consume 4 full batches and one partial batch.
    next_and_check(batch_size)
    next_and_check(2 * batch_size)
    next_and_check(3 * batch_size)
    next_and_check(
        4 * batch_size,
        should_batch_be_full=False,
        should_have_batch_after=False,
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
