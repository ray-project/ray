import pytest

import pyarrow as pa

from ray.data._internal.batcher import ShufflingBatcher


def gen_block(num_rows):
    return pa.table({"foo": [1] * num_rows})


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
            batcher.has_any()
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
