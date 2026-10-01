"""Tests for ray.data._internal.block_batching.interfaces."""

import pytest

from ray.data._internal.block_batching.interfaces import (
    Batch,
    BatchMetadata,
    BatchStageTimings,
    BlockStageTimings,
)
from ray.data._internal.stats import IterationStage, TimeSpan


class TestAccumulateBlockTimings:
    """Tests for BatchStageTimings.accumulate_block_timings().

    accumulate_block_timings appends each block's spans to the batch's lists
    (no merging) so that overlap attribution can sum non-overlapping spans
    without double-counting.
    """

    def test_single_block(self):
        """Accumulating a single block appends its span."""
        dst = BatchStageTimings()
        dst.accumulate_block_timings(
            BlockStageTimings(
                production_wait=TimeSpan(start_s=1.0, end_s=2.0),
                data_transfer=TimeSpan(start_s=2.0, end_s=3.0),
            )
        )
        assert len(dst.production_wait) == 1
        assert dst.production_wait[0].start_s == 1.0
        assert dst.production_wait[0].end_s == 2.0

    def test_multiple_blocks_kept_separate(self):
        """Multiple blocks produce a list of separate spans (no merge)."""
        dst = BatchStageTimings()

        dst.accumulate_block_timings(
            BlockStageTimings(
                production_wait=TimeSpan(start_s=1.0, end_s=2.0),
                data_transfer=TimeSpan(start_s=2.0, end_s=3.0),
            )
        )
        dst.accumulate_block_timings(
            BlockStageTimings(
                production_wait=TimeSpan(start_s=3.0, end_s=4.0),
                data_transfer=TimeSpan(start_s=4.0, end_s=5.0),
            )
        )
        dst.accumulate_block_timings(
            BlockStageTimings(
                production_wait=TimeSpan(start_s=5.0, end_s=6.0),
                data_transfer=TimeSpan(start_s=6.0, end_s=7.0),
            )
        )

        assert len(dst.production_wait) == 3
        assert [s.start_s for s in dst.production_wait] == [1.0, 3.0, 5.0]
        assert [s.end_s for s in dst.production_wait] == [2.0, 4.0, 6.0]

    def test_overlapping_blocks_kept_separate(self):
        """Overlapping windows are NOT merged — kept as separate spans."""
        dst = BatchStageTimings()

        dst.accumulate_block_timings(
            BlockStageTimings(
                production_wait=TimeSpan(start_s=1.0, end_s=5.0),
                data_transfer=TimeSpan(start_s=5.0, end_s=6.0),
            )
        )
        dst.accumulate_block_timings(
            BlockStageTimings(
                production_wait=TimeSpan(start_s=3.0, end_s=7.0),
                data_transfer=TimeSpan(start_s=7.0, end_s=8.0),
            )
        )

        assert len(dst.production_wait) == 2
        assert dst.production_wait[0].start_s == 1.0
        assert dst.production_wait[1].end_s == 7.0

    def test_into_empty_destination(self):
        """Accumulating into an empty BatchStageTimings appends the span."""
        dst = BatchStageTimings()
        dst.accumulate_block_timings(
            BlockStageTimings(
                production_wait=TimeSpan(start_s=10.0, end_s=20.0),
                data_transfer=TimeSpan(start_s=20.0, end_s=30.0),
            )
        )
        assert len(dst.production_wait) == 1
        assert dst.production_wait[0].start_s == 10.0

    def test_data_transfer_multiple_blocks(self):
        """data_transfer spans are kept separate across multiple blocks."""
        dst = BatchStageTimings()

        dst.accumulate_block_timings(
            BlockStageTimings(
                production_wait=TimeSpan(start_s=0.0, end_s=1.0),
                data_transfer=TimeSpan(start_s=1.0, end_s=2.0),
            )
        )
        dst.accumulate_block_timings(
            BlockStageTimings(
                production_wait=TimeSpan(start_s=2.0, end_s=3.0),
                data_transfer=TimeSpan(start_s=3.0, end_s=4.0),
            )
        )

        assert len(dst.data_transfer) == 2
        assert [s.start_s for s in dst.data_transfer] == [1.0, 3.0]

    def test_both_stages_independent(self):
        """production_wait and data_transfer lists are independent."""
        dst = BatchStageTimings()

        # Block 1: prod [1,2], xfer [2,3]
        dst.accumulate_block_timings(
            BlockStageTimings(
                production_wait=TimeSpan(start_s=1.0, end_s=2.0),
                data_transfer=TimeSpan(start_s=2.0, end_s=3.0),
            )
        )
        # Block 2: prod [5,6], xfer [6,7]
        dst.accumulate_block_timings(
            BlockStageTimings(
                production_wait=TimeSpan(start_s=5.0, end_s=6.0),
                data_transfer=TimeSpan(start_s=6.0, end_s=7.0),
            )
        )

        assert len(dst.production_wait) == 2
        assert len(dst.data_transfer) == 2
        assert [s.start_s for s in dst.production_wait] == [1.0, 5.0]
        assert [s.start_s for s in dst.data_transfer] == [2.0, 6.0]


class TestStageTimingsFields:
    """Tests that BatchStageTimings fields are accessible via stages()."""

    def test_batch_carries_timings_through_pipeline(self):
        """A Batch's metadata.stage_timings carries all stage windows."""
        timings = BatchStageTimings()
        timings.production_wait.append(TimeSpan(start_s=1.0, end_s=2.0))
        timings.batching = TimeSpan(start_s=2.0, end_s=3.0)
        timings.format = TimeSpan(start_s=3.0, end_s=4.0)
        timings.collate = TimeSpan(start_s=4.0, end_s=5.0)
        timings.finalize = TimeSpan(start_s=5.0, end_s=6.0)

        batch = Batch(
            BatchMetadata(batch_idx=0, num_rows=50, stage_timings=timings), None
        )

        # Verify all stages are accessible via stages() iterator
        stage_dict = dict(batch.metadata.stage_timings.stages())
        assert len(stage_dict) == 6
        assert stage_dict[IterationStage.PRODUCTION_WAIT][0].start_s == 1.0
        assert stage_dict[IterationStage.BATCHING][0].end_s == 3.0
        assert stage_dict[IterationStage.FORMAT][0].start_s == 3.0
        assert stage_dict[IterationStage.COLLATE][0].end_s == 5.0
        assert stage_dict[IterationStage.FINALIZE][0].start_s == 5.0
        assert batch.metadata.num_rows == 50


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
