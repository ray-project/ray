"""Tests for the pre-scheduling hint infrastructure.

Producer-side helpers, ``TaskContext`` staging field, and the wire envelope
(``BlockMetadataWithSchema``). No operator wiring is tested here — Download
and other consumers land in follow-on PRs.
"""

import pickle

import pytest

from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.scheduling_hints import (
    SchedulingHints,
    stage_memory_hint,
    stage_scheduling_hints,
)
from ray.data.block import BlockMetadata, BlockMetadataWithSchema


class TestSchedulingHints:
    def test_defaults_are_none(self):
        hints = SchedulingHints()
        assert hints.memory is None

    def test_equality(self):
        assert SchedulingHints(memory=10) == SchedulingHints(memory=10)
        assert SchedulingHints(memory=10) != SchedulingHints(memory=11)
        assert SchedulingHints() != SchedulingHints(memory=10)

    def test_is_hashable_and_frozen(self):
        # Frozen dataclasses are hashable, so hints can be used as dict keys
        # or set members if a consumer ever wants to dedupe forecasts.
        {SchedulingHints(memory=10): "ok"}
        with pytest.raises(Exception):
            SchedulingHints().memory = 5  # immutable

    def test_pickle_round_trip(self):
        hints = SchedulingHints(memory=1234)
        restored = pickle.loads(pickle.dumps(hints))
        assert restored == hints


class TestStageHelpers:
    def test_stage_scheduling_hints_writes_taskcontext(self):
        ctx = TaskContext(task_idx=0, op_name="t")
        with TaskContext.current(ctx):
            stage_scheduling_hints(SchedulingHints(memory=4096))
            assert ctx.next_block_scheduling_hints == SchedulingHints(memory=4096)

    def test_stage_scheduling_hints_no_context_is_noop(self):
        # Must not raise — direct unit-test callers may invoke a transform
        # without setting up a TaskContext.
        stage_scheduling_hints(SchedulingHints(memory=4096))

    def test_stage_memory_hint_is_equivalent_to_scheduling_hints(self):
        ctx = TaskContext(task_idx=0, op_name="t")
        with TaskContext.current(ctx):
            stage_memory_hint(99)
            assert ctx.next_block_scheduling_hints == SchedulingHints(memory=99)

    def test_stage_memory_hint_drops_nonpositive(self):
        ctx = TaskContext(task_idx=0, op_name="t")
        with TaskContext.current(ctx):
            stage_memory_hint(0)
            stage_memory_hint(-1)
            assert ctx.next_block_scheduling_hints is None

    def test_stage_memory_hint_no_context_is_noop(self):
        stage_memory_hint(99)  # must not raise


class TestTaskContextConsume:
    def test_consume_returns_and_clears(self):
        ctx = TaskContext(
            task_idx=0,
            op_name="t",
            next_block_scheduling_hints=SchedulingHints(memory=42),
        )
        assert ctx.consume_next_block_scheduling_hints() == SchedulingHints(memory=42)
        assert ctx.next_block_scheduling_hints is None
        # Second call: no stale carryover.
        assert ctx.consume_next_block_scheduling_hints() is None

    def test_consume_when_unset(self):
        ctx = TaskContext(task_idx=0, op_name="t")
        assert ctx.consume_next_block_scheduling_hints() is None


class TestBlockMetadataWithSchemaScheduling:
    """Verify the wire envelope round-trips the new field."""

    def test_default_is_none(self):
        bm = BlockMetadataWithSchema(num_rows=1, size_bytes=2, exec_stats=None)
        assert bm.scheduling_hints is None

    def test_from_metadata_threads_hints(self):
        meta = BlockMetadata(num_rows=1, size_bytes=2, exec_stats=None)
        bm = BlockMetadataWithSchema.from_metadata(
            meta, scheduling_hints=SchedulingHints(memory=512)
        )
        assert bm.scheduling_hints == SchedulingHints(memory=512)

    def test_metadata_property_does_not_expose_hints(self):
        # ``BlockMetadataWithSchema.metadata`` is the "block-state-only"
        # projection. Hints live on the envelope, not on the projection.
        bm = BlockMetadataWithSchema(
            num_rows=1,
            size_bytes=2,
            exec_stats=None,
            scheduling_hints=SchedulingHints(memory=512),
        )
        assert not hasattr(bm.metadata, "scheduling_hints")

    def test_pickle_round_trip_preserves_hints(self):
        bm = BlockMetadataWithSchema(
            num_rows=1,
            size_bytes=2,
            exec_stats=None,
            scheduling_hints=SchedulingHints(memory=512),
        )
        restored = pickle.loads(pickle.dumps(bm))
        assert restored.scheduling_hints == SchedulingHints(memory=512)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
