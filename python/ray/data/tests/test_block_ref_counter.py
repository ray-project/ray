"""Unit tests for BlockRefCounter."""
import pytest

from ray.data._internal.execution.block_ref_counter import BlockRefCounter


def make_ref():
    """Return a unique object that stands in for a ray.ObjectRef.

    The counter uses refs only as dict keys (no methods called on them),
    so a plain object() with identity-based hash is sufficient.
    """
    return object()


def make_op():
    """Return a unique object that stands in for a PhysicalOperator.

    The counter uses ops only as dict keys (no methods called on them),
    so a plain object() with identity-based hash is sufficient.
    """
    return object()


class TestBlockRefCounter:
    def setup_method(self):
        self.counter = BlockRefCounter()
        self.op = make_op()

    # ------------------------------------------------------------------
    # on_block_produced
    # ------------------------------------------------------------------

    def test_produced_registers_block(self):
        ref = make_ref()
        self.counter.on_block_produced(ref, 100, self.op)
        assert self.counter.get_object_store_memory_usage(self.op) == 100

    def test_produced_multiple_blocks_accumulate(self):
        ref1, ref2 = make_ref(), make_ref()
        self.counter.on_block_produced(ref1, 100, self.op)
        self.counter.on_block_produced(ref2, 200, self.op)
        assert self.counter.get_object_store_memory_usage(self.op) == 300

    def test_produced_multiple_ops_independent(self):
        op2 = make_op()
        ref1, ref2 = make_ref(), make_ref()
        self.counter.on_block_produced(ref1, 100, self.op)
        self.counter.on_block_produced(ref2, 250, op2)
        assert self.counter.get_object_store_memory_usage(self.op) == 100
        assert self.counter.get_object_store_memory_usage(op2) == 250

    def test_produced_duplicate_concurrent_dispatch(self):
        """Duplicate on_block_produced (same ref in two bundles, e.g.
        from_pandas_refs([ref, ref])) when both tasks run concurrently.

        Size is not double-counted. Block freed only after both tasks complete.
        """
        ref = make_ref()
        self.counter.on_block_produced(ref, 100, self.op)
        self.counter.on_block_produced(ref, 100, self.op)
        assert self.counter.get_object_store_memory_usage(self.op) == 100
        # Dispatch both bundles before either task completes.
        self.counter.on_block_dispatched_to_task(ref)
        self.counter.on_block_dispatched_to_task(ref)
        self.counter.on_task_completed(ref)
        assert self.counter.get_object_store_memory_usage(self.op) == 100  # still live
        self.counter.on_task_completed(ref)
        assert self.counter.get_object_store_memory_usage(self.op) == 0

    def test_produced_duplicate_sequential_dispatch(self):
        """Duplicate on_block_produced when task1 completes before task2 dispatches.

        The second bundle's queue hold must keep the block alive across the gap.
        """
        ref = make_ref()
        self.counter.on_block_produced(ref, 100, self.op)
        self.counter.on_block_produced(ref, 100, self.op)
        assert self.counter.get_object_store_memory_usage(self.op) == 100
        # Dispatch bundle1, complete task1, *then* dispatch bundle2.
        self.counter.on_block_dispatched_to_task(ref)
        self.counter.on_task_completed(ref)
        assert self.counter.get_object_store_memory_usage(self.op) == 100  # still live
        self.counter.on_block_dispatched_to_task(ref)
        self.counter.on_task_completed(ref)
        assert self.counter.get_object_store_memory_usage(self.op) == 0

    # ------------------------------------------------------------------
    # on_task_completed (simple path, no dispatch)
    # ------------------------------------------------------------------

    def test_completed_removes_block(self):
        ref = make_ref()
        self.counter.on_block_produced(ref, 100, self.op)
        self.counter.on_task_completed(ref)
        assert self.counter.get_object_store_memory_usage(self.op) == 0

    def test_completed_partial_removal(self):
        ref1, ref2 = make_ref(), make_ref()
        self.counter.on_block_produced(ref1, 100, self.op)
        self.counter.on_block_produced(ref2, 200, self.op)
        self.counter.on_task_completed(ref1)
        assert self.counter.get_object_store_memory_usage(self.op) == 200

    def test_completed_untracked_raises(self):
        ref = make_ref()
        with pytest.raises(AssertionError):
            self.counter.on_task_completed(ref)

    def test_double_completion_raises(self):
        ref = make_ref()
        self.counter.on_block_produced(ref, 100, self.op)
        self.counter.on_task_completed(ref)
        with pytest.raises(AssertionError, match="double completion"):
            self.counter.on_task_completed(ref)

    # ------------------------------------------------------------------
    # on_block_dispatched_to_task + on_task_completed
    # ------------------------------------------------------------------

    def test_first_dispatch_does_not_change_refcount(self):
        """First dispatch: ref_count stays at 1; block removed on completion."""
        ref = make_ref()
        self.counter.on_block_produced(ref, 100, self.op)
        self.counter.on_block_dispatched_to_task(ref)
        # Block is still live.
        assert self.counter.get_object_store_memory_usage(self.op) == 100
        self.counter.on_task_completed(ref)
        assert self.counter.get_object_store_memory_usage(self.op) == 0

    def test_strict_repartition_block_survives_first_completion(self):
        """Strict repartition: same block dispatched to two tasks.

        Block must stay live until BOTH tasks complete.
        """
        ref = make_ref()
        self.counter.on_block_produced(ref, 100, self.op)
        self.counter.on_block_dispatched_to_task(ref)  # first dispatch
        self.counter.on_block_dispatched_to_task(ref)  # second dispatch → ref_count = 2
        assert self.counter.get_object_store_memory_usage(self.op) == 100

        self.counter.on_task_completed(ref)  # first task done → ref_count = 1
        assert self.counter.get_object_store_memory_usage(self.op) == 100  # still live

        self.counter.on_task_completed(ref)  # second task done → ref_count = 0
        assert self.counter.get_object_store_memory_usage(self.op) == 0  # removed

    def test_strict_repartition_three_tasks(self):
        ref = make_ref()
        self.counter.on_block_produced(ref, 50, self.op)
        for _ in range(3):
            self.counter.on_block_dispatched_to_task(ref)
        # Three dispatches: first is no-op on ref_count, next two increment → ref_count = 3
        for remaining in [50, 50, 0]:
            self.counter.on_task_completed(ref)
            assert self.counter.get_object_store_memory_usage(self.op) == remaining

    def test_dispatch_untracked_raises(self):
        ref = make_ref()
        with pytest.raises(AssertionError):
            self.counter.on_block_dispatched_to_task(ref)

    # ------------------------------------------------------------------
    # get_object_store_memory_usage
    # ------------------------------------------------------------------

    def test_unknown_op_returns_zero(self):
        unknown_op = make_op()
        assert self.counter.get_object_store_memory_usage(unknown_op) == 0

    # ------------------------------------------------------------------
    # clear
    # ------------------------------------------------------------------

    def test_clear_resets_all_state(self):
        ref = make_ref()
        self.counter.on_block_produced(ref, 100, self.op)
        self.counter.clear()
        assert self.counter.get_object_store_memory_usage(self.op) == 0
        # After clear, the same ref can be re-registered without assertion errors.
        self.counter.on_block_produced(ref, 100, self.op)
        assert self.counter.get_object_store_memory_usage(self.op) == 100


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
