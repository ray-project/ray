import collections
import itertools
import random
from unittest.mock import MagicMock

import pytest

import ray
from ray.data._internal.execution.block_ref_counter import BlockRefCounter
from ray.data._internal.execution.interfaces import ExecutionOptions, RefBundle
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.output_splitter import OutputSplitter
from ray.data._internal.execution.util import make_ref_bundles
from ray.data.block import BlockMetadata
from ray.data.context import DataContext
from ray.tests.conftest import *  # noqa


@pytest.mark.parametrize("equal", [False, True])
@pytest.mark.parametrize("chunk_size", [1, 10])
def test_split_operator(ray_start_regular_shared, equal, chunk_size):
    num_input_blocks = 100
    num_splits = 3
    # Add this many input blocks each time.
    # Make sure it is greater than num_splits * 2,
    # so we can test the output order of `OutputSplitter.get_next`.
    num_add_input_blocks = 10
    input_op = InputDataBuffer(
        DataContext.get_current(),
        make_ref_bundles([[i] * chunk_size for i in range(num_input_blocks)]),
    )
    op = OutputSplitter(
        input_op,
        num_splits,
        equal=equal,
        data_context=DataContext.get_current(),
    )

    # Feed data and implement streaming exec.
    output_splits = [[] for _ in range(num_splits)]
    op.start(ExecutionOptions())
    while input_op.has_next():
        for _ in range(num_add_input_blocks):
            if not input_op.has_next():
                break
            op.add_input(input_op.get_next(), 0)
        while op.has_next():
            ref = op.get_next()
            assert ref.owns_blocks, ref
            for block_ref in ref.block_refs:
                assert ref.output_split_idx is not None
                output_splits[ref.output_split_idx].extend(
                    list(ray.get(block_ref)["id"])
                )
    op.all_inputs_done()

    expected_splits = [[] for _ in range(num_splits)]
    for i in range(num_splits):
        for j in range(i, num_input_blocks, num_splits):
            expected_splits[i].extend([j] * chunk_size)
    if equal:
        min_len = min(len(expected_splits[i]) for i in range(num_splits))
        for i in range(num_splits):
            expected_splits[i] = expected_splits[i][:min_len]
    for i in range(num_splits):
        assert output_splits[i] == expected_splits[i], (
            output_splits[i],
            expected_splits[i],
        )


@pytest.mark.parametrize("equal", [False, True])
@pytest.mark.parametrize("random_seed", list(range(10)))
def test_split_operator_random(ray_start_regular_shared, equal, random_seed):
    random.seed(random_seed)
    inputs = make_ref_bundles([[i] * random.randint(0, 10) for i in range(100)])
    num_inputs = sum(x.num_rows() for x in inputs)
    input_op = InputDataBuffer(DataContext.get_current(), inputs)
    op = OutputSplitter(
        input_op, 3, equal=equal, data_context=DataContext.get_current()
    )

    # Feed data and implement streaming exec.
    output_splits = collections.defaultdict(list)
    op.start(ExecutionOptions())
    while input_op.has_next():
        op.add_input(input_op.get_next(), 0)
    op.all_inputs_done()
    while op.has_next():
        ref = op.get_next()
        assert ref.owns_blocks, ref
        for block_ref in ref.block_refs:
            output_splits[ref.output_split_idx].extend(list(ray.get(block_ref)["id"]))
    if equal:
        actual = [len(output_splits[i]) for i in range(3)]
        expected = [num_inputs // 3] * 3
        assert actual == expected
    else:
        assert sum(len(output_splits[i]) for i in range(3)) == num_inputs, output_splits


def test_split_operator_locality_hints(ray_start_regular_shared):
    input_op = InputDataBuffer(
        DataContext.get_current(), make_ref_bundles([[i] for i in range(10)])
    )
    op = OutputSplitter(
        input_op,
        2,
        equal=False,
        data_context=DataContext.get_current(),
        locality_hints=["node1", "node2"],
    )

    def get_fake_loc(item):
        assert isinstance(item, int), item
        if item in [0, 1, 4, 5, 8]:
            return "node1"
        else:
            return "node2"

    def get_bundle_loc(bundle):
        block = ray.get(bundle.blocks[0][0])
        fval = list(block["id"])[0]
        return [get_fake_loc(fval)]

    op._get_locations = get_bundle_loc

    # Feed data and implement streaming exec.
    output_splits = collections.defaultdict(list)
    op.start(ExecutionOptions(actor_locality_enabled=True))
    while input_op.has_next():
        op.add_input(input_op.get_next(), 0)
    op.all_inputs_done()
    while op.has_next():
        ref = op.get_next()
        assert ref.owns_blocks, ref
        for block_ref in ref.block_refs:
            output_splits[ref.output_split_idx].extend(list(ray.get(block_ref)["id"]))

    total = 0
    for i in range(2):
        if i == 0:
            node = "node1"
        else:
            node = "node2"
        split = output_splits[i]
        for item in split:
            assert get_fake_loc(item) == node
            total += 1

    assert total == 10, total
    assert "all objects local" in op.progress_str()


@pytest.mark.parametrize("equal", [False, True])
@pytest.mark.parametrize("random_seed", list(range(10)))
def test_split_operator_with_locality(ray_start_regular_shared, equal, random_seed):
    """Test locality-based dispatching with equal=True and equal=False modes.

    This test verifies that the OutputSplitter:
    1. Correctly buffers data to ensure equal distribution when equal=True
    2. Respects locality hints in both modes
    3. Yields blocks incrementally when locality is matched (streaming behavior)
    4. The fix ensures that _can_safely_dispatch correctly calculates remaining
       buffer requirements.
    """

    random.seed(random_seed)

    # Create bundles with varying sizes to test buffer management
    input_bundles = make_ref_bundles([[i] * random.randint(1, 10) for i in range(100)])
    num_inputs = sum(x.num_rows() for x in input_bundles)

    input_op = InputDataBuffer(DataContext.get_current(), input_bundles)
    op = OutputSplitter(
        input_op,
        3,
        equal=equal,
        data_context=DataContext.get_current(),
        locality_hints=["node0", "node1", "node2"],
    )

    # Mock locality function: distribute items across 3 nodes
    def _map_row_to_node(first_row_id_val) -> str:
        return f"node{first_row_id_val % 3}"

    def _get_fake_bundle_loc(bundle):
        block = ray.get(bundle.block_refs[0])
        first_row_id_val = block["id"][0]
        return [_map_row_to_node(first_row_id_val)]

    op._get_locations = _get_fake_bundle_loc

    # Feed data and implement streaming exec
    output_splits = [[] for _ in range(3)]
    yielded_incrementally = 0

    op.start(ExecutionOptions(actor_locality_enabled=True))
    while input_op.has_next():
        op.add_input(input_op.get_next(), 0)

        # Drain some outputs to simulate streaming consumption
        while op.has_next():
            yielded_incrementally += 1

            ref = op.get_next()

            assert ref.owns_blocks, ref

            for block_ref in ref.block_refs:
                output_splits[ref.output_split_idx].extend(
                    list(ray.get(block_ref)["id"])
                )

    op.all_inputs_done()

    # Collect remaining outputs
    while op.has_next():
        ref = op.get_next()

        assert ref.owns_blocks, ref

        for block_ref in ref.block_refs:
            output_splits[ref.output_split_idx].extend(list(ray.get(block_ref)["id"]))

    # Verify streaming behavior: outputs should be yielded before all inputs are done
    # With locality hints, we should see outputs during input phase
    assert yielded_incrementally > 0, (
        f"Expected incremental output with locality hints, but got 0 outputs during "
        f"{len(input_bundles)} input blocks. This suggests buffering all data instead of streaming."
    )

    # Verify equal distribution when equal=True
    if equal:
        actual = [len(output_splits[i]) for i in range(3)]
        expected = [num_inputs // 3] * 3
        assert (
            actual == expected
        ), f"Expected equal distribution {expected}, got {actual}"
    else:
        # In non-equal mode, verify all data is output with correct row IDs
        all_output_row_ids = set(itertools.chain.from_iterable(output_splits))

        # Reconstruct expected row IDs from the input bundles
        expected_row_ids = set()
        for b in input_bundles:
            id_col = ray.get(b.block_refs[0])["id"]
            expected_row_ids.update(list(id_col))

        assert all_output_row_ids == expected_row_ids

    # Verify locality was respected (most items should be on their preferred node)
    locality_hits = 0
    total = 0

    for split_idx in range(3):
        actual_node = f"node{split_idx}"

        for row_id in output_splits[split_idx]:
            total += 1
            expected_node = _map_row_to_node(row_id)

            assert expected_node in ["node0", "node1", "node2"], expected_node

            if expected_node == actual_node:
                locality_hits += 1

    # Should have excellent locality since bundles are dispatched based on locality hints.
    # With perfect locality we'd get 100%, but buffering for equal distribution and
    # occasional forced dispatches when buffer is full may cause some misses.
    # We expect at least 85% locality hit rate, which validates the feature is working.
    locality_ratio = locality_hits / total if total > 0 else 0

    # NOTE: 90% is an observed locality ratio that should be fixed for this test
    assert locality_ratio >= 0.85, (
        f"Locality ratio {locality_ratio:.2f} too low. "
        f"Expected >=85% with locality-aware dispatching. "
        f"Hits: {locality_hits}/{total}"
    )


class TestOutputSplitterBlockRefCounter:
    """Unit tests for BlockRefCounter integration in OutputSplitter.

    _track_bundle_split tests run without Ray (using a minimal stub instead of
    RefBundle, since the method only accesses .blocks).  The truncation test uses
    the ray_start_regular_shared fixture because all_inputs_done() calls into
    RefBundle internals that require real ObjectRefs.
    """

    class _FakeBundle:
        """Minimal stand-in for RefBundle used by _track_bundle_split.

        _track_bundle_split only iterates over .blocks, so a plain object with
        a blocks tuple is sufficient — no Ray ObjectRef validation occurs.
        """

        def __init__(self, blocks):
            self.blocks = tuple(blocks)

    def _fake_bundle(self, refs_with_sizes):
        """Return a _FakeBundle with (fake_ref, size_bytes) pairs.

        Needed because `RefBundle.__post_init__` validates the first block.
        Used only for ``_track_bundle_split`` tests.
        """
        blocks = [
            (
                ref,
                BlockMetadata(
                    num_rows=1, size_bytes=size, input_files=None, exec_stats=None
                ),
            )
            for ref, size in refs_with_sizes
        ]
        return self._FakeBundle(blocks)

    def _real_bundle(self, ray_refs_with_sizes):
        """Return a real RefBundle backed by actual ray.ObjectRefs."""
        blocks = [
            (
                ref,
                BlockMetadata(
                    num_rows=1, size_bytes=size, input_files=None, exec_stats=None
                ),
            )
            for ref, size in ray_refs_with_sizes
        ]
        return RefBundle(blocks, owns_blocks=True, schema=None)

    def _make_op(self, n=2, equal=True):
        input_op = InputDataBuffer(DataContext.get_current(), [])
        op = OutputSplitter(
            input_op, n=n, equal=equal, data_context=DataContext.get_current()
        )
        counter = BlockRefCounter()
        op._block_ref_counter = counter
        return op, counter

    # ------------------------------------------------------------------
    # _track_bundle_split  (no Ray required)
    # ------------------------------------------------------------------

    def test_track_bundle_split_all_pass_through(self):
        """All refs pass through to left/right unchanged — counter is unaffected."""
        op, counter = self._make_op()

        ref1, ref2 = object(), object()
        counter.on_block_produced(ref1, 100, op)
        counter.on_block_produced(ref2, 200, op)

        original = self._fake_bundle([(ref1, 100), (ref2, 200)])
        # ref1 goes entirely to left, ref2 entirely to right — no block physically split.
        left = self._fake_bundle([(ref1, 100)])
        right = self._fake_bundle([(ref2, 200)])

        op._track_bundle_split(original, left, right)

        # No new refs produced, no original refs consumed.
        assert counter.get_object_store_memory_usage(op) == 300

    def test_track_bundle_split_straddle(self):
        """A straddling block is replaced by two brand-new refs."""
        op, counter = self._make_op()

        original_ref = object()
        counter.on_block_produced(original_ref, 300, op)

        original = self._fake_bundle([(original_ref, 300)])
        # _split_block() produced two new ObjectRefs.
        new_left, new_right = object(), object()
        left = self._fake_bundle([(new_left, 120)])
        right = self._fake_bundle([(new_right, 180)])

        op._track_bundle_split(original, left, right)

        # original_ref consumed; new_left (120) and new_right (180) registered.
        assert counter.get_object_store_memory_usage(op) == 300

    def test_track_bundle_split_mixed(self):
        """One pass-through block and one straddling block in the same bundle."""
        op, counter = self._make_op()

        pass_ref = object()
        straddle_ref = object()
        counter.on_block_produced(pass_ref, 100, op)
        counter.on_block_produced(straddle_ref, 200, op)

        original = self._fake_bundle([(pass_ref, 100), (straddle_ref, 200)])
        # pass_ref goes intact to left; straddle_ref is physically split.
        new_left, new_right = object(), object()
        left = self._fake_bundle([(pass_ref, 100), (new_left, 80)])
        right = self._fake_bundle([(new_right, 120)])

        op._track_bundle_split(original, left, right)

        # straddle_ref (200) untracked; new_left (80) + new_right (120) registered;
        # pass_ref (100) unchanged.
        assert counter.get_object_store_memory_usage(op) == 300

    # ------------------------------------------------------------------
    # all_inputs_done truncation  (requires Ray for real ObjectRefs)
    # ------------------------------------------------------------------

    def test_all_inputs_done_truncates_remainder_blocks(self, ray_start_regular_shared):
        """Blocks discarded by equal-split truncation are untracked from the counter.

        With n=2, equal=True, and 3 single-row bundles:
          allocation = [0, 0] (num_output starts at 0)
          remainder  = 3 - 0 = 3
          x          = 3 // 2 = 1   → allocation = [1, 1]

        Two bundles are dispatched to the output queue; the third is truncated
        and must be untracked from the counter.
        """
        op, counter = self._make_op(n=2, equal=True)

        # Real ObjectRefs — content is irrelevant, we only use them as dict keys.
        ref1, ref2, ref3 = ray.put(None), ray.put(None), ray.put(None)
        counter.on_block_produced(ref1, 100, op)
        counter.on_block_produced(ref2, 200, op)
        counter.on_block_produced(ref3, 300, op)

        b1 = self._real_bundle([(ref1, 100)])
        b2 = self._real_bundle([(ref2, 200)])
        b3 = self._real_bundle([(ref3, 300)])

        # Populate the buffer directly, bypassing add_input metric tracking.
        op._buffer.add(b1)
        op._buffer.add(b2)
        op._buffer.add(b3)

        # Mock metrics so the dequeue/enqueue callbacks don't crash.
        op._metrics = MagicMock()

        op.all_inputs_done()

        # b3 (ref3=300) was truncated and must be untracked.
        # b1 (ref1=100) and b2 (ref2=200) remain live in the output queue.
        assert counter.get_object_store_memory_usage(op) == 300


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
