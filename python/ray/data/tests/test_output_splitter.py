import collections
import itertools
import random

import pytest

import ray
from ray.data._internal.execution.interfaces import ExecutionOptions
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.output_splitter import OutputSplitter
from ray.data._internal.execution.util import make_ref_bundles
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
