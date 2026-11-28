from dataclasses import replace
from typing import Collection

import pytest
from typing_extensions import override

import ray
from ray.data._internal.execution.interfaces import ExecutionOptions, RefBundle
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.output_splitter import OutputSplitter
from ray.data._internal.execution.resource_bank import ResourceBank
from ray.data._internal.execution.util import make_ref_bundles
from ray.data.context import DataContext
from ray.data.tests.conftest import noop_counter
from ray.tests.conftest import *  # noqa

# Distinct from the node the split task actually runs on.
_STALE_NODE = "not-the-worker"


def test_split_from_buffer_updates_resource_bank(ray_start_regular_shared):
    """Mid-block splits create new ObjectRefs; register them and withdraw the original.

    The new pieces are charged to the worker that created them. The source
    block's node_id is stale once the objects are new.
    """
    bundles = make_ref_bundles([[0, 1, 2]])
    input_op = InputDataBuffer(DataContext.get_current(), bundles)
    op = OutputSplitter(input_op, 2, equal=True, data_context=DataContext.get_current())
    bank = ResourceBank()
    op.start(ExecutionOptions(), noop_counter(), resource_bank=bank)

    original = input_op.get_next()
    original = replace(
        original,
        blocks=tuple(
            replace(entry, metadata=replace(entry.metadata, node_id=_STALE_NODE))
            for entry in original.blocks
        ),
    )
    original_refs = {entry.ref for entry in original.blocks}
    for entry in original.blocks:
        bank.on_new_output(op=input_op, ref=entry.ref, bm=entry.metadata)

    op.add_input(original, 0)
    # equal=True cannot dispatch a 3-row bundle across 2 splits without leftover.
    assert not op.has_next()
    result = op._split_from_buffer(1)
    bank.drain_consumed_blocks()

    total_rows = 0
    for bundle in result:
        rows = bundle.num_rows()
        assert rows is not None
        total_rows += rows
    assert total_rows == 1
    after_refs = {entry.ref for b in result for entry in b.blocks} | {
        entry.ref for b in op._buffer for entry in b.blocks
    }
    new_refs = after_refs - original_refs
    replaced = original_refs - after_refs
    live_keys = set(bank.live_block_refs)

    assert {_ref_hex(ref) for ref in new_refs} <= live_keys
    assert {_ref_hex(ref) for ref in replaced}.isdisjoint(live_keys)
    worker_node = ray.get_runtime_context().get_node_id()
    assert worker_node != _STALE_NODE
    for ref in new_refs:
        recorded = bank.live_block_refs[_ref_hex(ref)]
        assert recorded.op is op
        assert recorded.bm.node_id == worker_node
    assert _total_blocks(bank.live_object_store(op=op)) == len(new_refs)
    assert _total_blocks(bank.live_object_store(op=op, node_id=worker_node)) == len(
        new_refs
    )
    assert _total_blocks(bank.live_object_store(node_id=_STALE_NODE)) == 0
    assert _total_blocks(bank.live_object_store(op=input_op)) == 0


def _ref_hex(ref) -> str:
    # ray.types.ObjectRef is a documentation stub without hex(); the runtime
    # ObjectRef from make_ref_bundles has it.
    return ref.hex()  # pyrefly: ignore[missing-attribute]


def _total_blocks(usage) -> int:
    return (
        usage.num_pulled_output_blocks
        + usage.num_prebuffered_output_blocks
        + usage.num_input_blocks
    )


class _RemoteBundleSplitter(OutputSplitter):
    """Every bundle lives on a node no split prefers, so with locality hints
    dispatch has to wait for the buffer cap."""

    @staticmethod
    @override
    def _get_locations(bundle: RefBundle) -> Collection[str]:
        return ["elsewhere"]


@pytest.mark.parametrize(
    "n, equal, locality, preserve_order, required_bundles",
    [
        # No buffering requirement: the first bundle dispatches.
        (2, False, False, False, 0),
        # Locality buffer: no preferred bundle, so wait for 2n bundles.
        (2, False, True, False, 4),
        # Equal: dispatching a bundle needs n bundles' rows to rebalance.
        (3, True, False, False, 3),
        # Both: the larger requirement wins.
        (2, True, True, False, 4),
        # preserve_order disables the locality buffer.
        (2, False, True, True, 0),
    ],
)
def test_min_rows_needed_matches_dispatch_point(
    ray_start_regular_shared, n, equal, locality, preserve_order, required_bundles
):
    """The escape hatch treats a producer as starved while it has pulled fewer
    rows than this requirement, so the requirement must be exactly the row
    count at which the split first dispatches."""
    rows_per_bundle = 5
    bundles = make_ref_bundles(
        [list(range(i * rows_per_bundle, (i + 1) * rows_per_bundle)) for i in range(8)]
    )
    ctx = DataContext.get_current()
    op = _RemoteBundleSplitter(
        InputDataBuffer(ctx, bundles),
        n,
        equal=equal,
        data_context=ctx,
        locality_hints=[f"node{i}" for i in range(n)] if locality else None,
    )
    op.start(
        ExecutionOptions(preserve_order=preserve_order, actor_locality_enabled=True),
        noop_counter(),
    )

    # The requirement is in rows, sized from the buffered bundles.
    op.add_input(bundles[0], 0)
    assert op.min_num_rows_needed_to_make_progress() == (
        required_bundles * rows_per_bundle
    )

    dispatch_at = max(1, required_bundles)
    for num_fed, bundle in enumerate(bundles[1:dispatch_at], start=1):
        assert not op.has_next(), f"dispatched early after {num_fed} bundles"
        op.add_input(bundle, 0)
    assert op.has_next()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
