from dataclasses import replace

import ray
from ray.data._internal.execution.interfaces import ExecutionOptions
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


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
