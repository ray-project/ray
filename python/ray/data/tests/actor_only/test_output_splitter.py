from ray.data._internal.execution.interfaces import ExecutionOptions
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.output_splitter import OutputSplitter
from ray.data._internal.execution.resource_bank import ResourceBank
from ray.data._internal.execution.util import make_ref_bundles
from ray.data.context import DataContext
from ray.data.tests.conftest import noop_counter
from ray.tests.conftest import *  # noqa


def test_split_from_buffer_updates_resource_bank(ray_start_regular_shared):
    """Mid-block splits create new ObjectRefs; register them and withdraw the original."""
    bundles = make_ref_bundles([[0, 1, 2]])
    input_op = InputDataBuffer(DataContext.get_current(), bundles)
    op = OutputSplitter(input_op, 2, equal=True, data_context=DataContext.get_current())
    bank = ResourceBank()
    op.start(ExecutionOptions(), noop_counter(), resource_bank=bank)

    original = input_op.get_next()
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
    for ref in new_refs:
        assert bank.live_block_refs[_ref_hex(ref)].op is op
    assert _total_blocks(bank.live_object_store(op=op)) == len(new_refs)
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
