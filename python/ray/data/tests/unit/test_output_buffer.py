import pandas as pd
import pyarrow as pa
import pytest

from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.output_buffer import BlockOutputBuffer, OutputBlockSizeOption
from ray.data.block import BlockAccessor


def _values(blocks):
    return [
        row["value"]
        for block in blocks
        for row in BlockAccessor.for_block(block).iter_rows(public_row_format=False)
    ]


@pytest.mark.parametrize(
    "block",
    [
        pa.table({"value": range(100)}),
        pd.DataFrame({"value": range(100)}),
    ],
    ids=["arrow", "pandas"],
)
def test_row_slicing_builds_buffer_once(monkeypatch, block):
    original_build = DelegatingBlockBuilder.build
    build_calls = 0

    def build(builder):
        nonlocal build_calls
        build_calls += 1
        return original_build(builder)

    monkeypatch.setattr(DelegatingBlockBuilder, "build", build)

    buffer = BlockOutputBuffer(OutputBlockSizeOption.of(target_num_rows_per_block=1))
    buffer.add_block(block)
    output = list(buffer.iter_ready_blocks())
    buffer.finalize()
    output.extend(buffer.iter_ready_blocks())

    assert build_calls == 1
    assert [BlockAccessor.for_block(block).num_rows() for block in output] == [1] * 100
    assert _values(output) == list(range(100))


def test_row_slicing_combines_partial_blocks():
    buffer = BlockOutputBuffer(OutputBlockSizeOption.of(target_num_rows_per_block=3))

    buffer.add_block(pa.table({"value": range(4)}))
    output = list(buffer.iter_ready_blocks())

    buffer.add_block(pa.table({"value": range(4, 6)}))
    output.extend(buffer.iter_ready_blocks())

    buffer.add_block(pa.table({"value": [6]}))
    output.extend(buffer.iter_ready_blocks())

    buffer.finalize()
    output.extend(buffer.iter_ready_blocks())

    assert [BlockAccessor.for_block(block).num_rows() for block in output] == [3, 3, 1]
    assert _values(output) == list(range(7))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
