import random

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


def _num_rows(blocks):
    return [BlockAccessor.for_block(block).num_rows() for block in blocks]


def _make_block(block_format, values):
    if block_format == "arrow":
        return pa.table({"value": pa.array(values, type=pa.int64())})
    return pd.DataFrame({"value": pd.Series(values, dtype="int64")})


@pytest.mark.parametrize("block_format", ["arrow", "pandas"])
def test_row_sized_output_builds_buffer_once(monkeypatch, block_format):
    """Regression test for https://github.com/ray-project/ray/issues/65434."""
    original_build = DelegatingBlockBuilder.build
    build_calls = 0

    def build(builder, *args, **kwargs):
        nonlocal build_calls
        build_calls += 1
        return original_build(builder, *args, **kwargs)

    monkeypatch.setattr(DelegatingBlockBuilder, "build", build)

    buffer = BlockOutputBuffer(OutputBlockSizeOption.of(target_num_rows_per_block=1))
    buffer.add_block(_make_block(block_format, list(range(100))))
    buffer.finalize()
    output = list(buffer.iter_ready_blocks())

    assert build_calls == 1
    assert _num_rows(output) == [1] * 100
    assert _values(output) == list(range(100))


@pytest.mark.parametrize("block_format", ["arrow", "pandas"])
def test_row_sized_output_carries_partial_tail_across_blocks(block_format):
    buffer = BlockOutputBuffer(OutputBlockSizeOption.of(target_num_rows_per_block=4))
    output = []

    buffer.add_block(_make_block(block_format, list(range(0, 10))))
    output.extend(buffer.iter_ready_blocks())
    # 10 rows buffered: emit while more than 4 rows remain, keeping a 2-row tail.
    assert _num_rows(output) == [4, 4]

    buffer.add_block(_make_block(block_format, []))
    buffer.add_block(_make_block(block_format, list(range(10, 13))))
    output.extend(buffer.iter_ready_blocks())
    # The 2-row tail is completed with rows from the next block.
    assert _num_rows(output) == [4, 4, 4]

    buffer.add_block(_make_block(block_format, list(range(13, 14))))
    output.extend(buffer.iter_ready_blocks())
    assert _num_rows(output) == [4, 4, 4]

    buffer.finalize()
    output.extend(buffer.iter_ready_blocks())
    assert _num_rows(output) == [4, 4, 4, 2]
    assert _values(output) == list(range(14))


@pytest.mark.parametrize("block_format", ["arrow", "pandas"])
def test_row_sized_output_empty_input_yields_one_empty_block(block_format):
    buffer = BlockOutputBuffer(OutputBlockSizeOption.of(target_num_rows_per_block=3))
    buffer.add_block(_make_block(block_format, []))
    assert not buffer.has_next()

    buffer.finalize()
    output = list(buffer.iter_ready_blocks())
    assert _num_rows(output) == [0]


def _expected_row_sized_output(ops, target_num_rows):
    """Independent model of the row-only path of ``BlockOutputBuffer``.

    Before ``finalize()``, a block of ``target_num_rows`` rows is ready only while
    strictly more than ``target_num_rows`` rows are buffered. After it, the
    remaining rows are emitted in chunks of ``target_num_rows`` (the last one may
    be smaller), plus a single empty block if nothing was emitted at all.
    """
    pending = []
    output = []
    finalized = False

    def has_next():
        if finalized:
            return not output or len(pending) > 0
        return len(pending) > target_num_rows

    def next_block():
        num_rows = min(target_num_rows, len(pending))
        output.append(pending[:num_rows])
        del pending[:num_rows]

    for op, arg in ops:
        if op == "add":
            pending.extend(arg)
        elif op == "next":
            if has_next():
                next_block()
        elif op == "drain":
            while has_next():
                next_block()
        else:
            assert op == "finalize"
            finalized = True
            while has_next():
                next_block()
    return output


@pytest.mark.parametrize("block_format", ["arrow", "pandas"])
def test_row_sized_output_matches_expected_chunking(block_format):
    rng = random.Random(65434)
    for _ in range(300):
        target_num_rows = rng.randint(1, 8)
        ops = []
        next_value = 0
        for _ in range(rng.randint(0, 8)):
            num_rows = rng.choice([0, 1, rng.randint(1, 3 * target_num_rows)])
            ops.append(("add", list(range(next_value, next_value + num_rows))))
            next_value += num_rows
            ops.extend(
                (rng.choice(["next", "drain"]), None) for _ in range(rng.randint(0, 2))
            )
        ops.append(("finalize", None))

        buffer = BlockOutputBuffer(
            OutputBlockSizeOption.of(target_num_rows_per_block=target_num_rows)
        )
        output = []
        for op, arg in ops:
            if op == "add":
                buffer.add_block(_make_block(block_format, arg))
            elif op == "next":
                if buffer.has_next():
                    output.append(buffer.next())
            elif op == "drain":
                output.extend(buffer.iter_ready_blocks())
            else:
                buffer.finalize()
                output.extend(buffer.iter_ready_blocks())

        expected = _expected_row_sized_output(ops, target_num_rows)
        assert _num_rows(output) == [len(block) for block in expected], ops
        assert _values(output) == [value for block in expected for value in block]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
