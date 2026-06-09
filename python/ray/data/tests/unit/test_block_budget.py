"""Unit tests for ray.data.block_budget and the BlockOutputBuffer budget path.

These tests are pure-Python and do NOT require a Ray cluster: they exercise the
budget classes and drive ``BlockOutputBuffer`` directly with in-memory blocks.
"""
import pickle

import pyarrow as pa
import pytest

from ray.data._internal.output_buffer import BlockOutputBuffer, OutputBlockSizeOption
from ray.data.block import BlockAccessor
from ray.data.block_budget import BlockBudget, ByteSize, RowCount, _parse_byte_size

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _DoublePerRow(BlockBudget):
    """A custom (non-builtin) budget: 2 units per row.

    Exercises the generic ``measure`` / ``boundary_row`` buffer path with a
    budget that is neither RowCount nor ByteSize -- i.e. the user extension
    point.
    """

    def __init__(self, limit: int):
        self._limit = limit

    @property
    def limit(self) -> int:
        return self._limit

    def measure(self, block) -> int:
        return 2 * block.num_rows()

    def boundary_row(self, block) -> int:
        return max(1, min(self._limit // 2, block.num_rows()))


def _drive(option, blocks):
    """Feed ``blocks`` through a BlockOutputBuffer; return the output blocks."""
    buf = BlockOutputBuffer(option)
    out = []
    for block in blocks:
        buf.add_block(block)
        while buf.has_next():
            out.append(buf.next())
    buf.finalize()
    while buf.has_next():
        out.append(buf.next())
    return out


def _rows(block):
    return BlockAccessor.for_block(block).num_rows()


# ---------------------------------------------------------------------------
# _parse_byte_size
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value,expected",
    [
        (1024, 1024),
        ("1024", 1024),
        ("128MiB", 128 * 1024**2),
        ("1GiB", 1024**3),
        ("1GB", 1000**3),
        ("500MB", 500 * 1000**2),
        ("1.5GiB", int(1.5 * 1024**3)),
        ("  64 KiB ", 64 * 1024),
    ],
)
def test_parse_byte_size(value, expected):
    assert _parse_byte_size(value) == expected


@pytest.mark.parametrize("bad", ["abc", "12Foo", "MiB", "", "-5MB"])
def test_parse_byte_size_invalid(bad):
    with pytest.raises(ValueError):
        _parse_byte_size(bad)


def test_parse_byte_size_rejects_bool():
    with pytest.raises(TypeError):
        _parse_byte_size(True)


# ---------------------------------------------------------------------------
# Construction / validation / repr / eq
# ---------------------------------------------------------------------------


def test_rowcount_validation():
    assert RowCount(5).limit == 5
    assert RowCount(5, strict=True).strict is True
    with pytest.raises(ValueError):
        RowCount(0)
    with pytest.raises(ValueError):
        RowCount(-1)


def test_bytesize_validation():
    assert ByteSize("1KiB").limit == 1024
    assert ByteSize(2048).limit == 2048
    with pytest.raises(ValueError):
        ByteSize(0)


def test_repr_and_eq():
    assert RowCount(5, strict=True) == RowCount(5, strict=True)
    assert RowCount(5) != RowCount(5, strict=True)
    assert ByteSize(2048) == ByteSize(2048)
    assert "strict=True" in repr(RowCount(5, strict=True))


# ---------------------------------------------------------------------------
# measure / boundary_row
# ---------------------------------------------------------------------------


def _acc(weights, column="w"):
    return BlockAccessor.for_block(pa.table({column: weights}))


def test_rowcount_measure_boundary():
    rc = RowCount(3)
    assert rc.measure(_acc([1, 1, 1, 1, 1])) == 5
    assert rc.boundary_row(_acc([1, 1, 1, 1, 1])) == 3  # cut to 3 rows
    assert rc.boundary_row(_acc([1, 1])) == 2  # fits, no cut


def test_bytesize_measure_boundary_hysteresis():
    bs = ByteSize(10**9)
    acc = _acc(list(range(100)))
    # Well below the 1.5x threshold -> no split (returns all rows).
    assert bs.boundary_row(acc) == acc.num_rows()
    assert bs.measure(acc) == acc.size_bytes()


# ---------------------------------------------------------------------------
# OutputBlockSizeOption
# ---------------------------------------------------------------------------


def test_option_conversions_and_properties():
    # Byte ceiling is intrinsic (not a budget).
    o = OutputBlockSizeOption.of(target_max_block_size=100)
    assert o.target_max_block_size == 100
    assert o.target_num_rows_per_block is None
    assert o.budgets == ()

    # target_num_rows_per_block lowers to a RowCount budget.
    o = OutputBlockSizeOption.of(target_num_rows_per_block=5)
    assert o.target_num_rows_per_block == 5
    assert len(o.budgets) == 1 and isinstance(o.budgets[0], RowCount)
    assert o.target_max_block_size is None

    # An explicit block_budget is carried as-is.
    o = OutputBlockSizeOption.of(block_budget=_DoublePerRow(8))
    assert len(o.budgets) == 1 and isinstance(o.budgets[0], _DoublePerRow)

    # Byte ceiling + a budget coexist (tightest wins downstream).
    o = OutputBlockSizeOption.of(target_max_block_size=1, target_num_rows_per_block=5)
    assert o.target_max_block_size == 1 and o.target_num_rows_per_block == 5

    assert OutputBlockSizeOption.of() is None
    assert OutputBlockSizeOption.of(disable_block_shaping=True).disable_block_shaping


# ---------------------------------------------------------------------------
# BlockOutputBuffer
# ---------------------------------------------------------------------------


def test_buffer_byte_ceiling_splits_large_block():
    # One large block, tiny byte target -> split into many; rows preserved.
    out = _drive(
        OutputBlockSizeOption.of(target_max_block_size=512),
        [pa.table({"x": list(range(1000))})],
    )
    assert len(out) > 1
    assert sum(_rows(b) for b in out) == 1000


def test_buffer_rowcount_budget_packs_to_n_rows():
    out = _drive(
        OutputBlockSizeOption.of(target_num_rows_per_block=10),
        [pa.table({"x": list(range(25))})],
    )
    rows = [_rows(b) for b in out]
    assert all(r == 10 for r in rows[:-1]) and sum(rows) == 25


def test_buffer_custom_budget_works():
    """A user-defined BlockBudget works end-to-end through the generic buffer.

    ``_DoublePerRow`` is neither RowCount nor ByteSize, so the buffer must drive
    it purely via the generic ``measure`` / ``boundary_row`` contract (no
    isinstance).
    """
    budget = _DoublePerRow(4)  # 2 units/row, limit 4 -> 2 rows per block
    out = _drive(
        OutputBlockSizeOption.of(block_budget=budget),
        [pa.table({"x": [i]}) for i in range(6)],
    )
    rows = [_rows(b) for b in out]
    assert all(r <= 2 for r in rows) and sum(rows) == 6


def test_buffer_row_inputs_reject_budgets():
    # Budgets are measured per block, so row/batch inputs are rejected.
    buf = BlockOutputBuffer(OutputBlockSizeOption.of(target_num_rows_per_block=2))
    with pytest.raises(AssertionError):
        buf.add({"x": 1})


def test_override_byte_ceiling_composes_with_budget():
    """Overriding the byte ceiling preserves an existing budget (orthogonal)."""
    from ray.data._internal.execution.operators.map_transformer import (
        BlockMapTransformFn,
    )

    fn = BlockMapTransformFn(
        lambda blocks, ctx: blocks,
        output_block_size_option=OutputBlockSizeOption.of(target_num_rows_per_block=5),
    )
    fn.override_target_max_block_size(1024)
    option = fn.output_block_size_option
    assert option.target_max_block_size == 1024  # ceiling applied
    assert option.target_num_rows_per_block == 5  # budget preserved


# ---------------------------------------------------------------------------
# Serialization (budgets are embedded in MapTransformer and shipped to tasks)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("budget", [RowCount(100, strict=True), ByteSize("128MiB")])
def test_budget_pickle_roundtrip(budget):
    assert pickle.loads(pickle.dumps(budget)) == budget


def test_option_with_budget_pickle_roundtrip():
    option = OutputBlockSizeOption.of(target_num_rows_per_block=5)
    restored = pickle.loads(pickle.dumps(option))
    assert isinstance(restored.budgets[0], RowCount)
    assert restored.budgets[0].limit == 5


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
