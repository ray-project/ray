"""Unit tests for skipping Delta data files with log statistics."""

import datetime
import math
import random
import sys
from decimal import Decimal
from typing import List, Optional

import pyarrow as pa
import pytest

from ray.data._internal.datasource_v2.formats.delta.delta_file_pruning import (
    statistics_keep_mask,
)
from ray.data.expressions import Expr, col, lit


def _actions(**stats) -> pa.Table:
    """Add actions for three files, with the given ``min.``/``max.`` columns."""
    columns = {"path": ["f0", "f1", "f2"]}
    for name, values in stats.items():
        columns[name.replace("__", ".")] = values
    return pa.table(columns)


# Three files holding x in [0, 9], [10, 19] and [20, 29].
_INT_ACTIONS = _actions(min__x=[0, 10, 20], max__x=[9, 19, 29])


def _kept(actions: pa.Table, predicate: Expr) -> List[str]:
    keep = statistics_keep_mask(actions, predicate)
    if keep is None:
        return actions["path"].to_pylist()
    return actions.filter(keep)["path"].to_pylist()


@pytest.mark.parametrize(
    "predicate, expected",
    [
        (col("x") > 19, ["f2"]),
        (col("x") >= 19, ["f1", "f2"]),
        (col("x") < 10, ["f0"]),
        (col("x") <= 10, ["f0", "f1"]),
        (col("x") == 15, ["f1"]),
        (col("x") == 100, []),
        # The column on the right.
        (lit(19) < col("x"), ["f2"]),
        (lit(10) > col("x"), ["f0"]),
        (lit(15) == col("x"), ["f1"]),
        (col("x").is_in([5, 25]), ["f0", "f2"]),
        (col("x").is_in([100]), []),
        ((col("x") > 5) & (col("x") < 12), ["f0", "f1"]),
        ((col("x") < 5) | (col("x") > 25), ["f0", "f2"]),
        # Nothing a range can rule out.
        (col("x") != 15, ["f0", "f1", "f2"]),
        (~(col("x") > 19), ["f0", "f1", "f2"]),
        (col("x").is_null(), ["f0", "f1", "f2"]),
        (col("x").not_in([15]), ["f0", "f1", "f2"]),
        (col("x") > col("y"), ["f0", "f1", "f2"]),
        # An OR side that rules nothing out could match anywhere.
        ((col("x") > 25) | (col("x") != 3), ["f0", "f1", "f2"]),
        # An AND side that rules nothing out leaves the other side's answer.
        ((col("x") > 25) & (col("x") != 3), ["f2"]),
        # No statistics for the column.
        (col("missing") > 5, ["f0", "f1", "f2"]),
        (col("x") == lit(None), ["f0", "f1", "f2"]),
        (col("x").is_in([5, None]), ["f0", "f1", "f2"]),
    ],
    ids=str,
)
def test_int_statistics(predicate, expected):
    assert _kept(_INT_ACTIONS, predicate) == expected


def test_null_statistics_keep_the_file():
    """A file without statistics, or with an all-null column, keeps."""
    actions = _actions(min__x=[0, None, 20], max__x=[9, None, None])

    assert _kept(actions, col("x") > 25) == ["f1", "f2"]
    assert _kept(actions, col("x") < 5) == ["f0", "f1"]


def test_empty_add_actions():
    actions = pa.table(
        {"path": pa.array([], pa.string()), "min.x": pa.array([], pa.int64())}
    )

    assert statistics_keep_mask(actions, col("x") > 1) is None


def test_incomparable_literal_keeps_every_file():
    assert _kept(_INT_ACTIONS, col("x") > "abc") == ["f0", "f1", "f2"]


def test_string_statistics_use_only_the_minimum():
    """Writers truncate long strings: a truncated maximum isn't an upper bound,
    but a truncated minimum is still a lower bound."""
    actions = _actions(min__s=["a", "m", "x"], max__s=["c", "o", "z"])

    # Ruled out by the minimum.
    assert _kept(actions, col("s") < "m") == ["f0"]
    # The maximum would rule out f0 and f1, but isn't trusted.
    assert _kept(actions, col("s") > "y") == ["f0", "f1", "f2"]
    assert _kept(actions, col("s") == "b") == ["f0"]


def test_timestamp_statistics_are_not_used():
    """Delta stores timestamp statistics in milliseconds."""
    t = [datetime.datetime(2024, 1, d) for d in (1, 2, 3)]
    actions = _actions(
        min__ts=pa.array(t, pa.timestamp("us")),
        max__ts=pa.array(t, pa.timestamp("us")),
    )

    assert _kept(actions, col("ts") > t[1]) == ["f0", "f1", "f2"]


def _date(day: int) -> datetime.date:
    return datetime.date(2024, 1, day)


@pytest.mark.parametrize(
    "minimums, maximums, value",
    [
        (
            pa.array([_date(1), _date(11), _date(21)], pa.date32()),
            pa.array([_date(9), _date(19), _date(29)], pa.date32()),
            _date(15),
        ),
        (
            pa.array(
                [Decimal("0"), Decimal("10"), Decimal("20")], pa.decimal128(10, 2)
            ),
            pa.array(
                [Decimal("9"), Decimal("19"), Decimal("29")], pa.decimal128(10, 2)
            ),
            Decimal("15"),
        ),
        (pa.array([0.0, 10.0, 20.0]), pa.array([9.0, 19.0, 29.0]), 15.0),
        (pa.array([False, True, True]), pa.array([False, True, True]), True),
    ],
    ids=["date", "decimal", "float", "bool"],
)
def test_exact_statistic_types(minimums, maximums, value):
    actions = _actions(min__v=minimums, max__v=maximums)

    assert _kept(actions, col("v") == value) == (
        ["f1", "f2"] if value is True else ["f1"]
    )


def test_nan_bounds_keep_the_file():
    """A float column holding NaN can have NaN statistics."""
    actions = _actions(min__f=[0.0, math.nan, 20.0], max__f=[9.0, math.nan, 29.0])

    assert _kept(actions, col("f") > 25.0) == ["f1", "f2"]
    assert _kept(actions, col("f") < 5.0) == ["f0", "f1"]


def _matches(value: Optional[int], op: str, literal: int) -> bool:
    if value is None:
        return False
    return {
        ">": value > literal,
        ">=": value >= literal,
        "<": value < literal,
        "<=": value <= literal,
        "==": value == literal,
        "!=": value != literal,
    }[op]


def _predicate(op: str, literal: int) -> Expr:
    return {
        ">": col("x") > literal,
        ">=": col("x") >= literal,
        "<": col("x") < literal,
        "<=": col("x") <= literal,
        "==": col("x") == literal,
        "!=": col("x") != literal,
    }[op]


@pytest.mark.parametrize("seed", range(20))
def test_never_drops_a_file_with_a_matching_row(seed):
    """Random files and predicates: every file holding a matching row is kept."""
    rng = random.Random(seed)
    files = [
        [rng.choice([None, *range(-20, 20)]) for _ in range(rng.randint(1, 6))]
        for _ in range(12)
    ]

    def stat(values, fn):
        present = [v for v in values if v is not None]
        # Drop some statistics, as files written without them have none.
        return fn(present) if present and rng.random() > 0.2 else None

    actions = pa.table(
        {
            "path": [f"f{i}" for i in range(len(files))],
            "min.x": pa.array([stat(f, min) for f in files], pa.int64()),
            "max.x": pa.array([stat(f, max) for f in files], pa.int64()),
        }
    )
    for _ in range(30):
        op_a, op_b = rng.choice(">,>=,<,<=,==,!=".split(",")), rng.choice("><")
        a, b = rng.randint(-25, 25), rng.randint(-25, 25)
        combine = rng.choice(["and", "or", "single"])
        predicate = _predicate(op_a, a)
        if combine == "and":
            predicate = predicate & _predicate(op_b, b)
        elif combine == "or":
            predicate = predicate | _predicate(op_b, b)

        kept = set(_kept(actions, predicate))

        for i, values in enumerate(files):
            matching = any(
                _matches(v, op_a, a)
                if combine == "single"
                else (
                    _matches(v, op_a, a) and _matches(v, op_b, b)
                    if combine == "and"
                    else _matches(v, op_a, a) or _matches(v, op_b, b)
                )
                for v in values
            )
            if matching:
                assert f"f{i}" in kept, (predicate, values)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
