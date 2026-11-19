"""Unit tests for the pushdown state carried by the ``Supports*`` mixins.

``SupportsColumnPruning`` and ``SupportsLimitPushdown`` hold the accepted
projection and limit and implement both of their methods, so every scanner
answers ``pruned_column_names`` / ``pushed_limit`` the same way. Planning
derives listing-time pruning from those answers, so a scanner that drifts from
them drops rows.
"""
from dataclasses import dataclass
from typing import Optional

import pyarrow as pa
import pytest

from ray.data._internal.datasource_v2.formats.parquet.parquet_scanner import (
    ParquetScanner,
)
from ray.data._internal.datasource_v2.interfaces.pushdown import (
    SupportsColumnPruning,
    SupportsLimitPushdown,
)
from ray.data._internal.datasource_v2.interfaces.reader import Reader
from ray.data._internal.datasource_v2.interfaces.scanner import Scanner

SCHEMA = pa.schema([("a", pa.int64()), ("b", pa.int64()), ("c", pa.int64())])


@dataclass(frozen=True)
class _BareScanner(Scanner, SupportsColumnPruning, SupportsLimitPushdown):
    """A scanner that declares the mixins and writes none of their bodies."""

    def read_schema(self) -> pa.Schema:
        return SCHEMA

    def create_reader(self) -> Reader:
        raise NotImplementedError


def _scanner(columns: Optional[tuple] = None, limit: Optional[int] = None):
    return ParquetScanner(schema=SCHEMA, columns=columns, limit=limit)


def test_mixins_alone_are_enough():
    """Mixing them in is the whole implementation: no overrides needed."""
    scanner = _BareScanner().prune_columns(["a"]).push_limit(5)

    assert scanner.pruned_column_names() == ("a",)
    assert scanner.pushed_limit() == 5


def test_unpruned_scanner_reports_no_projection():
    """``None`` means "every column", which is not the same as "no columns"."""
    assert _scanner().pruned_column_names() is None
    assert _scanner().pushed_limit() is None


def test_pruning_twice_intersects():
    """A second projection can only narrow the first, never re-widen it."""
    scanner = _scanner().prune_columns(["a", "b"]).prune_columns(["b", "c"])

    assert scanner.pruned_column_names() == ("b",)


def test_pruning_an_empty_projection_stays_empty():
    """``count()`` prunes to zero columns; the next prune must not undo that.

    The guard is ``self.columns is not None``, so an empty tuple -- a real
    state, not "unset" -- is intersected like any other.
    """
    scanner = _scanner().prune_columns([]).prune_columns(["a"])

    assert scanner.pruned_column_names() == ()


def test_pushed_limit_is_the_smallest_accepted():
    """Two limits leave the tighter one; reading more rows than it allows is
    what the pushdown exists to avoid."""
    assert _scanner().push_limit(10).push_limit(3).pushed_limit() == 3
    assert _scanner().push_limit(3).push_limit(10).pushed_limit() == 3


def test_pushdown_returns_a_new_scanner():
    """Scanners are immutable: pushing clones rather than mutating."""
    original = _scanner()
    pruned = original.prune_columns(["a"])

    assert original.pruned_column_names() is None
    assert pruned is not original


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
