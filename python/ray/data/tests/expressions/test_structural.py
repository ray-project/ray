"""Unit tests for expression structural equality."""

import pytest

from ray.data.expressions import col


def test_alias_structurally_equals_respects_rename_flag():
    expr = col("a")
    aliased = expr.alias("b")
    renamed = expr._rename("b")

    assert aliased.structurally_equals(aliased)
    assert renamed.structurally_equals(renamed)
    assert not aliased.structurally_equals(renamed)
    assert not aliased.structurally_equals(expr.alias("c"))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
