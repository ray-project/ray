from dataclasses import dataclass
from types import SimpleNamespace
from typing import Optional, Tuple, cast

import pytest

from ray.data._internal.logical.operators import Join, JoinSide
from ray.data._internal.logical.rules.predicate_pushdown import PredicatePushdown
from ray.data.expressions import Expr, col

# Left input produces ``a`` and ``b``; right input produces ``x`` and ``y``.
LEFT_COLUMNS = {"a": "a", "b": "b"}
RIGHT_COLUMNS = {"x": "x", "y": "y"}


@dataclass
class DerivationTestCase:
    """Test case for one-sided predicate derivation."""

    predicate: Expr
    expected_left: Optional[Expr]
    expected_right: Optional[Expr]
    description: str
    # ``(can push left, can push right)``, as the join type allows.
    allowed_sides: Tuple[bool, bool] = (True, True)


a, b, x, y = col("a"), col("b"), col("x"), col("y")

# fmt: off
TEST_CASES = [
    # ====================================================================
    # Predicates one side evaluates as is come back unchanged
    # ====================================================================
    DerivationTestCase(
        predicate=a == 1,
        expected_left=a == 1,
        expected_right=None,
        description="Left-only comparison",
    ),
    DerivationTestCase(
        predicate=x == 1,
        expected_left=None,
        expected_right=x == 1,
        description="Right-only comparison",
    ),
    DerivationTestCase(
        predicate=(a == 1) | (b == 2),
        expected_left=(a == 1) | (b == 2),
        expected_right=None,
        description="Left-only OR is accepted whole, not weakened",
    ),
    DerivationTestCase(
        predicate=a.is_null() & x.is_null(),
        expected_left=a.is_null(),
        expected_right=x.is_null(),
        description="Non-comparison leaves are accepted whole or not at all",
    ),
    # ====================================================================
    # AND keeps the conjuncts a side can use
    # ====================================================================
    DerivationTestCase(
        predicate=(a == 1) & (x == 1),
        expected_left=a == 1,
        expected_right=x == 1,
        description="AND splits per side",
    ),
    DerivationTestCase(
        predicate=(x == 1) & (y == 2),
        expected_left=None,
        expected_right=(x == 1) & (y == 2),
        description="AND with nothing for a side yields None, not an empty AND",
    ),
    DerivationTestCase(
        predicate=((a == 1) | (x == 1)) & (b == 2),
        expected_left=b == 2,
        expected_right=None,
        description="AND keeps the derivable conjunct when the OR yields nothing",
    ),
    # ====================================================================
    # OR needs every branch to yield something for the side
    # ====================================================================
    DerivationTestCase(
        predicate=((a == 1) & (x == 1)) | ((a == 2) & (x == 2)),
        expected_left=(a == 1) | (a == 2),
        expected_right=(x == 1) | (x == 2),
        description="OR of two-sided ANDs weakens per side (TPC-H q7 shape)",
    ),
    DerivationTestCase(
        predicate=((a == 1) & (x == 1)) | (x == 2),
        expected_left=None,
        expected_right=(x == 1) | (x == 2),
        description="OR branch with nothing for a side blocks that side",
    ),
    DerivationTestCase(
        predicate=a.is_null() | x.is_null(),
        expected_left=None,
        expected_right=None,
        description="OR over leaves of different sides derives nothing",
    ),
    DerivationTestCase(
        predicate=((a == 1) & (x == 1)) | ((a == 1) & (x == 2)),
        expected_left=a == 1,
        expected_right=(x == 1) | (x == 2),
        description="Identical OR branches collapse",
    ),
    DerivationTestCase(
        predicate=(((a == 1) & (x == 1)) | ((b == 1) & (y == 1))) & ((a == 3) | (x == 3)),
        expected_left=(a == 1) | (b == 1),
        expected_right=(x == 1) | (y == 1),
        description="Nested AND / OR / AND",
    ),
    # ====================================================================
    # Anything else is not weakened
    # ====================================================================
    DerivationTestCase(
        predicate=~((a == 1) & (x == 1)),
        expected_left=None,
        expected_right=None,
        description="NOT over a two-sided predicate",
    ),
    # ====================================================================
    # The join type decides which sides may be filtered early
    # ====================================================================
    DerivationTestCase(
        predicate=(a == 1) & (x == 1),
        expected_left=None,
        expected_right=x == 1,
        description="Protected side derives nothing from an AND",
        allowed_sides=(False, True),
    ),
    DerivationTestCase(
        predicate=((a == 1) & (x == 1)) | ((a == 2) & (x == 2)),
        expected_left=None,
        expected_right=(x == 1) | (x == 2),
        description="Protected side derives nothing from an OR",
        allowed_sides=(False, True),
    ),
]
# fmt: on


def _assert_predicate_matches(
    actual: Optional[Expr], expected: Optional[Expr], side: str, description: str
):
    if expected is None:
        assert (
            actual is None
        ), f"{description}: expected nothing for {side}, got {actual}"
    else:
        assert actual is not None, f"{description}: expected {expected} for {side}"
        assert actual.structurally_equals(
            expected
        ), f"{description}: {side} got {actual}, expected {expected}"


@pytest.mark.parametrize("test_case", TEST_CASES, ids=lambda tc: tc.description)
def test_derive_one_sided_predicate(test_case: DerivationTestCase):
    # Only ``allows_predicate_pushdown_to`` is consulted on the join.
    allowed = test_case.allowed_sides
    join = cast(
        Join, SimpleNamespace(allows_predicate_pushdown_to=lambda s: allowed[s.value])
    )
    output_to_input_by_side = (LEFT_COLUMNS, RIGHT_COLUMNS)

    for side, expected in (
        (JoinSide.LEFT, test_case.expected_left),
        (JoinSide.RIGHT, test_case.expected_right),
    ):
        actual = PredicatePushdown._derive_one_sided_predicate(
            test_case.predicate, join, output_to_input_by_side, side
        )
        _assert_predicate_matches(actual, expected, side.name, test_case.description)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
