"""Unit tests for Parquet predicate splitting optimization.

This module tests the _split_predicate_by_columns function which optimizes
predicate pushdown for partitioned Parquet datasets by splitting predicates
into data-column and partition-column parts.
"""

from dataclasses import dataclass
from typing import Optional, Set

import pytest

from ray.data._internal.datasource.parquet_datasource import (
    _split_predicate_by_columns,
)
from ray.data.expressions import Expr, col


@dataclass
class PredicateSplitTestCase:
    """Test case for predicate splitting."""

    predicate: Expr
    partition_cols: Set[str]
    expected_data_predicate: Optional[Expr]
    expected_partition_predicate: Optional[Expr]
    description: str


# fmt: off
TEST_CASES = [
    # ====================================================================
    # Pure data predicates - should push down everything
    # ====================================================================
    PredicateSplitTestCase(
        predicate=col("data1") > 5,
        partition_cols={"partition_col"},
        expected_data_predicate=col("data1") > 5,
        expected_partition_predicate=None,
        description="Simple data predicate",
    ),
    PredicateSplitTestCase(
        predicate=(col("data1") > 5) & (col("data2") == "x"),
        partition_cols={"partition_col"},
        expected_data_predicate=(col("data1") > 5) & (col("data2") == "x"),
        expected_partition_predicate=None,
        description="AND of data predicates",
    ),
    PredicateSplitTestCase(
        predicate=(col("data1") > 5) | (col("data2") == "x"),
        partition_cols={"partition_col"},
        expected_data_predicate=(col("data1") > 5) | (col("data2") == "x"),
        expected_partition_predicate=None,
        description="OR of data predicates",
    ),
    PredicateSplitTestCase(
        predicate=~(col("data1") > 5),
        partition_cols={"partition_col"},
        expected_data_predicate=~(col("data1") > 5),
        expected_partition_predicate=None,
        description="NOT of data predicate",
    ),
    PredicateSplitTestCase(
        predicate=col("data1").is_null(),
        partition_cols={"partition_col"},
        expected_data_predicate=col("data1").is_null(),
        expected_partition_predicate=None,
        description="IS_NULL of data column",
    ),
    PredicateSplitTestCase(
        predicate=col("data1").is_not_null(),
        partition_cols={"partition_col"},
        expected_data_predicate=col("data1").is_not_null(),
        expected_partition_predicate=None,
        description="IS_NOT_NULL of data column",
    ),
    PredicateSplitTestCase(
        predicate=((col("data1") > 5) & (col("data2") < 10)) | (col("data3") == "test"),
        partition_cols={"partition_col"},
        expected_data_predicate=((col("data1") > 5) & (col("data2") < 10)) | (col("data3") == "test"),
        expected_partition_predicate=None,
        description="Complex nested data predicates",
    ),
    # ====================================================================
    # Pure partition predicates - should enable pruning
    # ====================================================================
    PredicateSplitTestCase(
        predicate=col("partition_col") == "US",
        partition_cols={"partition_col"},
        expected_data_predicate=None,
        expected_partition_predicate=col("partition_col") == "US",
        description="Simple partition predicate",
    ),
    PredicateSplitTestCase(
        predicate=(col("partition1") == "US") & (col("partition2") == "2020"),
        partition_cols={"partition1", "partition2"},
        expected_data_predicate=None,
        expected_partition_predicate=(col("partition1") == "US") & (col("partition2") == "2020"),
        description="AND of partition predicates",
    ),
    PredicateSplitTestCase(
        predicate=(col("partition1") == "US") | (col("partition2") == "2020"),
        partition_cols={"partition1", "partition2"},
        expected_data_predicate=None,
        expected_partition_predicate=(col("partition1") == "US") | (col("partition2") == "2020"),
        description="OR of partition predicates",
    ),
    # ====================================================================
    # Mixed predicates with AND - should split both parts
    # ====================================================================
    PredicateSplitTestCase(
        predicate=(col("data1") > 5) & (col("partition_col") == "US"),
        partition_cols={"partition_col"},
        expected_data_predicate=col("data1") > 5,
        expected_partition_predicate=col("partition_col") == "US",
        description="Simple AND with data and partition",
    ),
    PredicateSplitTestCase(
        predicate=(col("partition_col") == "US") & (col("data1") > 5),
        partition_cols={"partition_col"},
        expected_data_predicate=col("data1") > 5,
        expected_partition_predicate=col("partition_col") == "US",
        description="Simple AND with partition and data (reversed order)",
    ),
    PredicateSplitTestCase(
        predicate=(col("data1") > 5) & (col("data2") < 10) & (col("partition_col") == "US"),
        partition_cols={"partition_col"},
        expected_data_predicate=(col("data1") > 5) & (col("data2") < 10),
        expected_partition_predicate=col("partition_col") == "US",
        description="Multiple data predicates AND partition",
    ),
    PredicateSplitTestCase(
        predicate=(col("data1") > 5) & (col("partition1") == "US") & (col("data2") < 10) & (col("partition2") == "2020"),
        partition_cols={"partition1", "partition2"},
        expected_data_predicate=(col("data1") > 5) & (col("data2") < 10),
        expected_partition_predicate=(col("partition1") == "US") & (col("partition2") == "2020"),
        description="Interleaved data and partition predicates",
    ),
    PredicateSplitTestCase(
        predicate=((col("data1") > 5) & (col("data2") < 10)) & (col("partition_col") == "US"),
        partition_cols={"partition_col"},
        expected_data_predicate=(col("data1") > 5) & (col("data2") < 10),
        expected_partition_predicate=col("partition_col") == "US",
        description="Nested AND of data predicates with partition",
    ),
    # ====================================================================
    # Mixed predicates with OR - CANNOT split safely
    # ====================================================================
    PredicateSplitTestCase(
        predicate=(col("data1") > 5) | (col("partition_col") == "US"),
        partition_cols={"partition_col"},
        expected_data_predicate=None,
        expected_partition_predicate=None,
        description="OR with data and partition (unsafe to split)",
    ),
    PredicateSplitTestCase(
        predicate=(col("partition_col") == "US") | (col("data1") > 5),
        partition_cols={"partition_col"},
        expected_data_predicate=None,
        expected_partition_predicate=None,
        description="OR with partition and data (unsafe to split)",
    ),
    PredicateSplitTestCase(
        predicate=((col("data1") > 5) & (col("data2") < 10)) | (col("partition_col") == "US"),
        partition_cols={"partition_col"},
        expected_data_predicate=None,
        expected_partition_predicate=None,
        description="OR with complex data predicate and partition",
    ),
    # ====================================================================
    # Mixed predicates with NOT - CANNOT split safely
    # ====================================================================
    PredicateSplitTestCase(
        predicate=~(col("partition_col") == "US"),
        partition_cols={"partition_col"},
        expected_data_predicate=None,
        expected_partition_predicate=~(col("partition_col") == "US"),
        description="NOT of partition predicate",
    ),
    PredicateSplitTestCase(
        predicate=~((col("data1") > 5) & (col("partition_col") == "US")),
        partition_cols={"partition_col"},
        expected_data_predicate=None,
        expected_partition_predicate=None,
        description="NOT of mixed AND (becomes OR via De Morgan, unsafe)",
    ),
    PredicateSplitTestCase(
        predicate=(col("data1") > 5) & ~(col("partition_col") == "US"),
        partition_cols={"partition_col"},
        expected_data_predicate=col("data1") > 5,
        expected_partition_predicate=~(col("partition_col") == "US"),
        description="AND with NOT of partition predicate (can extract both parts)",
    ),
    # ====================================================================
    # Complex nested scenarios
    # ====================================================================
    PredicateSplitTestCase(
        predicate=((col("data1") > 5) & (col("data2") < 10)) & ((col("data3") == "test") & (col("partition_col") == "US")),
        partition_cols={"partition_col"},
        expected_data_predicate=(col("data1") > 5) & (col("data2") < 10) & (col("data3") == "test"),
        expected_partition_predicate=col("partition_col") == "US",
        description="Deeply nested ANDs with mixed columns",
    ),
    PredicateSplitTestCase(
        predicate=((col("data1") > 5) | (col("data2") < 10)) & (col("partition_col") == "US"),
        partition_cols={"partition_col"},
        expected_data_predicate=(col("data1") > 5) | (col("data2") < 10),
        expected_partition_predicate=col("partition_col") == "US",
        description="AND of complex data predicate (with OR) and partition",
    ),
    PredicateSplitTestCase(
        predicate=(col("data1") > 5) & ((col("data2") < 10) & (col("partition_col") == "US")),
        partition_cols={"partition_col"},
        expected_data_predicate=(col("data1") > 5) & (col("data2") < 10),
        expected_partition_predicate=col("partition_col") == "US",
        description="Left-nested data with right-nested mixed",
    ),
    PredicateSplitTestCase(
        predicate=((col("data1") > 5) & (col("partition1") == "US")) & ((col("data2") < 10) & (col("partition2") == "2020")),
        partition_cols={"partition1", "partition2"},
        expected_data_predicate=(col("data1") > 5) & (col("data2") < 10),
        expected_partition_predicate=(col("partition1") == "US") & (col("partition2") == "2020"),
        description="Both sides have mixed predicates",
    ),
    PredicateSplitTestCase(
        predicate=((col("data1") > 5) | (col("partition_col") == "US")) & (col("data2") < 10),
        partition_cols={"partition_col"},
        expected_data_predicate=col("data2") < 10,
        expected_partition_predicate=None,
        description="AND with left side having OR with partition (can extract right side data)",
    ),
    # ====================================================================
    # Edge cases
    # ====================================================================
    PredicateSplitTestCase(
        predicate=(col("data1") > 5) & (col("data1") < 10),
        partition_cols={"partition_col"},
        expected_data_predicate=(col("data1") > 5) & (col("data1") < 10),
        expected_partition_predicate=None,
        description="Same column referenced multiple times (data)",
    ),
    PredicateSplitTestCase(
        predicate=(col("partition_col") == "US") & (col("partition_col") != "UK"),
        partition_cols={"partition_col"},
        expected_data_predicate=None,
        expected_partition_predicate=(col("partition_col") == "US") & (col("partition_col") != "UK"),
        description="Same partition column referenced multiple times",
    ),
    PredicateSplitTestCase(
        predicate=col("data1").is_null() & (col("partition_col") == "US"),
        partition_cols={"partition_col"},
        expected_data_predicate=col("data1").is_null(),
        expected_partition_predicate=col("partition_col") == "US",
        description="IS_NULL data predicate with partition",
    ),
    PredicateSplitTestCase(
        predicate=col("partition_col").is_null(),
        partition_cols={"partition_col"},
        expected_data_predicate=None,
        expected_partition_predicate=col("partition_col").is_null(),
        description="IS_NULL on partition column",
    ),
    # ====================================================================
    # No partition columns in dataset
    # ====================================================================
    PredicateSplitTestCase(
        predicate=col("data1") > 5,
        partition_cols=set(),
        expected_data_predicate=col("data1") > 5,
        expected_partition_predicate=None,
        description="No partition columns in dataset",
    ),
    PredicateSplitTestCase(
        predicate=(col("data1") > 5) & (col("data2") < 10),
        partition_cols=set(),
        expected_data_predicate=(col("data1") > 5) & (col("data2") < 10),
        expected_partition_predicate=None,
        description="Complex predicate with no partition columns",
    ),
    # ====================================================================
    # All columns are partition columns
    # ====================================================================
    PredicateSplitTestCase(
        predicate=col("partition1") > 5,
        partition_cols={"partition1", "partition2"},
        expected_data_predicate=None,
        expected_partition_predicate=col("partition1") > 5,
        description="All columns are partition columns",
    ),
]

def _assert_predicate_matches(
    actual: Optional[Expr], expected: Optional[Expr], pred_type: str, description: str
):
    """Helper to assert predicate matches expected value."""
    if expected is None:
        assert actual is None, (
            f"{description}: Expected no {pred_type} predicate (None), got {actual}"
        )
    else:
        assert actual is not None, f"{description}: Expected {pred_type} predicate, got None"


@pytest.mark.parametrize("test_case", TEST_CASES, ids=lambda tc: tc.description)
def test_split_predicate_by_columns(test_case: PredicateSplitTestCase):
    """Test predicate splitting for various scenarios.

    This test covers:
    - Pure data predicates (should extract data part only)
    - Pure partition predicates (should extract partition part only)
    - Mixed predicates with AND (should split both parts)
    - Mixed predicates with OR (can't split safely)
    - Mixed predicates with NOT (varies by case)
    - Complex nested scenarios
    - Edge cases
    """
    result = _split_predicate_by_columns(test_case.predicate, test_case.partition_cols)

    _assert_predicate_matches(
        result.data_predicate,
        test_case.expected_data_predicate,
        "data",
        test_case.description,
    )
    _assert_predicate_matches(
        result.partition_predicate,
        test_case.expected_partition_predicate,
        "partition",
        test_case.description,
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
