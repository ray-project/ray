import numpy as np
import pyarrow as pa
import pytest

from ray.data._internal.planner.repartition_by_column import split_single_block

"""Unit tests for the repartition_by_column module.

The actual test to the dataset API is tested elsewhere.
"""


@pytest.fixture
def empty_block():
    return pa.Table.from_pydict({})


@pytest.fixture
def non_varied_block():
    return pa.Table.from_pydict(
        {
            "x": np.array([1, 1, 1]),
            "y": np.array([2, 2, 2]),
        }
    )


@pytest.fixture
def non_numeric_block():
    return pa.Table.from_pydict(
        {
            "x": np.array(["a", "b", "b"]),
            "y": np.array(["c", "d", "d"]),
        }
    )


@pytest.fixture
def missing_values_block():
    return pa.Table.from_pydict(
        {
            "x": np.array([1, np.nan, 2]),
            "y": np.array([2, 3, np.nan]),
        }
    )


@pytest.fixture
def single_row_block():
    return pa.Table.from_pydict(
        {
            "x": np.array([1]),
            "y": np.array([2]),
        }
    )


@pytest.fixture
def pyarrow_block_0():
    return pa.Table.from_pydict(
        {
            "x": np.array([1, 1, 1]),
            "y": np.array([2, 4, 6]),
        }
    )


@pytest.fixture
def pyarrow_block_1():
    return pa.Table.from_pydict(
        {
            "x": np.array([1, 1, 1, 2, 2, 2]),
            "y": np.array([2, 2, 3, 4, 4, 5]),
        }
    )


def test_split_single_block_single_block(pyarrow_block_0):
    keys_and_blocks = list(split_single_block(pyarrow_block_0, "x"))

    assert len(keys_and_blocks) == 1
    assert keys_and_blocks[0][0] == 1

    keys_and_blocks = list(split_single_block(pyarrow_block_0, ["x"]))

    assert len(keys_and_blocks) == 1


def test_split_single_block_2_blocks(pyarrow_block_1):
    keys_and_blocks = list(split_single_block(pyarrow_block_1, "x"))

    assert len(keys_and_blocks) == 2
    assert [k for k, _ in keys_and_blocks] == [1, 2]

    keys_and_blocks = list(split_single_block(pyarrow_block_1, ["x"]))

    assert len(keys_and_blocks) == 2
    assert [k for k, _ in keys_and_blocks] == [1, 2]


def test_split_single_block_2_blocks_2_keys(pyarrow_block_1):
    keys_and_blocks = list(split_single_block(pyarrow_block_1, ["x", "y"]))

    assert len(keys_and_blocks) == 4
    assert [k for k, _ in keys_and_blocks] == [(1, 2), (1, 3), (2, 4), (2, 5)]


def test_split_single_block_empty_block(empty_block):
    keys_and_blocks = list(split_single_block(empty_block, "x"))
    assert len(keys_and_blocks) == 0


def test_split_single_block_non_varied_block(non_varied_block):
    keys_and_blocks = list(split_single_block(non_varied_block, "x"))
    assert len(keys_and_blocks) == 1
    assert keys_and_blocks[0][0] == 1


def test_split_single_block_non_numeric_block(non_numeric_block):
    keys_and_blocks = list(split_single_block(non_numeric_block, "x"))
    assert len(keys_and_blocks) == 2
    assert [k for k, _ in keys_and_blocks] == ["a", "b"]


def test_split_single_block_missing_values_block(missing_values_block):
    keys_and_blocks = list(split_single_block(missing_values_block, "x"))
    assert (
        len(keys_and_blocks) == 3
    )  # Assuming the function handles NaN as a separate group
    # Assuming that the NaN group comes last, the keys should be 1, 2, and then NaN
    assert keys_and_blocks[0][0] == 1
    assert np.isnan(keys_and_blocks[1][0])
    assert keys_and_blocks[2][0] == 2


def test_split_single_block_single_row_block(single_row_block):
    keys_and_blocks = list(split_single_block(single_row_block, "x"))
    assert len(keys_and_blocks) == 1
    assert keys_and_blocks[0][0] == 1


def test_split_single_block_multiple_keys(non_numeric_block):
    keys_and_blocks = list(split_single_block(non_numeric_block, ["x", "y"]))
    assert len(keys_and_blocks) == 2
    assert [k for k, _ in keys_and_blocks] == [("a", "c"), ("b", "d")]
