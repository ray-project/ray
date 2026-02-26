import pyarrow as pa

from ray.data._internal.dataset_repr import _build_dataset_ascii_repr_from_rows
from ray.data.dataset import Schema


def test_dataset_repr_from_rows_not_materialized():
    schema = Schema(pa.schema([("a", pa.int64()), ("b", pa.string())]))
    text = _build_dataset_ascii_repr_from_rows(
        schema=schema,
        num_rows=5,
        dataset_name="test_ds",
        is_materialized=False,
        head_rows=[],
        tail_rows=[],
    )
    assert text == (
        "name: test_ds\n"
        "shape: (5, 2)\n"
        "╭───────┬────────╮\n"
        "│ a     ┆ b      │\n"
        "│ ---   ┆ ---    │\n"
        "│ int64 ┆ string │\n"
        "╰───────┴────────╯\n"
        "(Dataset isn't materialized)"
    )


def test_dataset_repr_from_rows_gap():
    schema = Schema(pa.schema([("id", pa.int64())]))
    text = _build_dataset_ascii_repr_from_rows(
        schema=schema,
        num_rows=12,
        dataset_name=None,
        is_materialized=True,
        head_rows=[["0"], ["1"]],
        tail_rows=[["10"], ["11"]],
    )
    assert text == (
        "shape: (12, 1)\n"
        "╭───────╮\n"
        "│ id    │\n"
        "│ ---   │\n"
        "│ int64 │\n"
        "╞═══════╡\n"
        "│ 0     │\n"
        "│ 1     │\n"
        "│ …     │\n"
        "│ 10    │\n"
        "│ 11    │\n"
        "╰───────╯\n"
        "(Showing 4 of 12 rows)"
    )


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
