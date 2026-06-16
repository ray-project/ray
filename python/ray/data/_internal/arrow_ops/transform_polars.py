from typing import TYPE_CHECKING, List, Optional

from packaging.version import parse as parse_version

try:
    import pyarrow
except ImportError:
    pyarrow = None


if TYPE_CHECKING:
    from ray.data._internal.planner.exchange.sort_task_spec import SortKey

pl = None

# Polars 0.16.8 introduced the `descending` parameter for the `sort` method,
# replacing `reverse`.
# See https://github.com/pola-rs/polars/issues/5429 for more details.
_POLARS_SORT_DESCENDING_MIN_VERSION = parse_version("0.16.8")


def check_polars_installed():
    try:
        global pl
        import polars as pl
    except ImportError:
        raise ImportError(
            "polars not installed. Install with `pip install polars` or set "
            "`DataContext.use_polars_sort = False` to fall back to pyarrow"
        )


def sort(table: "pyarrow.Table", sort_key: "SortKey") -> "pyarrow.Table":
    check_polars_installed()
    df = pl.from_arrow(table)
    if parse_version(pl.__version__) >= _POLARS_SORT_DESCENDING_MIN_VERSION:
        return df.sort(
            sort_key.get_columns(), descending=sort_key.get_descending()
        ).to_arrow()
    else:
        return df.sort(
            sort_key.get_columns(), reverse=sort_key.get_descending()
        ).to_arrow()


def concat_and_sort(
    blocks: List["pyarrow.Table"], sort_key: "SortKey", *, promote_types: bool = False
) -> "pyarrow.Table":
    check_polars_installed()
    blocks = [pl.from_arrow(block) for block in blocks]
    if parse_version(pl.__version__) >= _POLARS_SORT_DESCENDING_MIN_VERSION:
        df = pl.concat(blocks).sort(
            sort_key.get_columns(), descending=sort_key.get_descending()
        )
    else:
        df = pl.concat(blocks).sort(
            sort_key.get_columns(), reverse=sort_key.get_descending()
        )
    return df.to_arrow()


def join(
    left_table: "pyarrow.Table",
    right_table: "pyarrow.Table",
    *,
    join_type: str,
    left_on: List[str],
    right_on: List[str],
    left_suffix: Optional[str],
    right_suffix: Optional[str],
) -> "pyarrow.Table":
    check_polars_installed()

    left_df = pl.from_arrow(left_table)
    right_df = pl.from_arrow(right_table)

    returns_both_sides = join_type in (
        "inner",
        "left outer",
        "right outer",
        "full outer",
    )
    if returns_both_sides:
        left_cols = set(left_table.schema.names)
        right_output_cols = set(right_table.schema.names) - set(right_on)
        collisions = left_cols & right_output_cols

        if left_suffix:
            left_df = left_df.rename(
                {column: f"{column}{left_suffix}" for column in collisions}
            )

        if right_suffix:
            right_df = right_df.rename(
                {column: f"{column}{right_suffix}" for column in collisions}
            )

    if join_type == "left outer":
        how = "left"
    elif join_type == "right outer":
        how = "right"
    elif join_type == "full outer":
        how = "full"
    elif join_type in ("left semi", "right semi"):
        how = "semi"
    elif join_type in ("left anti", "right anti"):
        how = "anti"
    else:
        how = join_type

    if join_type in ("right semi", "right anti"):
        left_df, right_df = right_df, left_df
        left_on, right_on = right_on, left_on

    return left_df.join(
        right_df,
        left_on=left_on,
        right_on=right_on,
        how=how,
        coalesce=True,
    ).to_arrow()
