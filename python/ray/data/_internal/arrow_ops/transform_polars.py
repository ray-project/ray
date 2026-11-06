from typing import TYPE_CHECKING, List

import pyarrow
from packaging.version import parse as parse_version

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


def _sort_dataframe(df, sort_key: "SortKey"):
    # Keep nulls at the end for both ascending and descending sorts, matching
    # the behavior of the PyArrow sort implementation.
    if parse_version(pl.__version__) >= _POLARS_SORT_DESCENDING_MIN_VERSION:
        return df.sort(
            sort_key.get_columns(),
            descending=sort_key.get_descending(),
            nulls_last=True,
        )
    else:
        return df.sort(
            sort_key.get_columns(),
            reverse=sort_key.get_descending(),
            nulls_last=True,
        )


def sort(table: "pyarrow.Table", sort_key: "SortKey") -> "pyarrow.Table":
    check_polars_installed()
    df = pl.from_arrow(table)
    return _sort_dataframe(df, sort_key).to_arrow()


def concat_and_sort(
    blocks: List["pyarrow.Table"], sort_key: "SortKey", *, promote_types: bool = False
) -> "pyarrow.Table":
    check_polars_installed()
    blocks = [pl.from_arrow(block) for block in blocks]
    df = _sort_dataframe(pl.concat(blocks), sort_key)
    return df.to_arrow()
