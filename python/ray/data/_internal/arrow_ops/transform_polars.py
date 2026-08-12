from typing import TYPE_CHECKING, List

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
