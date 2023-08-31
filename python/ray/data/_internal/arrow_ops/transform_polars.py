from typing import TYPE_CHECKING, List

try:
    import pyarrow
except ImportError:
    pyarrow = None


if TYPE_CHECKING:
    from ray.data._internal.sort import SortKey

pl = None


def check_polars_installed():
    try:
        global pl
        import polars as pl
    except ImportError:
        raise ImportError(
            "polars not installed. Install with `pip install polars` or set "
            "`DataContext.use_polars = False` to fall back to pyarrow"
        )


def sort(table: "pyarrow.Table", sort_key: "SortKey") -> "pyarrow.Table":
    check_polars_installed()
    df = pl.from_arrow(table)
    return df.sort(sort_key.get_columns(), reverse=sort_key.get_descending()).to_arrow()


def concat_and_sort(
    blocks: List["pyarrow.Table"], sort_key: "SortKey"
) -> "pyarrow.Table":
    check_polars_installed()
    blocks = [pl.from_arrow(block) for block in blocks]
    df = pl.concat(blocks).sort(
        sort_key.get_columns(), reverse=sort_key.get_descending()
    )
    return df.to_arrow()
