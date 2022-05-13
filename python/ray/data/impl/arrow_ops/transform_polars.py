from typing import List, TYPE_CHECKING

try:
    import pyarrow
except ImportError:
    pyarrow = None

try:
    import polars as pl
except ImportError:
    pl = None


if TYPE_CHECKING:
    from ray.data.impl.sort import SortKeyT


def check_polars_installed():
    if pl is None:
        raise ImportError(
            "polars not installed. Install with `pip install polars` or set "
            "`DatasetContext.use_polars = False` to fall back to pyarrow"
        )


def sort(table: "pyarrow.Table", key: "SortKeyT", descending: bool) -> "pyarrow.Table":
    check_polars_installed()
    col, _ = key[0]
    df = pl.from_arrow(table)
    return df.sort(col, reverse=descending).to_arrow()


def concat_and_sort(
    blocks: List["pyarrow.Table"], key: "SortKeyT", descending: bool
) -> "pyarrow.Table":
    check_polars_installed()
    col, _ = key[0]
    blocks = [pl.from_arrow(block) for block in blocks]
    df = pl.concat(blocks).sort(col, reverse=descending)
    return df.to_arrow()
