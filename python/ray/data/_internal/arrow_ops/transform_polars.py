from typing import TYPE_CHECKING, List

try:
    import pyarrow
except ImportError:
    pyarrow = None


if TYPE_CHECKING:
    from ray.data._internal.sort import SortKeyT

pl = None


def check_polars_installed():
    try:
        global pl
        import polars as pl
    except ImportError:
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
