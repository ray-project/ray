from typing import TYPE_CHECKING, List, Union

try:
    import pyarrow
except ImportError:
    pyarrow = None

if TYPE_CHECKING:
    from ray.data.impl.sort import SortKeyT


def sort(table: "pyarrow.Table", key: "SortKeyT", descending: bool) -> "pyarrow.Table":
    import pyarrow.compute as pac

    indices = pac.sort_indices(table, sort_keys=key)
    return table.take(indices)


def take_table(
    table: "pyarrow.Table",
    indices: Union[List[int], "pyarrow.Array", "pyarrow.ChunkedArray"],
) -> "pyarrow.Table":
    """Select rows from the table.

    This method is an alternative to pyarrow.Table.take(), which breaks for
    extension arrays. This is exposed as a static method for easier use on
    intermediate tables, not underlying an ArrowBlockAccessor.
    """
    from ray.air.util.transform_pyarrow import (
        _is_column_extension_type,
        _concatenate_extension_column,
    )

    if any(_is_column_extension_type(col) for col in table.columns):
        new_cols = []
        for col in table.columns:
            if _is_column_extension_type(col):
                # .take() will concatenate internally, which currently breaks for
                # extension arrays.
                col = _concatenate_extension_column(col)
            new_cols.append(col.take(indices))
        table = pyarrow.Table.from_arrays(new_cols, schema=table.schema)
    else:
        table = table.take(indices)
    return table


def concat_and_sort(
    blocks: List["pyarrow.Table"], key: "SortKeyT", descending: bool
) -> "pyarrow.Table":
    ret = pyarrow.concat_tables(blocks, promote=True)
    indices = pyarrow.compute.sort_indices(ret, sort_keys=key)
    return take_table(ret, indices)
