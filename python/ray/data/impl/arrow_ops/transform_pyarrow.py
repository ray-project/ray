from typing import List, TYPE_CHECKING
import pyarrow
import pyarrow.compute as pac

if TYPE_CHECKING:
    from ray.data.impl.sort import SortKeyT


def sort(table: "pyarrow.Table", key: "SortKeyT", descending: bool) -> "pyarrow.Table":
    indices = pac.sort_indices(table, sort_keys=key)
    return table.take(indices)


def concat_and_sort(
    blocks: List["pyarrow.Table"], key: "SortKeyT", descending: bool
) -> "pyarrow.Table":
    ret = pyarrow.concat_tables(blocks, promote=True)
    indices = pyarrow.compute.sort_indices(ret, sort_keys=key)
    return ret.take(indices)
