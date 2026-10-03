from typing import List, Union

import numpy as np
import pyarrow as pa

from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
def take_table(
    table: pa.Table,
    indices: Union[List[int], np.ndarray, pa.Array, pa.ChunkedArray],
) -> pa.Table:
    """Select rows from a PyArrow table, including Ray extension columns.

    Use this function to select rows from tables with Ray fixed-shape tensor,
    variable-shape tensor, or Python object extension columns, including columns
    with multiple chunks. Tables with ordinary Arrow columns are also supported.

    The result preserves the input schema and metadata and follows the order of
    ``indices``. Repeated indices produce repeated rows. An empty integer array
    produces an empty table. This function doesn't mutate ``table``.

    Examples:
        >>> import pyarrow as pa
        >>> from ray.data.extensions import take_table
        >>> table = pa.table({"value": [10, 20, 30]})
        >>> take_table(table, [2, 0, 2]).to_pydict()
        {'value': [30, 10, 30]}

    Args:
        table: Table to select rows from.
        indices: Zero-based, non-negative row indices less than the number of
            rows in ``table``. Accepts a list of integers, a one-dimensional
            NumPy integer array, a PyArrow integer array, or a PyArrow chunked
            integer array. For an empty selection, use an explicitly typed
            integer array, such as ``np.array([], dtype=np.int64)``.

    Returns:
        A table containing the selected rows.
    """
    # Avoid importing the Arrow transformation stack while extensions initialize.
    from ray.data._internal.arrow_ops.transform_pyarrow import (
        take_table as _take_table,
    )

    return _take_table(table, indices)
