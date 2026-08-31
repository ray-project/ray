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

    Use this function instead of :meth:`pyarrow.Table.take` when a table may
    contain Ray extension columns, such as tensor columns. The output preserves
    the input schema and follows the order of ``indices``; repeated indices
    produce repeated rows.

    This function doesn't mutate ``table``. Unsupported or invalid indices use
    PyArrow's standard validation and exception behavior.

    Examples:
        .. testcode::

            import pyarrow as pa

            from ray.data.extensions import take_table

            table = pa.table({"value": [10, 20, 30]})
            result = take_table(table, [2, 0, 2])
            print(result.to_pydict())

        .. testoutput::

            {'value': [30, 10, 30]}

    Args:
        table: Table to select rows from.
        indices: Zero-based row indices. This can be a list of integers, a
            one-dimensional NumPy integer array, a PyArrow array, or a PyArrow
            chunked array.

    Returns:
        A table containing the selected rows.
    """
    # Keep the public API independent of the internal module layout and avoid
    # importing the Arrow transformation stack while ray.data.extensions is
    # initializing.
    from ray.data._internal.arrow_ops.transform_pyarrow import (
        take_table as _take_table,
    )

    return _take_table(table, indices)
