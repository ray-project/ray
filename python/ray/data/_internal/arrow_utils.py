from __future__ import annotations

import itertools
from typing import List, Union

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc


def _counts_to_offsets(counts: pa.Array) -> pa.Array:
    """Convert per-row counts to list offsets via cumulative sum."""
    cumsum = pc.cumulative_sum(counts)
    return pa.concat_arrays([pa.array([0], type=cumsum.type), cumsum])


def _combine_as_list_array(
    column_values: List[Union[pa.Array, pa.ChunkedArray]] | None = None,
    *,
    offsets: pa.Array | None = None,
    values: pa.Array | None = None,
    is_large: bool = False,
    null_mask: pa.Array | None = None,
) -> pa.Array:
    """Combine list arrays or build a list array from offsets and values."""
    if column_values is None:
        if offsets is None or values is None:
            raise ValueError(
                "Either column_values or both offsets and values must be provided."
            )
    else:
        lens = [len(v) for v in column_values]
        offsets_type = pa.int64() if is_large else pa.int32()
        offsets = pa.array(np.concatenate([[0], np.cumsum(lens)]), type=offsets_type)
        values = pa.concat_arrays(
            itertools.chain(
                *[
                    v.chunks if isinstance(v, pa.ChunkedArray) else [v]
                    for v in column_values
                ]
            )
        )

    offsets_type = pa.int64() if is_large else pa.int32()
    offsets = pc.cast(offsets, offsets_type)
    array_cls = pa.LargeListArray if is_large else pa.ListArray
    list_type = pa.large_list(values.type) if is_large else pa.list_(values.type)
    return array_cls.from_arrays(offsets, values, list_type, mask=null_mask)
