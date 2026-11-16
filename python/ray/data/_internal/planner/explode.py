"""Block-level kernel for :class:`~ray.data._internal.logical.operators.Explode`.

Expands list columns into rows. ``plan_explode_op`` in ``plan_udf_map_op``
wraps :func:`explode_block` in the physical operator; everything else here is
pure PyArrow and has no dependency on the planner.
"""

from typing import Sequence, Tuple

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from ray.data.block import Block, BlockAccessor
from ray.data.namespace_expressions.list_namespace import _is_list_like


def _validate_explode_column(table: "pa.Table", column: str) -> int:
    """Return ``column``'s index in ``table``, raising if it can't be exploded."""
    col_idx = table.schema.get_field_index(column)
    if col_idx == -1:
        raise ValueError(
            f"explode() column {column!r} not found in input columns "
            f"{table.schema.names}."
        )
    column_type = table.schema.field(col_idx).type
    # A ``null``-typed column (every value null) is a column of null lists.
    if not (_is_list_like(column_type) or pa.types.is_null(column_type)):
        raise ValueError(
            f"explode() requires a list, large_list, or fixed_size_list "
            f"column, but column {column!r} has type {column_type}."
        )
    return col_idx


def _exclusive_starts(lengths: "pa.Array") -> Tuple["pa.Array", "pa.Array"]:
    """Return ``(start, end)`` offsets for per-row ``lengths`` (a cumulative sum)."""
    ends = pc.cumulative_sum(lengths)
    return pc.subtract(ends, lengths), ends


def _explode_single(table: "pa.Table", column: str, col_idx: int) -> "pa.Table":
    """Explode one column -- the common case, without the zip machinery."""
    arr = table.column(col_idx).combine_chunks()
    arr_type = arr.type
    if pa.types.is_null(arr_type):
        # Every row is a null list and explodes to one null row, so the block
        # passes through unchanged.
        return table

    if arr.null_count:
        if pa.types.is_fixed_size_list(arr_type):
            # A fixed-size list has no room for the single-element ``[null]``
            # below, so widen it to a variable-sized list first.
            arr = arr.cast(pa.list_(arr_type.value_field))
        # A null list explodes to exactly one null row, which is what a
        # one-element ``[null]`` list flattens to -- so substituting it for
        # every null list reduces the whole operation to a flatten. Empty
        # lists flatten to nothing, as their semantics require.
        arr = pc.coalesce(arr, pa.scalar([None], type=arr.type))
    values = pc.list_flatten(arr)
    # The source row of each flattened value, i.e. the rows to replicate.
    parent_indices = pc.list_parent_indices(arr)

    exploded_field = arr_type.value_field.with_name(column).with_nullable(True)
    # Gather the *other* columns only: this column's gathered values would be
    # overwritten by ``values`` anyway, and it is often the widest one here
    # (nested lists, lists of structs).
    others = table.remove_column(col_idx)
    if others.num_columns == 0:
        # Nothing to replicate; a column-less table carries no row count.
        return pa.Table.from_arrays(
            [values], schema=pa.schema([exploded_field], table.schema.metadata)
        )
    return others.take(parent_indices).add_column(col_idx, exploded_field, values)


def _explode_zipped(
    table: "pa.Table", columns: Sequence[str], col_idxs: Sequence[int]
) -> "pa.Table":
    """Explode several columns in lockstep, padding short lists with nulls.

    Row ``i`` produces ``max(len(c[i]) for c in columns)`` rows; a column
    whose list is shorter than that contributes nulls for the extra rows.
    """
    num_rows = table.num_rows
    real_lens, output_lens, flats, fields = [], [], [], []
    for column, col_idx in zip(columns, col_idxs):
        arr = table.column(col_idx).combine_chunks()
        if pa.types.is_null(arr.type):
            # Every row is a null list: no values, one output row each.
            # ``fill_null`` over a null array builds the constant run in
            # Arrow; a Python ``[0] * num_rows`` is ~75x slower per million.
            real_lens.append(pc.fill_null(pa.nulls(num_rows, pa.int64()), 0))
            output_lens.append(pc.fill_null(pa.nulls(num_rows, pa.int64()), 1))
            flats.append(pa.nulls(0, type=pa.null()))
            fields.append(pa.field(column, pa.null(), nullable=True))
            continue
        lengths = pc.list_value_length(arr).cast(pa.int64())
        # Values actually present, vs. rows this column asks the zip for: a
        # null list holds no values but still claims one (null) output row.
        real_lens.append(pc.fill_null(lengths, 0))
        output_lens.append(pc.fill_null(lengths, 1))
        flats.append(pc.list_flatten(arr))
        fields.append(arr.type.value_field.with_name(column).with_nullable(True))

    row_lens = output_lens[0]
    for other in output_lens[1:]:
        row_lens = pc.max_element_wise(row_lens, other)
    row_starts, row_ends = _exclusive_starts(row_lens)
    total = row_ends[-1].as_py() if num_rows else 0

    # ``list_parent_indices`` over a list array shaped by the output offsets
    # gives the source row of every output row -- the rows to replicate.
    offsets = pa.concat_arrays([pa.array([0], type=pa.int64()), row_ends])
    parent_indices = pc.list_parent_indices(
        pa.LargeListArray.from_arrays(offsets, pa.nulls(total))
    )
    # ``position`` counts output rows; ``rank`` is the position within the
    # source row, i.e. which list element each output row takes. ``np.arange``
    # is ~5x faster here than building the run with Arrow compute.
    position = pa.array(np.arange(total, dtype=np.int64))
    rank = pc.subtract(position, row_starts.take(parent_indices))

    values = []
    for real_len, flat in zip(real_lens, flats):
        starts, _ = _exclusive_starts(real_len)
        # Past this column's own list length, take a null index -> a null row.
        within = pc.less(rank, real_len.take(parent_indices))
        indices = pc.if_else(
            within,
            pc.add(starts.take(parent_indices), rank),
            pa.scalar(None, type=pa.int64()),
        )
        values.append(flat.take(indices))

    others = table
    for col_idx in sorted(col_idxs, reverse=True):
        others = others.remove_column(col_idx)
    # Re-insert by ascending schema index, not by the order the caller listed
    # the columns in: ``add_column`` indices address the shrunk table, so each
    # column only lands back in its original position once every column to its
    # left is already there.
    restored = sorted(zip(col_idxs, fields, values), key=lambda item: item[0])
    if others.num_columns == 0:
        # Nothing to replicate; a column-less table carries no row count.
        return pa.Table.from_arrays(
            [vals for _, _, vals in restored],
            schema=pa.schema([f for _, f, _ in restored], table.schema.metadata),
        )
    out = others.take(parent_indices)
    for col_idx, field, vals in restored:
        out = out.add_column(col_idx, field, vals)
    return out


def explode_block(block: Block, columns: Sequence[str]) -> Block:
    """Expand each element of ``block``'s list ``columns`` into its own row,
    replicating all other columns.

    Semantics: a null list produces one row with a null value; an empty list
    produces no rows. Supports ``list``, ``large_list``, and
    ``fixed_size_list`` columns; exploding a ``list<list<T>>`` removes exactly
    one level of nesting. Several columns explode in lockstep, with shorter
    lists padded with nulls (matching DataFusion's ``unnest``).

    Pandas blocks are converted to Arrow (``pd.Series.explode`` turns empty
    lists into NaN rows, which contradicts these semantics), so the output
    block is always Arrow-backed.
    """
    table = BlockAccessor.for_block(block).to_arrow()
    col_idxs = [_validate_explode_column(table, column) for column in columns]
    if len(col_idxs) == 1:
        return _explode_single(table, columns[0], col_idxs[0])
    return _explode_zipped(table, columns, col_idxs)
