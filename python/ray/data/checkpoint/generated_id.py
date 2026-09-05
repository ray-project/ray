"""Stable Parquet row IDs used by generated_id_column checkpointing."""

import os
from typing import Optional

import numpy as np
import pyarrow as pa

from ray.data.context import DataContext

PATH_PREFIX_FIELD = "path_prefix"
FILE_NAME_FIELD = "file_name"
FRAGMENT_FIELD = "fragment"
NUM_FRAGMENTS_FIELD = "num_fragments"
NUM_ROWS_FIELD = "num_rows"
ROW_ID_FIELD = "row_id"

GENERATED_ID_COLUMN_FIELD_NAMES = [
    PATH_PREFIX_FIELD,
    FILE_NAME_FIELD,
    FRAGMENT_FIELD,
    NUM_FRAGMENTS_FIELD,
    NUM_ROWS_FIELD,
    ROW_ID_FIELD,
]

GENERATED_ID_COLUMN_FIELDS = {
    PATH_PREFIX_FIELD: pa.dictionary(pa.int32(), pa.string()),
    FILE_NAME_FIELD: pa.dictionary(pa.int32(), pa.string()),
    FRAGMENT_FIELD: pa.dictionary(pa.int32(), pa.int32()),
    NUM_FRAGMENTS_FIELD: pa.dictionary(pa.int32(), pa.int32()),
    NUM_ROWS_FIELD: pa.dictionary(pa.int32(), pa.int32()),
    ROW_ID_FIELD: pa.int32(),
}

GENERATED_ID_COLUMN_TYPE = pa.struct(
    [
        pa.field(name, GENERATED_ID_COLUMN_FIELDS[name], nullable=False)
        for name in GENERATED_ID_COLUMN_FIELD_NAMES
    ]
)


def get_generated_id_column_name() -> Optional[str]:
    ctx = DataContext.get_current()
    config = getattr(ctx, "checkpoint_config", None)
    return getattr(config, "generated_id_column", None)


def get_generated_id_column(
    path: str,
    row_group_idx: int,
    num_row_groups: int,
    total_num_rows: int,
    current_row_offset: int,
    current_num_rows: int,
) -> pa.Array:
    """One struct ID per row in this batch.

    ``row_id`` is the position inside this row group, starting at 0.
    ``current_row_offset`` is how many rows of this group were already
    emitted, so the next batch continues the count.
    """
    path_prefix = os.path.dirname(path)
    file_name = os.path.basename(path)
    row_ids = np.arange(current_row_offset, current_row_offset + current_num_rows)

    def _field(name: str) -> pa.Array:
        if name == ROW_ID_FIELD:
            return pa.array(row_ids, type=pa.int32())
        fill = {
            PATH_PREFIX_FIELD: path_prefix,
            FILE_NAME_FIELD: file_name,
            FRAGMENT_FIELD: row_group_idx,
            NUM_FRAGMENTS_FIELD: num_row_groups,
            NUM_ROWS_FIELD: total_num_rows,
        }[name]
        base_type = (
            pa.string() if name in (PATH_PREFIX_FIELD, FILE_NAME_FIELD) else pa.int32()
        )
        # Constant column: a one-element dictionary with repeated index 0 is
        # cheaper than materializing and encoding N copies of the value.
        return pa.DictionaryArray.from_arrays(
            indices=np.zeros(current_num_rows, dtype=np.int32),
            dictionary=pa.array([fill], type=base_type),
        )

    fields = [
        pa.field(name, dtype, nullable=False)
        for name, dtype in GENERATED_ID_COLUMN_FIELDS.items()
    ]
    return pa.StructArray.from_arrays(
        [_field(name) for name in GENERATED_ID_COLUMN_FIELD_NAMES],
        fields=fields,
    )
