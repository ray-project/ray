"""Row IDs that Ray Data generates for ``CheckpointConfig(generated_id_column=...)``.

A generated ID names where a row lives in its Parquet file: the file, the row
group, and the row's position in that row group. Ray Data stamps it on every
row with :class:`GeneratedIdColumn`, a synthesized column the V2 Parquet
reader appends to each batch, so checkpointing works without a unique ID
column in the input.

The ID is a struct whose path and row-group fields are constant within a row
group, so they are dictionary-encoded and cost almost nothing per row. Its
field names, order and types are the checkpoint format: changing them would
make existing checkpoints unreadable.
"""

import posixpath
from dataclasses import dataclass

import numpy as np
import pyarrow as pa

from ray.data._internal.datasource_v2.interfaces.synthesized_columns import (
    ReadUnitPosition,
    SynthesizedColumn,
)
from ray.util.annotations import DeveloperAPI

PATH_PREFIX_FIELD = "path_prefix"
FILE_NAME_FIELD = "file_name"
FRAGMENT_FIELD = "fragment"
NUM_FRAGMENTS_FIELD = "num_fragments"
NUM_ROWS_FIELD = "num_rows"
ROW_ID_FIELD = "row_id"

GENERATED_ID_COLUMN_FIELDS = {
    # Directory and base name of the source file.
    PATH_PREFIX_FIELD: pa.dictionary(pa.int32(), pa.string()),
    FILE_NAME_FIELD: pa.dictionary(pa.int32(), pa.string()),
    # Index of the row group in the file, and the file's row group count.
    FRAGMENT_FIELD: pa.dictionary(pa.int32(), pa.int32()),
    NUM_FRAGMENTS_FIELD: pa.dictionary(pa.int32(), pa.int32()),
    # Rows in the row group as stored on disk.
    NUM_ROWS_FIELD: pa.dictionary(pa.int32(), pa.int32()),
    # Position of the row among the rows the reader produced from its row
    # group, counted after any pushed-down filter.
    ROW_ID_FIELD: pa.int32(),
}

GENERATED_ID_COLUMN_TYPE = pa.struct(
    [
        pa.field(name, dtype, nullable=False)
        for name, dtype in GENERATED_ID_COLUMN_FIELDS.items()
    ]
)


def _build_generated_ids(
    path: str,
    row_group_index: int,
    num_row_groups: int,
    row_group_num_rows: int,
    rows_before: int,
    num_rows: int,
) -> pa.StructArray:
    """Build the generated IDs of ``num_rows`` consecutive rows of one row group.

    Args:
        path: Path of the source file.
        row_group_index: Index of the row group in the file.
        num_row_groups: Number of row groups in the file.
        row_group_num_rows: Rows in the row group as stored on disk.
        rows_before: Rows the reader already produced from this row group;
            the first row gets ``row_id == rows_before``.
        num_rows: Number of IDs to build.

    Returns:
        A struct array of type ``GENERATED_ID_COLUMN_TYPE`` with ``num_rows``
        entries.
    """
    # Readers of the ID rejoin these with ``posixpath.join``; that must give
    # back the exact source path, which holds for any separator.
    constants = {
        PATH_PREFIX_FIELD: (posixpath.dirname(path), pa.string()),
        FILE_NAME_FIELD: (posixpath.basename(path), pa.string()),
        FRAGMENT_FIELD: (row_group_index, pa.int32()),
        NUM_FRAGMENTS_FIELD: (num_row_groups, pa.int32()),
        NUM_ROWS_FIELD: (row_group_num_rows, pa.int32()),
    }
    # A one-entry dictionary indexed by zeros encodes a constant column
    # without materializing ``num_rows`` copies of the value.
    zeros = np.zeros(num_rows, dtype=np.int32)
    arrays = [
        pa.DictionaryArray.from_arrays(zeros, pa.array([value], type=value_type))
        for value, value_type in constants.values()
    ]
    arrays.append(
        pa.array(np.arange(rows_before, rows_before + num_rows), type=pa.int32())
    )
    return pa.StructArray.from_arrays(arrays, fields=list(GENERATED_ID_COLUMN_TYPE))


@DeveloperAPI
@dataclass
class GeneratedIdColumn(SynthesizedColumn):
    """The row ID column behind ``CheckpointConfig(generated_id_column=...)``.

    Needs read unit boundaries: every batch must come from a single row
    group so its rows can be numbered within that row group.

    Attributes:
        name: Name of the generated column.
    """

    name: str
    type = GENERATED_ID_COLUMN_TYPE
    requires_read_unit_boundaries = True

    def compute(self, position: ReadUnitPosition, num_rows: int) -> pa.Array:
        unit = position.unit
        if unit.id == unit.source or unit.count is None or unit.num_rows is None:
            # A whole-file unit can't say which row group a row came from, so
            # IDs built from it wouldn't match the row-group IDs checkpoints
            # are keyed on. Parquet reads list files by footer and always get
            # row-group units; fail loudly rather than write wrong IDs.
            raise ValueError(
                f"`generated_id_column` needs each row group of {unit.source!r} "
                "read on its own, but the file was read as a single unit. "
                "Generated row IDs are supported only for Parquet reads listed "
                "by footer on the V2 datasource path."
            )
        return _build_generated_ids(
            path=unit.source,
            row_group_index=unit.index,
            num_row_groups=unit.count,
            row_group_num_rows=unit.num_rows,
            rows_before=position.rows_before,
            num_rows=num_rows,
        )
