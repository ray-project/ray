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
from dataclasses import dataclass, field
from typing import Dict, FrozenSet, List, Union

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from ray.data._internal.datasource_v2.formats.parquet.parquet_file_chunking_utils import (
    _row_group_unit_id,
)
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


@DeveloperAPI
@dataclass(frozen=True)
class GeneratedIdCheckpoint:
    """What a generated-ID checkpoint says is already done, in read-unit terms.

    Attributes:
        done_unit_ids: Read unit ids whose rows are all committed: a file's
            path when all its row groups are done, otherwise
            ``"<path>#rg<N>"`` for each done row group. Listing drops these
            units before reading.
        partial_masks: For each row group with only some rows committed, its
            read unit id mapped to a boolean mask indexed by ``row_id``
            (``True`` means committed). The reader drops those rows.
    """

    done_unit_ids: FrozenSet[str] = frozenset()
    partial_masks: Dict[str, np.ndarray] = field(default_factory=dict)


# One row per input file with committed rows, built by ``_compact_file_ids``.
_COMPACTED_CHECKPOINT_SCHEMA = pa.schema(
    [
        ("path", pa.string()),
        ("num_row_groups", pa.int32()),
        ("done_row_groups", pa.list_(pa.int32())),
        ("partial_row_groups", pa.list_(pa.int32())),
        ("partial_masks", pa.list_(pa.list_(pa.bool_()))),
    ]
)


def _id_field(
    ids: Union[pa.Array, pa.ChunkedArray], name: str, value_type: pa.DataType
) -> np.ndarray:
    # Fields are looked up by name: the struct's field order isn't guaranteed
    # to survive a Parquet round trip.
    values = pc.cast(pc.struct_field(ids, name), value_type)
    return values.to_numpy(zero_copy_only=False)


def _compact_file_ids(ids: Union[pa.Array, pa.ChunkedArray]) -> pa.Table:
    """Compact the committed generated IDs of one file into a single row.

    Each row group with committed rows is either done (all its on-disk rows
    are committed) or partial, in which case its committed ``row_id`` values
    become a boolean mask. Duplicate IDs, from rows written more than once,
    are counted once.

    Args:
        ids: Generated IDs that all name the same file.

    Returns:
        A one-row table with ``_COMPACTED_CHECKPOINT_SCHEMA``.
    """
    path_prefix = _id_field(ids, PATH_PREFIX_FIELD, pa.string())[0]
    file_name = _id_field(ids, FILE_NAME_FIELD, pa.string())[0]
    num_row_groups = int(_id_field(ids, NUM_FRAGMENTS_FIELD, pa.int32())[0])
    row_groups = _id_field(ids, FRAGMENT_FIELD, pa.int32())
    row_group_sizes = _id_field(ids, NUM_ROWS_FIELD, pa.int32())
    row_ids = _id_field(ids, ROW_ID_FIELD, pa.int32())

    done_row_groups: List[int] = []
    partial_row_groups: List[int] = []
    partial_masks: List[List[bool]] = []
    for row_group in np.unique(row_groups):
        in_group = row_groups == row_group
        size = int(row_group_sizes[in_group][0])
        committed = np.unique(row_ids[in_group])
        # Only IDs inside the row group count; anything else must never mark
        # rows done.
        committed = committed[(committed >= 0) & (committed < size)]
        if len(committed) >= size:
            done_row_groups.append(int(row_group))
        else:
            mask = np.zeros(size, dtype=bool)
            mask[committed] = True
            partial_row_groups.append(int(row_group))
            partial_masks.append(mask.tolist())

    return pa.Table.from_pylist(
        [
            {
                "path": posixpath.join(path_prefix, file_name),
                "num_row_groups": num_row_groups,
                "done_row_groups": done_row_groups,
                "partial_row_groups": partial_row_groups,
                "partial_masks": partial_masks,
            }
        ],
        schema=_COMPACTED_CHECKPOINT_SCHEMA,
    )


def _checkpoint_from_compacted(compacted: pa.Table) -> GeneratedIdCheckpoint:
    """Turn compacted per-file rows into done read unit ids and partial masks.

    Args:
        compacted: Rows with ``_COMPACTED_CHECKPOINT_SCHEMA``.

    Returns:
        The checkpoint, in read-unit terms.
    """
    done_unit_ids = set()
    partial_masks: Dict[str, np.ndarray] = {}
    for row in compacted.to_pylist():
        path = row["path"]
        if len(row["done_row_groups"]) == row["num_row_groups"]:
            done_unit_ids.add(path)
            continue
        done_unit_ids.update(
            _row_group_unit_id(path, row_group) for row_group in row["done_row_groups"]
        )
        for row_group, mask in zip(row["partial_row_groups"], row["partial_masks"]):
            partial_masks[_row_group_unit_id(path, row_group)] = np.asarray(
                mask, dtype=bool
            )
    return GeneratedIdCheckpoint(frozenset(done_unit_ids), partial_masks)
