"""Stable Parquet row IDs used by generated_id_column checkpointing.

This module holds everything specific to the generated-ID checkpoint format:

- The per-row struct ID type (``GENERATED_ID_COLUMN_TYPE``) and its
  construction (:func:`get_generated_id_column`).
- The compact per-file checkpoint representation
  (``CHECKPOINTED_GENERATED_ID_COLUMN_TABLE_SCHEMA``) that
  ``GeneratedIdColumnCheckpointManager`` builds from committed row IDs at
  load time, plus the helpers that consume it at list time
  (:func:`index_checkpointed_fragments`,
  :func:`get_checkpoint_fragments_info_for_file`,
  :func:`is_file_fragments_fully_checkpointed`) and at read time
  (:func:`parse_checkpointed_fragment_info`,
  :func:`exclude_checkpointed_rows`).

Compact-representation semantics: a file that was never read has no row in
the checkpoint table (a null struct downstream); a fully-committed row group
stores an empty ``checkpointed_row_ids`` list; a partially-committed row
group stores a dense boolean mask of length ``num_rows`` (True =
checkpointed). Unfinished work is encoded as absence — uncommitted rows are
never stored.
"""

import os
from dataclasses import dataclass
from typing import Dict, Optional, Union

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset

from ray.data.block import Block, BlockAccessor
from ray.data.context import DataContext

# Name of the kwarg carrying the loaded checkpoint block into ListFiles tasks.
CHECKPOINTED_IDS_KWARG_NAME = "checkpointed_ids"

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

#
# Compact checkpoint table schema (built at load time from committed IDs).
#

CHECKPOINTED_FILE_COLUMN_NAME = "checkpointed_file"
CHECKPOINTED_FILE_FRAGMENTS_COLUMN_NAME = "checkpointed_file_fragments"

# Per-row-group struct fields.
CHECKPOINTED_FILE_FRAGMENT_ID_FIELD = "fragment_id"
CHECKPOINTED_FILE_FRAGMENT_NUM_ROWS_FIELD = "num_rows"
CHECKPOINTED_FILE_FRAGMENT_NUM_CHECKPOINTED_ROWS_FIELD = "num_checkpointed_rows"
CHECKPOINTED_FILE_FRAGMENT_CHECKPOINTED_ROW_IDS_FIELD = "checkpointed_row_ids"

CHECKPOINTED_FRAGMENT_TYPE = pa.struct(
    [
        # Row group index within the file.
        pa.field(CHECKPOINTED_FILE_FRAGMENT_ID_FIELD, pa.int32(), nullable=False),
        # Total number of rows in this row group (on-disk).
        pa.field(CHECKPOINTED_FILE_FRAGMENT_NUM_ROWS_FIELD, pa.int32(), nullable=False),
        # Number of already-checkpointed rows in this row group.
        pa.field(
            CHECKPOINTED_FILE_FRAGMENT_NUM_CHECKPOINTED_ROWS_FIELD,
            pa.int32(),
            nullable=False,
        ),
        # Dense boolean mask of length ``num_rows`` (True = checkpointed).
        # An empty list means every row is checkpointed.
        pa.field(
            CHECKPOINTED_FILE_FRAGMENT_CHECKPOINTED_ROW_IDS_FIELD,
            pa.large_list(pa.bool_()),
            nullable=True,
        ),
    ]
)

# Per-file struct fields. This struct rides on the file manifest into readers.
CHECKPOINTED_FILE_FRAGMENTS_NUM_FRAGMENTS_FIELD = "num_fragments"
CHECKPOINTED_FILE_FULLY_CHECKPOINTED_FIELD = "fully_checkpointed"
CHECKPOINTED_FILE_FRAGMENTS_INFO_FIELD = "fragments"

CHECKPOINTED_FILE_FRAGMENTS_TYPE = pa.struct(
    [
        # Number of row groups with checkpoint entries for this file.
        pa.field(
            CHECKPOINTED_FILE_FRAGMENTS_NUM_FRAGMENTS_FIELD, pa.int32(), nullable=False
        ),
        # Whether every row group of the file is fully checkpointed.
        pa.field(
            CHECKPOINTED_FILE_FULLY_CHECKPOINTED_FIELD, pa.bool_(), nullable=False
        ),
        # One CHECKPOINTED_FRAGMENT_TYPE entry per touched row group.
        pa.field(
            CHECKPOINTED_FILE_FRAGMENTS_INFO_FIELD,
            pa.large_list(CHECKPOINTED_FRAGMENT_TYPE),
            nullable=True,
        ),
    ]
)

CHECKPOINTED_GENERATED_ID_COLUMN_TABLE_SCHEMA = pa.schema(
    [
        pa.field(CHECKPOINTED_FILE_COLUMN_NAME, pa.string()),
        pa.field(
            CHECKPOINTED_FILE_FRAGMENTS_COLUMN_NAME, CHECKPOINTED_FILE_FRAGMENTS_TYPE
        ),
    ]
)


@dataclass
class CheckpointedFragmentInfo:
    """Checkpoint state of one row group, resolved for a read."""

    # The Parquet fragment (one row group).
    fragment: pa.dataset.ParquetFileFragment
    # The row group index within the file.
    row_group_idx: int
    # Total number of rows in the row group (on-disk).
    num_rows: int
    # Whether every row of the row group is checkpointed.
    fully_checkpointed: bool
    # Boolean mask over the row group (True = checkpointed), or None when
    # nothing is checkpointed. Empty array means fully checkpointed.
    checkpointed_row_ids: Optional[pa.Array]
    # Number of checkpointed rows in the row group.
    checkpointed_row_count: int


@dataclass
class CheckpointFragmentsInfo:
    """Checkpoint state of one file, as looked up from the compact table."""

    # The file path.
    path: str
    # The file's CHECKPOINTED_FILE_FRAGMENTS_TYPE struct scalar, or None when
    # the file has no checkpoint entry (never seen).
    checkpointed_file_fragments: Optional[pa.StructScalar]


def get_struct_field_index(
    struct_value: Union[pa.Array, pa.ChunkedArray, pa.Scalar], field_name: str
) -> int:
    """Return the index of ``field_name`` in a struct array/scalar's type.

    Struct field order is not preserved through an Arrow -> Parquet -> Arrow
    round-trip, so struct fields must always be resolved by name, never
    positionally.
    """
    if isinstance(struct_value, pa.ChunkedArray):
        struct_type = struct_value.chunks[0].type
    else:
        struct_type = struct_value.type

    field_index = struct_type.get_field_index(field_name)
    if field_index == -1:
        raise ValueError(f"Field '{field_name}' not found in struct type {struct_type}")
    return field_index


def _create_empty_checkpointed_fragment_info(
    fragment: pa.dataset.ParquetFileFragment,
    row_group_idx: int,
) -> CheckpointedFragmentInfo:
    """Info for a row group with no checkpointed rows."""
    return CheckpointedFragmentInfo(
        fragment=fragment,
        row_group_idx=row_group_idx,
        num_rows=fragment.metadata.row_group(row_group_idx).num_rows,
        fully_checkpointed=False,
        checkpointed_row_ids=None,
        checkpointed_row_count=0,
    )


def parse_checkpointed_fragment_info(
    fragment: pa.dataset.ParquetFileFragment,
    row_group_idx: int,
    checkpointed_file_fragments: Optional[pa.StructScalar],
) -> CheckpointedFragmentInfo:
    """Resolve one row group's checkpoint state from its file's struct scalar.

    Args:
        fragment: Parquet fragment carrying exactly this row group.
        row_group_idx: Row group index within the file.
        checkpointed_file_fragments: The file's
            ``CHECKPOINTED_FILE_FRAGMENTS_TYPE`` struct scalar from the
            manifest, or None / a null scalar when the file was never seen.

    Returns:
        The row group's :class:`CheckpointedFragmentInfo`. When the file or
        row group has no checkpoint entry, an empty info (nothing
        checkpointed) is returned.
    """
    if (
        checkpointed_file_fragments is None
        or checkpointed_file_fragments.is_valid is False
    ):
        return _create_empty_checkpointed_fragment_info(fragment, row_group_idx)

    fragments_field_idx = get_struct_field_index(
        checkpointed_file_fragments, CHECKPOINTED_FILE_FRAGMENTS_INFO_FIELD
    )
    fragments = pc.struct_field(checkpointed_file_fragments, [fragments_field_idx])
    if fragments.is_valid is False or len(fragments) == 0:
        return _create_empty_checkpointed_fragment_info(fragment, row_group_idx)
    fragments_values = fragments.values  # StructArray

    fragment_id_field_idx = get_struct_field_index(
        fragments_values, CHECKPOINTED_FILE_FRAGMENT_ID_FIELD
    )
    fragment_ids = pc.struct_field(fragments_values, [fragment_id_field_idx])

    wanted_mask = pc.equal(fragment_ids, pa.scalar(row_group_idx, fragment_ids.type))
    indices = pc.indices_nonzero(wanted_mask)
    if len(indices) == 0:
        return _create_empty_checkpointed_fragment_info(fragment, row_group_idx)

    checkpointed_fragment = fragments_values[indices[0].as_py()]
    checkpointed_row_ids = pc.struct_field(
        checkpointed_fragment,
        [
            get_struct_field_index(
                checkpointed_fragment,
                CHECKPOINTED_FILE_FRAGMENT_CHECKPOINTED_ROW_IDS_FIELD,
            )
        ],
    )
    num_rows = pc.struct_field(
        checkpointed_fragment,
        [
            get_struct_field_index(
                checkpointed_fragment, CHECKPOINTED_FILE_FRAGMENT_NUM_ROWS_FIELD
            )
        ],
    ).as_py()
    num_checkpointed_rows = pc.struct_field(
        checkpointed_fragment,
        [
            get_struct_field_index(
                checkpointed_fragment,
                CHECKPOINTED_FILE_FRAGMENT_NUM_CHECKPOINTED_ROWS_FIELD,
            )
        ],
    ).as_py()

    actual_num_rows = fragment.metadata.row_group(row_group_idx).num_rows
    assert num_rows == actual_num_rows, (
        f"Number of rows in the row group {actual_num_rows} does not match "
        f"the number of rows in the checkpointed fragment {num_rows}"
    )

    if len(checkpointed_row_ids) == 0:
        # Empty list encodes "every row checkpointed".
        assert num_checkpointed_rows == num_rows, (
            f"Number of checkpointed rows {num_checkpointed_rows} does not match "
            f"the number of rows in the checkpointed fragment {num_rows}"
        )
        fully_checkpointed = True
        final_checkpointed_row_ids = pa.array([], type=pa.bool_())
    else:
        fully_checkpointed = False
        final_checkpointed_row_ids = checkpointed_row_ids.values

    return CheckpointedFragmentInfo(
        fragment=fragment,
        row_group_idx=row_group_idx,
        num_rows=num_rows,
        fully_checkpointed=fully_checkpointed,
        checkpointed_row_ids=final_checkpointed_row_ids,
        checkpointed_row_count=num_checkpointed_rows,
    )


def exclude_checkpointed_rows(
    table: pa.Table,
    checkpointed_fragment_info: CheckpointedFragmentInfo,
    current_row_offset: int,
    current_num_rows: int,
) -> pa.Table:
    """Drop already-checkpointed rows from a batch of one row group.

    Args:
        table: The batch to filter (rows of a single row group).
        checkpointed_fragment_info: The row group's checkpoint state.
        current_row_offset: Position of the batch's first row within the row
            group's read stream (pre-filter).
        current_num_rows: Number of rows the batch had before filtering.

    Returns:
        The batch with already-checkpointed rows removed.
    """
    checkpointed_row_ids = checkpointed_fragment_info.checkpointed_row_ids

    if checkpointed_row_ids is None:
        assert (
            not checkpointed_fragment_info.fully_checkpointed
        ), "Checkpointed row ids is None, intended to be empty checkpointed fragment"
        return table

    if len(checkpointed_row_ids) == 0:
        assert checkpointed_fragment_info.fully_checkpointed, (
            "Checkpointed row ids is empty array, intended to be fully "
            "checkpointed fragment"
        )
        return table.slice(0, 0)

    assert current_row_offset + current_num_rows <= len(checkpointed_row_ids), (
        f"Current row offset {current_row_offset} + current num rows "
        f"{current_num_rows} is greater than the length of the boolean mask "
        f"{len(checkpointed_row_ids)}"
    )

    relevant_bools = checkpointed_row_ids.slice(current_row_offset, current_num_rows)
    # True (checkpointed) rows are excluded; False (pending) rows are kept.
    return table.filter(pc.invert(relevant_bools))


def index_checkpointed_fragments(checkpointed_ids: Optional[Block]) -> Dict[str, int]:
    """Map file path -> row index in the compact checkpoint block."""
    if checkpointed_ids is None:
        return {}

    checkpointed_ids_table = BlockAccessor.for_block(checkpointed_ids).to_arrow()
    if checkpointed_ids_table.num_rows == 0:
        return {}

    file_paths = checkpointed_ids_table[CHECKPOINTED_FILE_COLUMN_NAME].to_pylist()
    return {file_path: i for i, file_path in enumerate(file_paths)}


def get_checkpoint_fragments_info_for_file(
    checkpointed_ids: Optional[Block],
    path: str,
    checkpointed_fragments_by_path: Dict[str, int],
) -> CheckpointFragmentsInfo:
    """Look up one file's checkpoint struct from the compact checkpoint block.

    Args:
        checkpointed_ids: Block with
            ``CHECKPOINTED_GENERATED_ID_COLUMN_TABLE_SCHEMA``, or None.
        path: The file path to look up.
        checkpointed_fragments_by_path: Index built by
            :func:`index_checkpointed_fragments` over the same block.

    Returns:
        The file's :class:`CheckpointFragmentsInfo`; its struct scalar is
        None when the file has no checkpoint entry.
    """
    if checkpointed_ids is None or path not in checkpointed_fragments_by_path:
        return CheckpointFragmentsInfo(path=path, checkpointed_file_fragments=None)

    checkpointed_ids_table = BlockAccessor.for_block(checkpointed_ids).to_arrow()
    if checkpointed_ids_table.num_rows == 0:
        return CheckpointFragmentsInfo(path=path, checkpointed_file_fragments=None)

    file_index = checkpointed_fragments_by_path[path]
    checkpointed_file_fragments = checkpointed_ids_table[
        CHECKPOINTED_FILE_FRAGMENTS_COLUMN_NAME
    ][file_index]
    return CheckpointFragmentsInfo(
        path=path, checkpointed_file_fragments=checkpointed_file_fragments
    )


def is_file_fragments_fully_checkpointed(
    checkpointed_fragments_info: CheckpointFragmentsInfo,
) -> bool:
    """Whether every row group of the file is fully checkpointed."""
    fully_checkpointed_field_idx = get_struct_field_index(
        checkpointed_fragments_info.checkpointed_file_fragments,
        CHECKPOINTED_FILE_FULLY_CHECKPOINTED_FIELD,
    )
    return pc.struct_field(
        checkpointed_fragments_info.checkpointed_file_fragments,
        [fully_checkpointed_field_idx],
    ).as_py()


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
