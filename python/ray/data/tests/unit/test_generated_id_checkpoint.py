"""Unit tests for compacting committed generated IDs into what a resumed job
can skip: done read units and row masks of partly done row groups."""

import numpy as np
import pyarrow as pa
import pytest

from ray.data.checkpoint.generated_id import (
    _COMPACTED_CHECKPOINT_SCHEMA,
    GeneratedIdCheckpoint,
    _build_generated_ids,
    _checkpoint_from_compacted,
    _compact_file_ids,
)

PATH = "bucket/dir/a.parquet"
NUM_ROW_GROUPS = 3
ROW_GROUP_SIZE = 4


def _ids(path, committed_rows_by_row_group, num_row_groups=NUM_ROW_GROUPS):
    """Generated IDs of the committed ``(row_group, [row_ids])`` pairs."""
    arrays = []
    for row_group, row_ids in committed_rows_by_row_group:
        for row_id in row_ids:
            arrays.append(
                _build_generated_ids(
                    path,
                    row_group_index=row_group,
                    num_row_groups=num_row_groups,
                    row_group_num_rows=ROW_GROUP_SIZE,
                    rows_before=row_id,
                    num_rows=1,
                )
            )
    return pa.chunked_array(arrays)


def _compact(*files):
    return pa.concat_tables([_compact_file_ids(ids) for ids in files])


def test_compact_marks_done_and_partial_row_groups():
    compacted = _compact_file_ids(_ids(PATH, [(0, [0, 1, 2, 3]), (2, [1, 3])]))

    assert compacted.schema == _COMPACTED_CHECKPOINT_SCHEMA
    assert compacted.to_pylist() == [
        {
            "path": PATH,
            "num_row_groups": NUM_ROW_GROUPS,
            "done_row_groups": [0],
            "partial_row_groups": [2],
            "partial_masks": [[False, True, False, True]],
        }
    ]


def test_compact_counts_duplicate_ids_once():
    """A row written twice must not make a partly done row group look done."""
    compacted = _compact_file_ids(_ids(PATH, [(1, [0, 0, 1, 1])]))

    assert compacted["done_row_groups"].to_pylist() == [[]]
    assert compacted["partial_masks"].to_pylist() == [[[True, True, False, False]]]


def test_compact_ignores_row_ids_outside_the_row_group():
    """Out-of-range IDs must never make a row group look done."""
    compacted = _compact_file_ids(_ids(PATH, [(0, [-1, 0, 1, 7])]))

    assert compacted["done_row_groups"].to_pylist() == [[]]
    assert compacted["partial_masks"].to_pylist() == [[[True, True, False, False]]]


def test_checkpoint_names_whole_file_when_all_row_groups_done():
    every_row = list(range(ROW_GROUP_SIZE))
    compacted = _compact(_ids(PATH, [(g, every_row) for g in range(NUM_ROW_GROUPS)]))

    checkpoint = _checkpoint_from_compacted(compacted)

    assert checkpoint.done_unit_ids == {PATH}
    assert checkpoint.partial_masks == {}


def test_checkpoint_names_done_row_groups_and_partial_masks():
    other = "bucket/dir/b.parquet"
    compacted = _compact(
        _ids(PATH, [(0, [0, 1, 2, 3]), (1, [2])]),
        _ids(other, [(2, [0, 1, 2, 3])]),
    )

    checkpoint = _checkpoint_from_compacted(compacted)

    assert checkpoint.done_unit_ids == {f"{PATH}#rg0", f"{other}#rg2"}
    assert set(checkpoint.partial_masks) == {f"{PATH}#rg1"}
    np.testing.assert_array_equal(
        checkpoint.partial_masks[f"{PATH}#rg1"], [False, False, True, False]
    )


def test_empty_checkpoint_skips_nothing():
    checkpoint = GeneratedIdCheckpoint()

    assert checkpoint.done_unit_ids == frozenset()
    assert checkpoint.partial_masks == {}


@pytest.mark.parametrize(
    "path",
    ["a.parquet", "/abs/dir/a.parquet", "C:\\data\\a.parquet"],
    ids=["no_directory", "absolute", "backslashes"],
)
def test_compacted_path_matches_the_source_path(path):
    """IDs split a path into directory and name; joining them back must give
    the exact path the reader used, or listing would never match it."""
    every_row = list(range(ROW_GROUP_SIZE))
    compacted = _compact(_ids(path, [(0, every_row)], num_row_groups=1))

    assert _checkpoint_from_compacted(compacted).done_unit_ids == {path}


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
