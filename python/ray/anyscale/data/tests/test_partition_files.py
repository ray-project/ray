from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from ray.anyscale.data._internal.logical.operators.list_files_operator import (
    FILE_SIZE_COLUMN_NAME,
    PATH_COLUMN_NAME,
)
from ray.anyscale.data._internal.planner.plan_partition_files_op import partition_files
from ray.anyscale.data._internal.readers import FileReader


@pytest.mark.parametrize(
    "num_paths, expected_partitions",
    (
        # These diagrams represent the state before leftover paths are yielded. Each
        # column represent a bucket, the height represent the max bucket size, and
        # numbers represent paths. There are two buckets, the min bucket size is 1, and
        # the max bucket size is 3.
        #
        # | | | |  Yeilds [1].
        # | | | |
        # |1| | |
        [1, [["1"]]],
        # | | | |  Move to the second bucket because ther first one exceeds the min
        # | | | |  bucket size (1).
        # |1| |2|
        [2, [["1"], ["2"]]],
        # |5| | |    | | | |  Continue spreading paths because all buckets contain the
        # |3| |4| -> | | |4|  min bucket size. Once the first bucket is full, yield the
        # |1| |2|    | | |2|  paths, clear the bucket, and move to the second bucket.
        [5, [["1", "3", "5"], ["2", "4"]]],
        # | | |6|    | | | |  The second bucket is full, so we yield the paths, clear
        # | | |4| -> | | | |  the bucket, and move back to the first.
        # | | |2|    | | | |
        [6, [["1", "3", "5"], ["2", "4", "6"]]],
        # | | | |  Repeat.
        # | | | |
        # |7| | |
        [7, [["1", "3", "5"], ["2", "4", "6"], ["7"]]],
    ),
)
def test_partition_files_produces_correct_partitions(num_paths, expected_partitions):
    num_buckets = 2
    min_bucket_size = 1
    max_bucket_size = 3
    inputs = [
        pa.Table.from_pydict(
            {
                PATH_COLUMN_NAME: [str(i) for i in range(1, num_paths + 1)],
                FILE_SIZE_COLUMN_NAME: [1] * num_paths,
            }
        )
    ]

    outputs = partition_files(
        inputs,
        MagicMock(),
        num_buckets=num_buckets,
        min_bucket_size=min_bucket_size,
        max_bucket_size=max_bucket_size,
        reader=MagicMock(
            spec=FileReader, estimate_in_memory_size=MagicMock(return_value=1)
        ),
        filesystem=MagicMock(),
    )

    partitions = [output[PATH_COLUMN_NAME].to_pylist() for output in outputs]
    assert partitions == expected_partitions


def test_partition_files_with_no_file_sizes():
    # This tests the case where we don't have file sizes. This can happen if you use
    # HTTPFileSystem.
    inputs = [
        pa.Table.from_pydict(
            {
                PATH_COLUMN_NAME: ["path0", "path1", "path2"],
                FILE_SIZE_COLUMN_NAME: [None, None, None],
            }
        )
    ]

    outputs = partition_files(
        inputs,
        MagicMock(),
        num_buckets=1,
        min_bucket_size=1,
        max_bucket_size=1,
        reader=MagicMock(
            spec=FileReader, estimate_in_memory_size=MagicMock(return_value=None)
        ),
        filesystem=MagicMock(),
    )
    partitions = [output[PATH_COLUMN_NAME].to_pylist() for output in outputs]

    # All paths should be in the same partition because we don't have file sizes.
    assert partitions == [["path0", "path1", "path2"]]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
