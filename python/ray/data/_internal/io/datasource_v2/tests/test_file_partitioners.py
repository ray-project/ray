from unittest.mock import MagicMock

import numpy as np
import pyarrow as pa
import pytest

from ray.data._internal.io.datasource_v2.listing.file_manifest import (
    FILE_SIZE_COLUMN_NAME,
    PATH_COLUMN_NAME,
)
from ray.data._internal.io.datasource_v2.listing.listing_utils import partition_files
from ray.data._internal.io.datasource_v2.partitioners.round_robin_partitioner import (
    RoundRobinPartitioner,
)
from ray.data._internal.io.datasource_v2.readers.in_memory_size_estimator import (
    InMemorySizeEstimator,
)


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
def test_round_robin_partitioner_produces_correct_partitions(
    num_paths, expected_partitions
):
    input_table = pa.Table.from_pydict(
        {
            PATH_COLUMN_NAME: [str(i) for i in range(1, num_paths + 1)],
            FILE_SIZE_COLUMN_NAME: [1] * num_paths,
        }
    )

    class StubInMemorySizeEstimator(InMemorySizeEstimator):
        def estimate_in_memory_sizes(
            self,
            manifest,
        ) -> np.ndarray:
            return np.ones(len(manifest))

    outputs = partition_files(
        iter([input_table]),
        MagicMock(),
        partitioner=RoundRobinPartitioner(
            in_memory_size_estimator=StubInMemorySizeEstimator(),
            num_buckets=2,
            min_bucket_size=1,
            max_bucket_size=3,
        ),
    )

    partitions = [output[PATH_COLUMN_NAME].to_pylist() for output in outputs]
    assert partitions == expected_partitions


def test_round_robin_partitioner_with_no_size_estimates():
    # This tests the case where we don't have size estimates. This can happen if you use
    # HTTPFileSystem.
    input_table = pa.Table.from_pydict(
        {
            PATH_COLUMN_NAME: ["path0", "path1", "path2"],
            FILE_SIZE_COLUMN_NAME: [None, None, None],
        }
    )

    class StubInMemorySizeEstimator(InMemorySizeEstimator):
        def estimate_in_memory_sizes(
            self,
            manifest,
        ) -> np.ndarray:
            return manifest.file_sizes

    outputs = partition_files(
        iter([input_table]),
        MagicMock(),
        partitioner=RoundRobinPartitioner(
            in_memory_size_estimator=StubInMemorySizeEstimator(),
            num_buckets=2,
            min_bucket_size=1,
            max_bucket_size=1,
        ),
    )
    partitions = [output[PATH_COLUMN_NAME].to_pylist() for output in outputs]

    # If in-memory size estimates aren't available, the partitioner should round-robin
    # the paths across the buckets, disregarding the bucket size limits.
    assert len(partitions) == 2
    assert partitions[0] == ["path0", "path2"]
    assert partitions[1] == ["path1"]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
