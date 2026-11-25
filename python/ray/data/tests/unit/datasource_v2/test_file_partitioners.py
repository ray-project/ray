from unittest.mock import MagicMock

import numpy as np
import pyarrow as pa
import pytest

from ray.data._internal.datasource_v2.common.listing_utils import partition_files
from ray.data._internal.datasource_v2.common.round_robin_partitioner import (
    RoundRobinPartitioner,
)
from ray.data._internal.datasource_v2.common.size_estimators import (
    SamplingInMemorySizeEstimator,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    FILE_CHUNK_METADATA_COLUMN_NAME,
    FILE_SIZE_COLUMN_NAME,
    PATH_COLUMN_NAME,
    FileChunk,
)
from ray.data._internal.datasource_v2.interfaces.file_partitioner import (
    PartitionHints,
)
from ray.data._internal.datasource_v2.interfaces.in_memory_size_estimator import (
    InMemorySizeEstimator,
)
from ray.data._internal.weighted_round_robin import WeightedRoundRobinPartitioner


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
            FILE_CHUNK_METADATA_COLUMN_NAME: [None] * num_paths,
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
            hints=PartitionHints(min_bucket_size=1, max_bucket_size=3, num_buckets=2),
        ),
    )

    partitions = [output[PATH_COLUMN_NAME].to_pylist() for output in outputs]
    assert partitions == expected_partitions


class _FileSizeEstimator(InMemorySizeEstimator):
    def estimate_in_memory_sizes(self, manifest) -> np.ndarray:
        return manifest.file_sizes


@pytest.mark.parametrize(
    "estimator",
    # The sampling estimator can't sample a file of unknown size, so it gives
    # each one a ``NaN`` estimate without opening any file.
    [_FileSizeEstimator(), SamplingInMemorySizeEstimator(reader=MagicMock())],
    ids=["file_size", "sampling"],
)
def test_round_robin_partitioner_with_no_size_estimates(estimator):
    # This tests the case where we don't have size estimates. This can happen if you use
    # HTTPFileSystem.
    input_table = pa.Table.from_pydict(
        {
            PATH_COLUMN_NAME: ["path0", "path1", "path2"],
            FILE_SIZE_COLUMN_NAME: pa.array([None, None, None], type=pa.int64()),
            FILE_CHUNK_METADATA_COLUMN_NAME: [None, None, None],
        }
    )

    outputs = partition_files(
        iter([input_table]),
        MagicMock(),
        partitioner=RoundRobinPartitioner(
            in_memory_size_estimator=estimator,
            hints=PartitionHints(min_bucket_size=1, max_bucket_size=1, num_buckets=2),
        ),
    )
    partitions = [output[PATH_COLUMN_NAME].to_pylist() for output in outputs]

    # An unknown size can't be budgeted against other files in a read task.
    assert partitions == [["path0"], ["path1"], ["path2"]]


def test_unknown_size_partition_is_emitted_before_listing_finishes():
    input_table = pa.table(
        {
            PATH_COLUMN_NAME: ["unknown"],
            FILE_SIZE_COLUMN_NAME: pa.array([None], type=pa.int64()),
            FILE_CHUNK_METADATA_COLUMN_NAME: [None],
        }
    )

    def listing():
        yield input_table
        raise AssertionError("Read task was not emitted before listing continued")

    outputs = iter(
        partition_files(
            listing(),
            MagicMock(),
            partitioner=RoundRobinPartitioner(
                _FileSizeEstimator(),
                hints=PartitionHints(
                    min_bucket_size=1, max_bucket_size=10, num_buckets=2
                ),
            ),
        )
    )

    assert next(outputs).to_pydict() == input_table.to_pydict()


@pytest.mark.parametrize(
    "num_buckets, expected_rows",
    [
        pytest.param(1, [[0], [1], [2], [3], [4]], id="single_bucket_keeps_order"),
        pytest.param(2, [[1], [3], [0, 2], [4]], id="multi_bucket_groups_known"),
    ],
)
def test_round_robin_partitioner_with_mixed_size_estimates(num_buckets, expected_rows):
    input_table = pa.table(
        {
            PATH_COLUMN_NAME: ["a", "unknown", "b", "unknown", "c"],
            FILE_SIZE_COLUMN_NAME: pa.array([1, None, 1, None, 1], type=pa.int64()),
            FILE_CHUNK_METADATA_COLUMN_NAME: [
                FileChunk(unit_ids=(0,), num_rows=1, size_bytes=1).to_metadata(),
                None,
                None,
                None,
                None,
            ],
        }
    )
    outputs = partition_files(
        iter([input_table]),
        MagicMock(),
        partitioner=RoundRobinPartitioner(
            _FileSizeEstimator(),
            hints=PartitionHints(
                min_bucket_size=2, max_bucket_size=10, num_buckets=num_buckets
            ),
        ),
    )

    assert [output.to_pydict() for output in outputs] == [
        input_table.take(rows).to_pydict() for rows in expected_rows
    ]


def test_weighted_round_robin_partitioner_can_emit_before_overflow():
    partitioner = WeightedRoundRobinPartitioner(
        num_buckets=1,
        min_bucket_size=1,
        max_bucket_size=3,
        emit_before_overflow=True,
    )

    partitioner.add_item("a", 2)
    partitioner.add_item("b", 2)

    assert partitioner.has_partition()
    assert partitioner.next_partition() == ["a"]

    partitioner.finalize()
    assert partitioner.has_partition()
    assert partitioner.next_partition() == ["b"]


def test_weighted_round_robin_keeps_default_unknown_weight_grouping():
    partitioner = WeightedRoundRobinPartitioner(
        num_buckets=2,
        min_bucket_size=1,
        max_bucket_size=3,
        emit_before_overflow=True,
    )
    for item in range(5):
        partitioner.add_item(item, None)
    partitioner.finalize()

    assert [partitioner.next_partition(), partitioner.next_partition()] == [
        [0, 2, 4],
        [1, 3],
    ]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
