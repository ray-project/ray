from unittest.mock import MagicMock

import numpy as np
import pyarrow as pa
import pytest

from ray.data._internal.datasource_v2.listing.file_manifest import (
    FILE_CHUNK_METADATA_COLUMN_NAME,
    FILE_SIZE_COLUMN_NAME,
    PATH_COLUMN_NAME,
)
from ray.data._internal.datasource_v2.listing.listing_utils import partition_files
from ray.data._internal.datasource_v2.partitioners.round_robin_partitioner import (
    RoundRobinPartitioner,
)
from ray.data._internal.datasource_v2.readers.in_memory_size_estimator import (
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
            FILE_CHUNK_METADATA_COLUMN_NAME: [None, None, None],
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


@pytest.mark.parametrize("size", [0, 1])
def test_round_robin_emits_bounded_partitions_before_input_ends(size):
    from ray.data._internal.datasource_v2.readers.in_memory_size_estimator import (
        IdentityInMemorySizeEstimator,
    )

    consumed = 0

    def inputs():
        nonlocal consumed
        for index in range(100):
            consumed += 1
            yield pa.table(
                {
                    PATH_COLUMN_NAME: [str(index)],
                    FILE_SIZE_COLUMN_NAME: [size],
                    FILE_CHUNK_METADATA_COLUMN_NAME: [None],
                }
            )

    outputs = iter(
        partition_files(
            inputs(),
            MagicMock(),
            partitioner=RoundRobinPartitioner(
                in_memory_size_estimator=IdentityInMemorySizeEstimator(),
                num_buckets=2,
                min_bucket_size=1000,
                max_bucket_size=10000,
                max_items_per_bucket=3,
            ),
        )
    )
    first = next(outputs)
    assert consumed <= 6
    partitions = [first, *outputs]
    assert all(len(partition) <= 3 for partition in partitions)
    assert sorted(
        int(path)
        for partition in partitions
        for path in partition[PATH_COLUMN_NAME].to_pylist()
    ) == list(range(100))


def test_round_robin_partitioner_preserves_contiguous_input_order():
    input_table = pa.Table.from_pydict(
        {
            PATH_COLUMN_NAME: ["0", "1", "2", "3"],
            FILE_SIZE_COLUMN_NAME: [10, 5, 5, 10],
            FILE_CHUNK_METADATA_COLUMN_NAME: [None] * 4,
        }
    )

    class FileSizeEstimator(InMemorySizeEstimator):
        def estimate_in_memory_sizes(self, manifest) -> np.ndarray:
            return manifest.file_sizes

    outputs = partition_files(
        iter([input_table]),
        MagicMock(),
        partitioner=RoundRobinPartitioner(
            in_memory_size_estimator=FileSizeEstimator(),
            num_buckets=2,
            min_bucket_size=10,
            max_bucket_size=15,
            preserve_order=True,
        ),
    )

    partitions = [output[PATH_COLUMN_NAME].to_pylist() for output in outputs]
    assert partitions == [["0", "1"], ["2", "3"]]


def test_round_robin_partitioner_enforces_contiguous_bucket_count():
    input_blocks = [
        pa.Table.from_pydict(
            {
                PATH_COLUMN_NAME: [str(index)],
                FILE_SIZE_COLUMN_NAME: [1],
                FILE_CHUNK_METADATA_COLUMN_NAME: [None],
            }
        )
        for index in range(4)
    ]

    class FileSizeEstimator(InMemorySizeEstimator):
        def estimate_in_memory_sizes(self, manifest) -> np.ndarray:
            return manifest.file_sizes

    partitioner = RoundRobinPartitioner(
        in_memory_size_estimator=FileSizeEstimator(),
        num_buckets=2,
        min_bucket_size=10,
        max_bucket_size=100,
        preserve_order=True,
        enforce_num_buckets=True,
    )
    outputs = partition_files(iter(input_blocks), MagicMock(), partitioner=partitioner)

    assert partitioner.requires_global_input
    assert [output[PATH_COLUMN_NAME].to_pylist() for output in outputs] == [
        ["0", "1"],
        ["2", "3"],
    ]


@pytest.mark.parametrize("file_size", [1, 100, 200])
def test_explicit_block_target_survives_single_file_and_safety_flush(file_size):
    from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest
    from ray.data._internal.datasource_v2.readers.in_memory_size_estimator import (
        IdentityInMemorySizeEstimator,
    )

    manifest = FileManifest.construct_manifest(
        paths=["data.csv"], sizes=[file_size], chunk_metadatas=[None]
    )
    outputs = list(
        partition_files(
            [manifest.as_block()],
            MagicMock(),
            partitioner=RoundRobinPartitioner(
                in_memory_size_estimator=IdentityInMemorySizeEstimator(),
                num_buckets=4,
                min_bucket_size=1,
                max_bucket_size=100,
                enforce_num_buckets=True,
            ),
        )
    )
    assert len(outputs) == 1
    output = FileManifest(outputs[0])
    assert output.paths.tolist() == ["data.csv"]
    assert output.output_split_factor == 4


@pytest.mark.parametrize("enforce_num_buckets", [False, True])
def test_round_robin_partitioner_caps_items_per_ordered_bucket(
    enforce_num_buckets,
):
    input_table = pa.Table.from_pydict(
        {
            PATH_COLUMN_NAME: [str(index) for index in range(5)],
            FILE_SIZE_COLUMN_NAME: [1] * 5,
            FILE_CHUNK_METADATA_COLUMN_NAME: [None] * 5,
        }
    )

    class FileSizeEstimator(InMemorySizeEstimator):
        def estimate_in_memory_sizes(self, manifest) -> np.ndarray:
            return manifest.file_sizes

    outputs = partition_files(
        iter([input_table]),
        MagicMock(),
        partitioner=RoundRobinPartitioner(
            in_memory_size_estimator=FileSizeEstimator(),
            num_buckets=1,
            min_bucket_size=0,
            max_bucket_size=100,
            preserve_order=True,
            enforce_num_buckets=enforce_num_buckets,
            max_items_per_bucket=2,
        ),
    )

    partitions = [output[PATH_COLUMN_NAME].to_pylist() for output in outputs]
    assert partitions == [["0", "1"], ["2", "3"], ["4"]]
    assert max(map(len, partitions)) == 2


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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
