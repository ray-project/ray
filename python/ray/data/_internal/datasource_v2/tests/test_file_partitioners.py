from unittest.mock import MagicMock

import numpy as np
import pyarrow as pa
import pytest

from ray.data._internal.datasource_v2.listing.file_manifest import (
    FILE_CHUNK_METADATA_COLUMN_NAME,
    FILE_SIZE_COLUMN_NAME,
    PATH_COLUMN_NAME,
    FileManifest,
)
from ray.data._internal.datasource_v2.listing.listing_utils import partition_files
from ray.data._internal.datasource_v2.partitioners.file_affinity_partitioner import (
    FileAffinityPartitioner,
)
from ray.data._internal.datasource_v2.readers.in_memory_size_estimator import (
    PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT,
    InMemorySizeEstimator,
    ParquetFooterDerivedInMemorySizeEstimator,
)
from ray.data._internal.weighted_round_robin import WeightedRoundRobinPartitioner


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


class _FileSizeStubEstimator(InMemorySizeEstimator):
    """In-memory size == the manifest's on-disk ``file_sizes`` (test control)."""

    def estimate_in_memory_sizes(self, manifest) -> np.ndarray:
        return np.asarray(manifest.file_sizes)


def _affinity_table(paths, sizes, metas):
    return pa.Table.from_pydict(
        {
            PATH_COLUMN_NAME: paths,
            FILE_SIZE_COLUMN_NAME: sizes,
            FILE_CHUNK_METADATA_COLUMN_NAME: metas,
        }
    )


def _affinity_outputs(table, max_bucket_size):
    return list(
        partition_files(
            iter([table]),
            MagicMock(),
            partitioner=FileAffinityPartitioner(
                in_memory_size_estimator=_FileSizeStubEstimator(),
                max_bucket_size=max_bucket_size,
            ),
        )
    )


def test_file_affinity_groups_by_file_and_bounds_size():
    # File "a": 4 chunks of size 1; "b": 1 chunk. max_bucket_size=2 -> "a"
    # splits into two 2-chunk partitions, "b" is its own. No partition mixes
    # files.
    table = _affinity_table(["a", "a", "a", "a", "b"], [1, 1, 1, 1, 1], [None] * 5)
    partitions = [o[PATH_COLUMN_NAME].to_pylist() for o in _affinity_outputs(table, 2)]
    assert partitions == [["a", "a"], ["a", "a"], ["b"]]
    assert all(len(set(p)) == 1 for p in partitions)  # each partition single-file


def test_file_affinity_interleaved_input_groups_by_file():
    # Chunks of "a" and "b" interleaved in the input still group per file.
    table = _affinity_table(["a", "b", "a", "b"], [1, 1, 1, 1], [None] * 4)
    partitions = [o[PATH_COLUMN_NAME].to_pylist() for o in _affinity_outputs(table, 2)]
    assert partitions == [["a", "a"], ["b", "b"]]


def test_file_affinity_small_file_is_single_partition():
    table = _affinity_table(["a", "a"], [1, 1], [None, None])
    partitions = [
        o[PATH_COLUMN_NAME].to_pylist() for o in _affinity_outputs(table, 100)
    ]
    assert partitions == [["a", "a"]]


def test_file_affinity_empty_input_emits_nothing():
    table = _affinity_table([], [], [])
    assert _affinity_outputs(table, 2) == []


def test_file_affinity_sorts_partition_chunks_by_row_group_start():
    # Row groups arrive out of order; the emitted partition is ascending.
    metas = [
        {"row_group_start": 2, "row_group_end": 3},
        {"row_group_start": 0, "row_group_end": 1},
        {"row_group_start": 1, "row_group_end": 2},
    ]
    table = _affinity_table(["a", "a", "a"], [1, 1, 1], metas)
    outputs = _affinity_outputs(table, 100)
    assert len(outputs) == 1
    starts = [
        cm["row_group_start"]
        for cm in outputs[0][FILE_CHUNK_METADATA_COLUMN_NAME].to_pylist()
    ]
    assert starts == [0, 1, 2]


def test_file_affinity_sorts_line_delimited_chunks_by_byte_offset():
    # FileAffinityPartitioner is the default for ALL V2 datasources, so it must
    # also order line-delimited (CSV/JSON) chunks by their byte offset, not only
    # Parquet row groups (covered above).
    metas = [
        {"chunk_byte_start_idx": 200, "chunk_byte_end_idx": 300},
        {"chunk_byte_start_idx": 0, "chunk_byte_end_idx": 100},
        {"chunk_byte_start_idx": 100, "chunk_byte_end_idx": 200},
    ]
    table = _affinity_table(["a", "a", "a"], [1, 1, 1], metas)
    outputs = _affinity_outputs(table, 100)
    assert len(outputs) == 1
    starts = [
        cm["chunk_byte_start_idx"]
        for cm in outputs[0][FILE_CHUNK_METADATA_COLUMN_NAME].to_pylist()
    ]
    assert starts == [0, 100, 200]


def test_footer_derived_estimator_reads_hint_and_falls_back():
    # The estimator reads each chunk's footer-derived ``in_memory_size`` hint and
    # falls back to ``file_size × ratio`` for chunks without one (e.g. a
    # whole-file ``None`` chunk on a corrupt footer / non-Parquet input).
    manifest = FileManifest.construct_manifest(
        ["a", "b", "c"],
        [10, 20, 30],
        [
            {"row_group_start": 0, "row_group_end": 1, "in_memory_size": 1234},
            None,
            {"row_group_start": 0, "row_group_end": 2, "in_memory_size": 5678},
        ],
    )
    sizes = ParquetFooterDerivedInMemorySizeEstimator().estimate_in_memory_sizes(
        manifest
    )
    assert list(sizes) == [
        1234.0,
        20 * PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT,
        5678.0,
    ]


def test_footer_derived_estimator_handles_none_file_size():
    # File sizes can be unavailable (e.g. HTTPFileSystem) -> a null size column
    # surfaces as None/NaN. With no footer hint, the estimate must coerce to 0.0
    # rather than raise TypeError/ValueError on ``float(...)``.
    manifest = FileManifest.construct_manifest(["a", "b"], [None, 50], [None, None])
    sizes = ParquetFooterDerivedInMemorySizeEstimator().estimate_in_memory_sizes(
        manifest
    )
    assert list(sizes) == [0.0, 50 * PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT]


def test_file_affinity_handles_none_size_and_nan_estimate_without_raising():
    # None file sizes (HTTPFileSystem) and a NaN in-memory estimate must not
    # crash ``add_input``'s int(...) coercions; both are treated as 0 and the
    # file is still partitioned.
    class _NanEstimator(InMemorySizeEstimator):
        def estimate_in_memory_sizes(self, manifest) -> np.ndarray:
            return np.full(len(manifest), np.nan)

    table = _affinity_table(["a", "a"], [None, None], [None, None])
    outputs = list(
        partition_files(
            iter([table]),
            MagicMock(),
            partitioner=FileAffinityPartitioner(
                in_memory_size_estimator=_NanEstimator(), max_bucket_size=100
            ),
        )
    )
    partitions = [o[PATH_COLUMN_NAME].to_pylist() for o in outputs]
    assert partitions == [["a", "a"]]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
