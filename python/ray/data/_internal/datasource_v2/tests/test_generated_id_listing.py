"""List-time skip tests for generated-ID checkpointing.

Covers the compact checkpoint block flowing through listing: both indexers
drop fully-checkpointed files and stamp the per-file checkpoint struct onto
manifest rows, and the struct column survives manifest reconstruction in the
partitioners.
"""


import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ray.data._internal.datasource_v2.listing.file_indexer import (
    NonSamplingFileIndexer,
)
from ray.data._internal.datasource_v2.listing.file_manifest import (
    FILE_FRAGMENTS_CHECKPOINT_COLUMN_NAME,
    FileManifest,
)
from ray.data._internal.datasource_v2.listing.footer_file_indexer import (
    FooterFileIndexer,
)
from ray.data._internal.datasource_v2.partitioners.online_bin_packer import (
    OnlineBinPacker,
)
from ray.data._internal.datasource_v2.partitioners.round_robin_partitioner import (
    RoundRobinPartitioner,
)
from ray.data.checkpoint.generated_id import (
    CHECKPOINTED_FILE_COLUMN_NAME,
    CHECKPOINTED_FILE_FRAGMENTS_COLUMN_NAME,
    CHECKPOINTED_FILE_FRAGMENTS_TYPE,
    CHECKPOINTED_GENERATED_ID_COLUMN_TABLE_SCHEMA,
    CheckpointFragmentsInfo,
)


def _file_fragments_value(fragments):
    """One file's struct value from (fragment_id, num_rows, committed) tuples."""
    entries = []
    num_fully = 0
    for fragment_id, num_rows, committed in fragments:
        fully = len(committed) == num_rows
        num_fully += fully
        entries.append(
            {
                "fragment_id": fragment_id,
                "num_rows": num_rows,
                "num_checkpointed_rows": len(committed),
                "checkpointed_row_ids": (
                    [] if fully else [i in set(committed) for i in range(num_rows)]
                ),
            }
        )
    return {
        "num_fragments": len(fragments),
        "fully_checkpointed": num_fully == len(fragments),
        "fragments": entries,
    }


def _checkpoint_block(files):
    """Compact checkpoint table from {path: [(fragment_id, num_rows, committed)]}."""
    return pa.table(
        {
            CHECKPOINTED_FILE_COLUMN_NAME: list(files.keys()),
            CHECKPOINTED_FILE_FRAGMENTS_COLUMN_NAME: pa.array(
                [_file_fragments_value(f) for f in files.values()],
                type=CHECKPOINTED_FILE_FRAGMENTS_TYPE,
            ),
        },
        schema=CHECKPOINTED_GENERATED_ID_COLUMN_TABLE_SCHEMA,
    )


def _write_parquet(path: str, num_rows: int, row_group_size: int):
    pq.write_table(
        pa.table({"x": list(range(num_rows))}), path, row_group_size=row_group_size
    )


def _fully_checkpointed_flags(manifest: FileManifest):
    column = manifest.file_fragments_checkpoint
    assert column is not None
    return [
        None if scalar.as_py() is None else scalar["fully_checkpointed"].as_py()
        for scalar in column
    ]


# --- FileManifest column behavior ---


def test_manifest_checkpoint_column_optional():
    manifest = FileManifest.construct_manifest(
        paths=["a"], sizes=[1], chunk_metadatas=[None]
    )
    assert manifest.file_fragments_checkpoint is None
    assert FILE_FRAGMENTS_CHECKPOINT_COLUMN_NAME not in manifest.as_block().column_names


def test_manifest_checkpoint_column_from_mixed_values():
    """construct_manifest accepts infos, raw scalars, and None per row."""
    scalar = pa.array(
        [_file_fragments_value([(0, 2, [0])])], type=CHECKPOINTED_FILE_FRAGMENTS_TYPE
    )[0]
    null_scalar = pa.array([None], type=CHECKPOINTED_FILE_FRAGMENTS_TYPE)[0]
    info = CheckpointFragmentsInfo(path="b", checkpointed_file_fragments=scalar)
    never_seen = CheckpointFragmentsInfo(path="c", checkpointed_file_fragments=None)

    manifest = FileManifest.construct_manifest(
        paths=["a", "b", "c", "d"],
        sizes=[1, 1, 1, 1],
        chunk_metadatas=[None] * 4,
        checkpoint_file_fragments=[scalar, info, never_seen, null_scalar],
    )
    column = manifest.file_fragments_checkpoint
    assert column is not None
    assert column.type == CHECKPOINTED_FILE_FRAGMENTS_TYPE
    assert [s.is_valid for s in column] == [True, True, False, False]


def test_manifest_checkpoint_column_survives_concat_and_shuffle():
    scalar = pa.array(
        [_file_fragments_value([(0, 2, [0])])], type=CHECKPOINTED_FILE_FRAGMENTS_TYPE
    )[0]
    m1 = FileManifest.construct_manifest(
        paths=["a"],
        sizes=[1],
        chunk_metadatas=[None],
        checkpoint_file_fragments=[scalar],
    )
    m2 = FileManifest.construct_manifest(
        paths=["b"],
        sizes=[2],
        chunk_metadatas=[None],
        checkpoint_file_fragments=[None],
    )
    merged = FileManifest.concat([m1, m2])
    assert [s.is_valid for s in merged.file_fragments_checkpoint] == [True, False]

    shuffled = merged.shuffle(seed=42)
    by_path = dict(zip(shuffled.paths, shuffled.file_fragments_checkpoint))
    assert by_path["a"].is_valid and not by_path["b"].is_valid


# --- Indexer skip + stamp ---


@pytest.fixture
def three_file_dataset(tmp_path):
    """Files a (2 row groups, fully done), b (partial), c (never seen)."""
    paths = {}
    for name in ("a", "b", "c"):
        path = str(tmp_path / f"{name}.parquet")
        _write_parquet(path, num_rows=6, row_group_size=3)
        paths[name] = path
    checkpoint_block = _checkpoint_block(
        {
            paths["a"]: [(0, 3, [0, 1, 2]), (1, 3, [0, 1, 2])],
            paths["b"]: [(0, 3, [1])],
        }
    )
    return tmp_path, paths, checkpoint_block


@pytest.mark.parametrize("indexer_cls", [NonSamplingFileIndexer, FooterFileIndexer])
def test_indexer_drops_fully_checkpointed_and_stamps(indexer_cls, three_file_dataset):
    tmp_path, paths, checkpoint_block = three_file_dataset
    from pyarrow.fs import LocalFileSystem

    indexer = indexer_cls(ignore_missing_paths=False)
    manifests = list(
        indexer.list_files(
            pa.array([str(tmp_path)]),
            filesystem=LocalFileSystem(),
            checkpoint_ids=checkpoint_block,
        )
    )
    merged = FileManifest.concat(manifests)

    listed_paths = set(merged.paths)
    assert paths["a"] not in listed_paths
    assert {paths["b"], paths["c"]} == listed_paths

    flags = dict(zip(merged.paths, _fully_checkpointed_flags(merged)))
    assert flags[paths["b"]] is False  # partial file, stamped struct
    assert flags[paths["c"]] is None  # never seen -> null struct


@pytest.mark.parametrize("indexer_cls", [NonSamplingFileIndexer, FooterFileIndexer])
def test_indexer_without_checkpoint_block_unchanged(indexer_cls, three_file_dataset):
    tmp_path, paths, _ = three_file_dataset
    from pyarrow.fs import LocalFileSystem

    indexer = indexer_cls(ignore_missing_paths=False)
    manifests = list(
        indexer.list_files(pa.array([str(tmp_path)]), filesystem=LocalFileSystem())
    )
    merged = FileManifest.concat(manifests)
    assert set(merged.paths) == set(paths.values())
    assert merged.file_fragments_checkpoint is None


# --- Partitioners preserve the column ---


class _FixedSizeEstimator:
    def estimate_in_memory_sizes(self, manifest):
        return [100] * len(manifest)


def _stamped_manifest(paths):
    """Manifest whose even-indexed rows carry a checkpoint struct."""
    scalar_values = [
        _file_fragments_value([(0, 2, [0])]) if i % 2 == 0 else None
        for i in range(len(paths))
    ]
    return FileManifest.construct_manifest(
        paths=paths,
        sizes=[10] * len(paths),
        chunk_metadatas=[None] * len(paths),
        checkpoint_file_fragments=[
            pa.array([v], type=CHECKPOINTED_FILE_FRAGMENTS_TYPE)[0]
            for v in scalar_values
        ],
    )


def test_round_robin_partitioner_preserves_checkpoint_column():
    partitioner = RoundRobinPartitioner(
        _FixedSizeEstimator(), min_bucket_size=1, max_bucket_size=200, num_buckets=2
    )
    paths = [f"f{i}.parquet" for i in range(4)]
    partitioner.add_input(_stamped_manifest(paths))
    partitioner.finalize()

    out_flags = {}
    while partitioner.has_partition():
        manifest = partitioner.next_partition()
        out_flags.update(zip(manifest.paths, _fully_checkpointed_flags(manifest)))
    assert set(out_flags) == set(paths)
    # Even-indexed files carried a struct; odd ones were null.
    for i, path in enumerate(paths):
        assert (out_flags[path] is not None) == (i % 2 == 0)


def test_round_robin_partitioner_without_column():
    partitioner = RoundRobinPartitioner(
        _FixedSizeEstimator(), min_bucket_size=1, max_bucket_size=200, num_buckets=2
    )
    partitioner.add_input(
        FileManifest.construct_manifest(
            paths=["a", "b"], sizes=[1, 1], chunk_metadatas=[None, None]
        )
    )
    partitioner.finalize()
    while partitioner.has_partition():
        assert partitioner.next_partition().file_fragments_checkpoint is None


def test_online_bin_packer_preserves_checkpoint_column():
    packer = OnlineBinPacker(max_bin_bytes=15)
    paths = [f"f{i}.parquet" for i in range(4)]
    packer.add_input(_stamped_manifest(paths))
    packer.finalize()

    out_flags = {}
    while packer.has_partition():
        manifest = packer.next_partition()
        out_flags.update(zip(manifest.paths, _fully_checkpointed_flags(manifest)))
    assert set(out_flags) == set(paths)
    for i, path in enumerate(paths):
        assert (out_flags[path] is not None) == (i % 2 == 0)


def test_online_bin_packer_without_column():
    packer = OnlineBinPacker(max_bin_bytes=15)
    packer.add_input(
        FileManifest.construct_manifest(
            paths=["a", "b"], sizes=[1, 1], chunk_metadatas=[None, None]
        )
    )
    packer.finalize()
    while packer.has_partition():
        assert packer.next_partition().file_fragments_checkpoint is None


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
