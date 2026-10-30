"""Unit tests for the manifest chunk-metadata types in DataSourceV2."""
import pytest

from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    FileChunk,
    FileManifest,
)


def test_file_chunk_defaults():
    run = FileChunk(unit_ids=(3,), num_rows=5, size_bytes=10)
    assert run.fully_matched is True
    assert run.unit_sizes == () and run.unit_rows == ()
    assert run.to_metadata() == {
        "unit_ids": (3,),
        "num_rows": 5,
        "size_bytes": 10,
        "fully_matched": True,
        "unit_sizes": (),
        "unit_rows": (),
        "unit_start_rows": (),
        "file_num_units": None,
    }


def test_file_chunk_rejects_a_breakdown_that_does_not_match_unit_ids():
    with pytest.raises(AssertionError, match="unit_sizes has 1 entries for 2"):
        FileChunk(unit_ids=(0, 1), num_rows=2, size_bytes=20, unit_sizes=(10,))
    with pytest.raises(AssertionError, match="unit_rows has 3 entries for 2"):
        FileChunk(unit_ids=(0, 1), num_rows=2, size_bytes=20, unit_rows=(1, 1, 1))


def test_file_chunk_round_trips_through_a_manifest():
    # Arrow stores the row's tuples as lists and its ints as numpy scalars;
    # from_metadata must hand back the run that was written.
    run = FileChunk(
        unit_ids=(0, 1),
        num_rows=30,
        size_bytes=90,
        fully_matched=False,
        unit_sizes=(40, 50),
        unit_rows=(10, 20),
    )
    manifest = FileManifest.construct_manifest(
        paths=["a"], sizes=[90], chunk_metadatas=[run.to_metadata()]
    )
    assert FileChunk.from_metadata(manifest.file_chunk_metadatas[0]) == run


def test_positioned_chunk_split_and_merge_keep_aligned_payload():
    from ray.data._internal.datasource_v2.common.online_bin_packer import (
        OnlineBinPacker,
    )

    run = FileChunk(
        unit_ids=(0, 1, 2),
        num_rows=16,
        size_bytes=150,
        unit_sizes=(40, 50, 60),
        unit_rows=(5, 7, 4),
        unit_start_rows=(0, 5, 12),
        file_num_units=3,
    )
    packer = OnlineBinPacker(100, split_coalesced=True)
    packer.add_input(
        FileManifest.construct_manifest(
            paths=["a"], sizes=[1000], chunk_metadatas=[run.to_metadata()]
        )
    )
    packer.finalize()
    units = []
    while packer.has_partition():
        for metadata in packer.next_partition().file_chunk_metadatas:
            chunk = FileChunk.from_metadata(metadata)
            assert chunk.file_num_units == 3
            units.extend(
                zip(
                    chunk.unit_ids,
                    chunk.unit_rows,
                    chunk.unit_start_rows,
                    chunk.unit_sizes,
                )
            )
    assert sorted(units) == [(0, 5, 0, 40), (1, 7, 5, 50), (2, 4, 12, 60)]


def test_positioned_chunk_rejects_inconsistent_file_counts():
    from ray.data._internal.datasource_v2.common.online_bin_packer import (
        OnlineBinPacker,
    )

    runs = [
        FileChunk((0,), 5, 40, unit_rows=(5,), unit_start_rows=(0,), file_num_units=2),
        FileChunk((1,), 7, 50, unit_rows=(7,), unit_start_rows=(5,), file_num_units=3),
    ]
    packer = OnlineBinPacker(1000)
    packer.add_input(
        FileManifest.construct_manifest(
            paths=["a", "a"],
            sizes=[1000, 1000],
            chunk_metadatas=[r.to_metadata() for r in runs],
        )
    )
    packer.finalize()
    with pytest.raises(ValueError, match="Inconsistent"):
        packer.next_partition()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
