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


def test_split_fully_matched_rows_contract():
    """The helper separates exact-counted rows from rows that must be read."""
    from ray.data._internal.datasource_v2.common.file_reader import (
        split_fully_matched,
    )

    matched = FileChunk(unit_ids=(0,), num_rows=5, size_bytes=10, fully_matched=True)
    partial = FileChunk(unit_ids=(1,), num_rows=3, size_bytes=10, fully_matched=False)
    manifest = FileManifest.construct_manifest(
        paths=["whole", "matched", "partial"],
        sizes=[10, 10, 10],
        chunk_metadatas=[None, matched.to_metadata(), partial.to_metadata()],
    )

    stub_num_rows, to_read = split_fully_matched(manifest)

    assert stub_num_rows == [5]
    assert to_read.paths.tolist() == ["whole", "partial"]
    assert to_read.file_chunk_metadatas[0] is None


def test_split_fully_matched_without_stubs_returns_the_input_manifest():
    """With nothing to stub the helper must not copy the manifest."""
    from ray.data._internal.datasource_v2.common.file_reader import (
        split_fully_matched,
    )

    partial = FileChunk(unit_ids=(0,), num_rows=5, size_bytes=10, fully_matched=False)
    manifest = FileManifest.construct_manifest(
        paths=["partial"], sizes=[10], chunk_metadatas=[partial.to_metadata()]
    )

    stub_num_rows, to_read = split_fully_matched(manifest)

    assert stub_num_rows == []
    assert to_read is manifest


def test_split_fully_matched_with_every_row_matched_reads_nothing():
    from ray.data._internal.datasource_v2.common.file_reader import (
        split_fully_matched,
    )

    matched = FileChunk(unit_ids=(0,), num_rows=5, size_bytes=10, fully_matched=True)
    manifest = FileManifest.construct_manifest(
        paths=["matched"], sizes=[10], chunk_metadatas=[matched.to_metadata()]
    )

    stub_num_rows, to_read = split_fully_matched(manifest)

    assert stub_num_rows == [5]
    assert len(to_read) == 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
