"""Unit tests for the manifest chunk-metadata types in DataSourceV2."""
import pytest

from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    FileManifest,
    UnitRun,
)


def test_unit_run_defaults():
    run = UnitRun(unit_ids=(3,), num_rows=5, size_bytes=10)
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


def test_unit_run_round_trips_through_a_manifest():
    # Arrow stores the row's tuples as lists and its ints as numpy scalars;
    # from_metadata must hand back the run that was written.
    run = UnitRun(
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
    assert UnitRun.from_metadata(manifest.file_chunk_metadatas[0]) == run


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
