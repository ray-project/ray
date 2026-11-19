"""End-to-end CSV listing through the header-reader actor pool.

Drives ``CsvFileIndexer`` via ``ray.data.read_csv`` with the pool's default
on, complementing the serial inline path in
``python/ray/data/tests/unit/datasource_v2/test_csv_datasource_v2.py``.
"""

import pytest

_N_FILES = 20
_N_ROWS = 30


@pytest.fixture
def split_csv(tmp_path, monkeypatch):
    """Write split files whose columns differ, so a swapped header shows."""
    monkeypatch.setenv("RAY_DATA_LINE_CHUNK_BYTES", "64")
    # More batches in flight than the two-per-actor window, so batches
    # resolve out of order and still land on their own files' chunks.
    monkeypatch.setenv("RAY_DATA_CSV_HEADER_NUM_ACTORS", "2")
    monkeypatch.setenv("RAY_DATA_CSV_HEADER_BATCH_SIZE", "3")

    for f in range(_N_FILES):
        rows = "".join(f"{k},2\n" for k in range(_N_ROWS))
        (tmp_path / f"{f}.csv").write_text(f"id{f},x\n" + rows)
    yield str(tmp_path)


def test_e2e_headers_from_the_actor_pool(split_csv):
    import ray

    ds = ray.data.read_csv(split_csv)
    assert ds.count() == _N_FILES * _N_ROWS
    rows = ds.take_all()
    assert set(ds.schema().names) == {f"id{f}" for f in range(_N_FILES)} | {"x"}
    for f in range(_N_FILES):
        assert sorted(row[f"id{f}"] for row in rows if row[f"id{f}"] is not None) == (
            list(range(_N_ROWS))
        )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
