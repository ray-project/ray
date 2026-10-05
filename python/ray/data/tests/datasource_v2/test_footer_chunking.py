"""End-to-end reads through the footer-based Parquet chunking path.

Drives ``FooterFileIndexer`` via ``ray.data.read_parquet``, covering
predicate / limit / projection push-down. The pure-logic pieces
(coalescing, bin packing) are unit-tested in
``python/ray/data/tests/unit/datasource_v2/test_online_bin_packer.py``.
"""

import pytest

# ---------------------------------------------------------------------------
# End-to-end through ray.data.read_parquet (footer path)
# ---------------------------------------------------------------------------

_N_PER_FILE = 400
_N_FILES = 3


@pytest.fixture
def footer_parquet(tmp_path, monkeypatch):
    """Write a small multi-row-group Parquet dataset."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    from ray.data.context import DataContext

    monkeypatch.setenv("RAY_DATA_PARQUET_FOOTER_NUM_ACTORS", "2")
    monkeypatch.setenv("RAY_DATA_PARQUET_FOOTER_BATCH_SIZE", "2")

    ctx = DataContext.get_current()
    prev_v2 = ctx.use_datasource_v2
    ctx.use_datasource_v2 = True

    for f in range(_N_FILES):
        start = f * _N_PER_FILE
        table = pa.table(
            {
                "id": list(range(start, start + _N_PER_FILE)),
                "val": [f"v{i}" for i in range(_N_PER_FILE)],
            }
        )
        pq.write_table(table, str(tmp_path / f"part_{f}.parquet"), row_group_size=100)

    try:
        yield str(tmp_path)
    finally:
        ctx.use_datasource_v2 = prev_v2


def test_e2e_footer_read_matches_expected(footer_parquet):
    import ray

    total = _N_PER_FILE * _N_FILES
    ds = ray.data.read_parquet(footer_parquet)
    assert ds.count() == total
    assert sorted(r["id"] for r in ds.take_all()) == list(range(total))


@pytest.mark.parametrize(
    "op, expected",
    [
        pytest.param(lambda ds: ds.filter(expr="id < 50").count(), 50, id="filter"),
        pytest.param(lambda ds: ds.limit(10).count(), 10, id="limit"),
        pytest.param(
            lambda ds: ds.select_columns(["id"]).schema().names,
            ["id"],
            id="projection",
        ),
    ],
)
def test_e2e_footer_pushdowns(footer_parquet, op, expected):
    import ray

    assert op(ray.data.read_parquet(footer_parquet)) == expected


# Filter and limit push down together: the limit stops listing early once the
# ``num_rows`` of *fully matched* row groups reaches it, so that classification
# has to be exact. Nulls are the interesting case -- Parquet min/max statistics
# are computed over non-null values only, so a group whose non-null values all
# satisfy the filter looks fully matched by bounds alone while its null rows do
# not survive. Deliberately lopsided at 10 survivors per 100 rows: the stop is
# evaluated per file, so a fixture with a small shortfall can pass by luck when
# the last file's overshoot covers the deficit.
_NULL_FILES = 20
_NULL_ROWS_PER_FILE = 100
_NULL_TOTAL_SURVIVORS = _NULL_FILES * 10


@pytest.fixture
def nullable_parquet(tmp_path, monkeypatch):
    """Multi-file, multi-row-group data whose filtered column holds nulls."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    from ray.data.context import DataContext

    monkeypatch.setenv("RAY_DATA_PARQUET_FOOTER_NUM_ACTORS", "2")
    monkeypatch.setenv("RAY_DATA_PARQUET_FOOTER_BATCH_SIZE", "2")

    ctx = DataContext.get_current()
    prev_v2 = ctx.use_datasource_v2
    ctx.use_datasource_v2 = True

    for f in range(_NULL_FILES):
        ids = [
            3 + f * 1000 + i if i % 10 == 0 else None
            for i in range(_NULL_ROWS_PER_FILE)
        ]
        pq.write_table(
            pa.table({"id": pa.array(ids, pa.int64())}),
            str(tmp_path / f"part_{f}.parquet"),
            row_group_size=25,
        )

    try:
        yield str(tmp_path)
    finally:
        ctx.use_datasource_v2 = prev_v2


@pytest.mark.parametrize(
    "limit", [1, 10, 100, _NULL_TOTAL_SURVIVORS, 10 * _NULL_TOTAL_SURVIVORS]
)
def test_e2e_filter_then_limit_with_nulls(nullable_parquet, limit):
    """``filter(...).limit(n)`` delivers ``n`` rows whenever ``n`` survivors exist.

    If nulls were ever counted as survivors, listing would stop short and
    ``Limit`` would return fewer rows than asked for, with no error.
    """
    import ray
    from ray.data.expressions import col

    ds = ray.data.read_parquet(nullable_parquet)
    rows = ds.filter(expr=col("id") > 2).limit(limit).take_all()

    assert len(rows) == min(limit, _NULL_TOTAL_SURVIVORS)
    assert all(r["id"] is not None and r["id"] > 2 for r in rows)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
