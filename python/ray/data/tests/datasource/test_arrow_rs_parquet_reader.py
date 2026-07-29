"""Correctness + integration tests for the experimental arrow-rs Parquet reader.

These run only when the native ``ray_data_arrow_rs`` extension is importable
(built via ``maturin`` from the crate under
``_internal/datasource_v2/native/ray_data_arrow_rs/``); otherwise the whole
module is skipped. They confirm that:

- reading through the arrow-rs path yields byte-identical columns to PyArrow,
- the native decode path actually runs (not the PyArrow fallback), and
- unsupported schemas transparently fall back to PyArrow and stay correct.
"""
import os

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

import ray
from ray.data.context import DataContext
from ray.data.datasource.path_util import _unwrap_protocol

ray_data_arrow_rs = pytest.importorskip("ray_data_arrow_rs")


@pytest.fixture
def restore_ctx():
    ctx = DataContext.get_current()
    v2, arrow_rs = ctx.use_datasource_v2, ctx.use_arrow_rs_parquet_reader
    try:
        ctx.use_datasource_v2 = True
        yield ctx
    finally:
        ctx.use_datasource_v2 = v2
        ctx.use_arrow_rs_parquet_reader = arrow_rs


def _flat_table(num_rows=20_000):
    rng = np.random.default_rng(0)
    return pa.table(
        {
            "id": pa.array(np.arange(num_rows, dtype=np.int64)),
            "x": pa.array(rng.random(num_rows)),
            "label": pa.array((np.arange(num_rows) % 5).astype(np.int32)),
            "name": pa.array([f"row-{i}" for i in range(num_rows)]),
        }
    )


def _read_sorted(path, use_arrow_rs, restore_ctx, **read_kwargs):
    restore_ctx.use_arrow_rs_parquet_reader = use_arrow_rs
    ds = ray.data.read_parquet(str(path), **read_kwargs)
    return pa.Table.from_pandas(ds.to_pandas()).sort_by("id")


def _read_arrow_sorted(path, use_arrow_rs, restore_ctx, **read_kwargs):
    """Like :func:`_read_sorted` but materializes straight to Arrow (no pandas
    round-trip). Required for columns whose Arrow type can't round-trip through
    pandas — e.g. multi-dimensional tensor extensions, which ``from_pandas``
    rejects ("Can only convert 1-dimensional array values"). Both readers go
    through this identically, so the ``.equals`` comparison stays a fair parity
    check on the reader output itself."""
    restore_ctx.use_arrow_rs_parquet_reader = use_arrow_rs
    ds = ray.data.read_parquet(str(path), **read_kwargs)
    return pa.concat_tables(ray.get(ds.to_arrow_refs())).sort_by("id")


@pytest.mark.parametrize("row_group_size", [20_000, 5_000])
def test_arrow_rs_parity_full_scan(tmp_path, restore_ctx, row_group_size):
    """arrow-rs and PyArrow produce identical tables (full scan)."""
    path = tmp_path / "data.parquet"
    table = _flat_table()
    pq.write_table(
        table, str(path), write_page_index=True, row_group_size=row_group_size
    )

    pa_tbl = _read_sorted(path, False, restore_ctx)
    rs_tbl = _read_sorted(path, True, restore_ctx)

    assert pa_tbl.num_rows == rs_tbl.num_rows == table.num_rows
    assert pa_tbl.equals(rs_tbl)


def test_arrow_rs_parity_with_projection(tmp_path, restore_ctx):
    path = tmp_path / "data.parquet"
    table = _flat_table()
    pq.write_table(table, str(path), write_page_index=True)

    pa_tbl = _read_sorted(path, False, restore_ctx, columns=["id", "x"])
    rs_tbl = _read_sorted(path, True, restore_ctx, columns=["id", "x"])
    assert rs_tbl.column_names == ["id", "x"]
    assert pa_tbl.equals(rs_tbl)


@pytest.mark.parametrize("row_group_size", [20_000, 5_000])
def test_arrow_rs_parity_sum(tmp_path, restore_ctx, row_group_size):
    """The aggregation workload benchmarked in Agents.md §3.3 (``ds.sum()``) must
    return identical results via the arrow-rs decode path and PyArrow. This is a
    decode-heavy / output-light consumer: the read decodes every value and the
    aggregation collapses it to a scalar, so it exercises full-column decode
    correctness end-to-end through Ray's aggregation, not just a raw table read.
    """
    path = tmp_path / "data.parquet"
    table = _flat_table()
    pq.write_table(
        table, str(path), write_page_index=True, row_group_size=row_group_size
    )

    # Ground truth from the source table, independent of either reader.
    expected_id = pc.sum(table["id"]).as_py()
    expected_label = pc.sum(table["label"]).as_py()

    restore_ctx.use_arrow_rs_parquet_reader = False
    pa_sum = ray.data.read_parquet(str(path)).sum(["id", "label"])
    restore_ctx.use_arrow_rs_parquet_reader = True
    rs_sum = ray.data.read_parquet(str(path)).sum(["id", "label"])

    assert rs_sum == pa_sum
    assert rs_sum["sum(id)"] == expected_id
    assert rs_sum["sum(label)"] == expected_label


def _read_crate_stream(path, **kwargs):
    """Read a file straight through the crate (bypassing the reader) into a
    single table, so we can force the K-split path via ``split_threshold_bytes``
    / ``k`` explicitly."""
    stream = ray_data_arrow_rs.read_row_groups(str(path), **kwargs)
    return pa.RecordBatchReader.from_stream(stream).read_all()


@pytest.mark.parametrize("k", [2, 4, 8])
def test_kspilt_parity_and_order(tmp_path, k):
    """The intra-fragment K-split path (single big row group, forced via
    ``split_threshold_bytes=0``) must be byte-identical to both the sequential
    (k=1) crate path and PyArrow, and preserve row order across the K parallel
    range workers.

    Row order is the load-bearing property here: the split decodes K disjoint
    row ranges on separate threads and merges them back. A merge bug would
    surface as a shuffled ``id`` column even when the row *set* is correct, so
    we assert the ``id`` column is exactly ``0..n-1`` in order.
    """
    num_rows = 50_000
    path = tmp_path / "big_single_rg.parquet"
    table = _flat_table(num_rows)
    # One row group covering all rows → a lone fragment Ray's pool can't split.
    pq.write_table(table, str(path), write_page_index=True, row_group_size=num_rows)
    assert pq.ParquetFile(str(path)).num_row_groups == 1

    # k=1 sequential (never splits) vs forced K-split (threshold=0).
    seq = _read_crate_stream(path, k=1)
    split = _read_crate_stream(path, k=k, split_threshold_bytes=0)

    # Byte-identical to the sequential path and to the source table.
    assert split.equals(seq)
    assert split.equals(table)
    # Order preserved across ranges: id is exactly 0..n-1, not just the right set.
    assert split.column("id").to_pylist() == list(range(num_rows))


def test_native_path_actually_runs(tmp_path):
    """Directly exercise the reader and confirm it calls the native extension
    rather than silently falling back to PyArrow."""
    import pyarrow.dataset as pds
    from pyarrow.fs import LocalFileSystem

    from ray.data._internal.datasource_v2.readers.arrow_rs_parquet_file_reader import (
        ArrowRsParquetFileReader,
    )

    path = tmp_path / "data.parquet"
    table = _flat_table()
    pq.write_table(table, str(path), write_page_index=True)

    calls = {"n": 0}
    orig = ray_data_arrow_rs.read_row_groups

    def wrapped(*a, **k):
        calls["n"] += 1
        return orig(*a, **k)

    ray_data_arrow_rs.read_row_groups = wrapped
    try:
        reader = ArrowRsParquetFileReader(
            filesystem=LocalFileSystem(), target_block_size=128 * 1024 * 1024
        )
        dataset = pds.dataset(str(path), format="parquet", filesystem=LocalFileSystem())
        fragment = next(dataset.get_fragments())
        scanner_kwargs = {
            "columns": None,
            "filter": None,
            "batch_size": reader._resolve_batch_size(dataset),
        }
        got = pa.concat_tables(
            list(reader._iter_fragment_tables(fragment, scanner_kwargs))
        )
    finally:
        ray_data_arrow_rs.read_row_groups = orig

    assert calls["n"] > 0, "native read_row_groups was not called (fell back)"
    assert got.sort_by("id").equals(table.sort_by("id"))


def _make_manifest(paths, sizes, chunk_metadatas):
    from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest

    return FileManifest.construct_manifest(paths, sizes, chunk_metadatas)


def test_native_read_is_pyarrow_free(tmp_path, monkeypatch):
    """The whole point of the ``read()`` rewrite: for a file the native reader
    supports, PyArrow must *never open it*. The footer, row-group layout, and
    decode all come from the crate (``read_metadata`` + ``read_row_groups``);
    ``pyarrow.dataset.dataset`` — the only way the base reader opens a Parquet
    file — must not be called at all.

    We drive ``reader.read(manifest)`` directly (not through
    ``ray.data.read_parquet``) so the assertion is scoped to the *read* stage:
    the listing/indexing stage does call pyarrow to enumerate row groups, and
    counting over a full pipeline execution would conflate the two. Here the
    manifest is handed in pre-built, so any ``pds.dataset`` call can only come
    from the reader itself.
    """
    import pyarrow.dataset as pds
    from pyarrow.fs import LocalFileSystem

    from ray.data._internal.datasource_v2.readers.arrow_rs_parquet_file_reader import (
        ArrowRsParquetFileReader,
    )

    path = tmp_path / "data.parquet"
    table = _flat_table()
    pq.write_table(table, str(path), write_page_index=True)

    # Spy: pyarrow.dataset.dataset must NOT be called for a supported native read.
    ds_calls = {"n": 0}
    orig_dataset = pds.dataset

    def spy_dataset(*a, **k):
        ds_calls["n"] += 1
        return orig_dataset(*a, **k)

    monkeypatch.setattr(pds, "dataset", spy_dataset)

    # Spy: the native crate must actually run (metadata footer read + decode),
    # so a "0 pyarrow calls" result can't be a silent no-op.
    native_calls = {"read_metadata": 0, "read_row_groups": 0}
    for name in native_calls:
        orig = getattr(ray_data_arrow_rs, name)

        def make_spy(orig, name):
            def spy(*a, **k):
                native_calls[name] += 1
                return orig(*a, **k)

            return spy

        monkeypatch.setattr(ray_data_arrow_rs, name, make_spy(orig, name))

    reader = ArrowRsParquetFileReader(
        filesystem=LocalFileSystem(), target_block_size=128 * 1024 * 1024
    )
    manifest = _make_manifest([str(path)], [os.path.getsize(path)], [None])
    got = pa.concat_tables(list(reader.read(manifest)))

    assert ds_calls["n"] == 0, (
        "pyarrow.dataset.dataset was called during a supported native read "
        "(pyarrow opened the file — the read is not pyarrow-free)"
    )
    assert native_calls["read_metadata"] > 0, "native footer read never ran"
    assert native_calls["read_row_groups"] > 0, "native decode never ran"
    assert got.sort_by("id").equals(table.sort_by("id"))


def _write_pickle_object_file(path, objs):
    import pickle

    from ray.data._internal.object_extensions.arrow import ArrowPythonObjectType

    ext_type = ArrowPythonObjectType()
    storage = pa.array([pickle.dumps(o) for o in objs], type=ext_type.storage_type)
    table = pa.table(
        {
            "id": pa.array(range(len(objs)), type=pa.int64()),
            "obj": pa.ExtensionArray.from_storage(ext_type, storage),
        }
    )
    pq.write_table(table, str(path), write_page_index=True)


def test_native_read_rejects_pickle_object_columns(tmp_path, monkeypatch):
    """The pyarrow path refuses to serve pickled-object columns without the
    explicit env opt-in — unpickling executes arbitrary code, so the guard is
    a security boundary, not a convenience. The native path must enforce the
    same gate. Regression test for the corpus `pickle_default` finding, where
    the native decode served the column with no error."""
    from pyarrow.fs import LocalFileSystem

    from ray.data._internal.datasource_v2.readers.arrow_rs_parquet_file_reader import (
        ArrowRsParquetFileReader,
    )

    marker = tmp_path / "exploit_marker"

    class Exploit:
        def __reduce__(self):
            return (os.system, (f"touch {marker}",))

    path = tmp_path / "data.parquet"
    _write_pickle_object_file(path, [Exploit()])

    monkeypatch.delenv("RAY_DATA_AUTOLOAD_PICKLE_OBJECT_SCALAR", raising=False)

    # Spy: the raise must come from the NATIVE path — if the file fell back,
    # the pyarrow reader's own guard would fire and this test would prove
    # nothing about the native one.
    native_calls = {"n": 0}
    orig = ray_data_arrow_rs.read_row_groups

    def spy(*a, **k):
        native_calls["n"] += 1
        return orig(*a, **k)

    monkeypatch.setattr(ray_data_arrow_rs, "read_row_groups", spy)

    reader = ArrowRsParquetFileReader(
        filesystem=LocalFileSystem(), target_block_size=128 * 1024 * 1024
    )
    manifest = _make_manifest([str(path)], [os.path.getsize(path)], [None])
    with pytest.raises(ValueError, match="arrow_pickled_object"):
        pa.concat_tables(list(reader.read(manifest)))

    assert native_calls["n"] > 0, "native decode never ran (pyarrow fallback?)"
    assert not marker.exists(), "pickle.load executed attacker code"


def test_native_read_allows_pickle_object_columns_with_env_var(tmp_path, monkeypatch):
    from pyarrow.fs import LocalFileSystem

    from ray.data._internal.datasource_v2.readers.arrow_rs_parquet_file_reader import (
        ArrowRsParquetFileReader,
    )

    path = tmp_path / "data.parquet"
    _write_pickle_object_file(path, [{"key": "value"}, {"key": "other"}])

    monkeypatch.setenv("RAY_DATA_AUTOLOAD_PICKLE_OBJECT_SCALAR", "1")

    reader = ArrowRsParquetFileReader(
        filesystem=LocalFileSystem(), target_block_size=128 * 1024 * 1024
    )
    manifest = _make_manifest([str(path)], [os.path.getsize(path)], [None])
    got = pa.concat_tables(list(reader.read(manifest))).sort_by("id")
    assert got.column("obj").to_pylist() == [{"key": "value"}, {"key": "other"}]


def _write_int96_file(path, num_rows=1_000):
    """INT96-physical timestamps, all pre-1970 and 1ns past a microsecond
    boundary — the values where decode-time unit coercion (floors) and a
    post-decode cast (truncates toward zero) differ by exactly one unit."""
    us_vals = [(i - num_rows) * 86_400_000_000 + i for i in range(num_rows)]
    table = pa.table(
        {
            "id": pa.array(range(num_rows), type=pa.int64()),
            "ts": pa.array([v * 1000 + 1 for v in us_vals], type=pa.timestamp("ns")),
        }
    )
    pq.write_table(
        table,
        str(path),
        use_deprecated_int96_timestamps=True,
        store_schema=False,
        write_page_index=True,
    )


def test_coerce_int96_kwarg_parity(tmp_path, restore_ctx):
    """`coerce_int96_timestamp_unit` must yield exactly what the base V2
    pyarrow reader yields. On the full V2 pipeline that is subtle: the pinned
    unified schema (kwarg-blind ``pq.read_schema``) casts the coerced values
    BACK to the inferred ns — so the kwarg's observable effect is
    ms-quantized-and-FLOORED *values* in an ns-typed column. A native ns
    decode plus cast can't reproduce the floor on pre-1970 values, so the
    reader falls back per file; this test pins the observable contract,
    however it's met. Regression test for the corpus `int96_coerce_ms`
    finding (native path returned raw ns, ignoring the kwarg)."""
    path = tmp_path / "int96.parquet"
    _write_int96_file(path)

    kw = {"dataset_kwargs": {"coerce_int96_timestamp_unit": "ms"}}
    expected = _read_arrow_sorted(
        path, use_arrow_rs=False, restore_ctx=restore_ctx, **kw
    )
    got = _read_arrow_sorted(path, use_arrow_rs=True, restore_ctx=restore_ctx, **kw)
    assert got.equals(expected)

    # The kwarg must have had its observable effect (guards against a "parity"
    # where both readers ignored it): values are ms-quantized and floored —
    # the raw values sit 1ns past a µs boundary, so flooring to ms lands on
    # the boundary and truncation toward zero would not.
    ts = expected.column("ts").cast(pa.int64()).to_pylist()
    raw = _read_arrow_sorted(path, use_arrow_rs=False, restore_ctx=restore_ctx)
    raw_ts = raw.column("ts").cast(pa.int64()).to_pylist()
    assert all(v % 1_000_000 == 0 for v in ts), "values not ms-quantized"
    assert all(v <= r for v, r in zip(ts, raw_ts)), "not floored (truncated?)"


def test_coerce_int96_kwarg_routes_int96_file_to_fallback(tmp_path, monkeypatch):
    """The kwarg-honoring mechanism: a file that decodes an INT96 column under
    `coerce_int96_timestamp_unit` must NOT go native; the same file without
    the kwarg must."""
    from pyarrow.fs import LocalFileSystem

    from ray.data._internal.datasource_v2.readers.arrow_rs_parquet_file_reader import (
        ArrowRsParquetFileReader,
    )

    path = tmp_path / "int96.parquet"
    _write_int96_file(path)

    native_calls = {"n": 0}
    orig = ray_data_arrow_rs.read_row_groups

    def spy(*a, **k):
        native_calls["n"] += 1
        return orig(*a, **k)

    monkeypatch.setattr(ray_data_arrow_rs, "read_row_groups", spy)

    def run(parquet_format_kwargs):
        reader = ArrowRsParquetFileReader(
            filesystem=LocalFileSystem(),
            target_block_size=128 * 1024 * 1024,
            parquet_format_kwargs=parquet_format_kwargs,
        )
        manifest = _make_manifest([str(path)], [os.path.getsize(path)], [None])
        return pa.concat_tables(list(reader.read(manifest)))

    native_calls["n"] = 0
    with_kwarg = run({"coerce_int96_timestamp_unit": "ms"})
    assert native_calls["n"] == 0, "int96 file went native despite the kwarg"
    assert with_kwarg.schema.field("ts").type == pa.timestamp("ms")

    native_calls["n"] = 0
    without_kwarg = run(None)
    assert native_calls["n"] > 0, "int96 file without the kwarg should stay native"
    assert without_kwarg.schema.field("ts").type == pa.timestamp("ns")


def test_native_chunked_read_row_hash_parity(tmp_path):
    """A chunked file (one manifest row per ``[row_group_start, row_group_end)``
    range) must produce byte-identical ``row_hash`` values via the native path
    and PyArrow.

    ``row_hash`` is seeded by ``(fragment_path, file_row_offset)`` per sub-
    fragment (:func:`_compute_row_hashes`), so this is the load-bearing test for
    :meth:`ArrowRsParquetFileReader._native_fragments_for_file`: it must emit one
    native fragment *per row group* seeded with that group's cumulative pre-filter
    row offset, exactly mirroring the base ``_fragments_from_chunk_metadata``. A
    single off-by-one in the offset accumulation would shift the hashes and this
    would catch it. We drive both readers on the *same* hand-built chunked
    manifest so the only variable is the fragment-offset logic.
    """
    from pyarrow.fs import LocalFileSystem

    from ray.data._internal.datasource_v2.chunkers.file_chunker import (
        ParquetFileChunkMetadata,
        create_chunk_metadata,
    )
    from ray.data._internal.datasource_v2.readers.arrow_rs_parquet_file_reader import (
        ArrowRsParquetFileReader,
    )
    from ray.data._internal.datasource_v2.readers.parquet_file_reader import (
        ParquetFileReader,
    )

    path = tmp_path / "data.parquet"
    table = _flat_table(20_000)
    # 4 row groups of 5k rows each.
    pq.write_table(table, str(path), write_page_index=True, row_group_size=5_000)
    assert pq.ParquetFile(str(path)).num_row_groups == 4

    # Two chunks over the same file: row groups [0,2) and [2,4). This forces the
    # per-row-group native fragment path with a non-zero offset on the 2nd chunk.
    chunks = [
        create_chunk_metadata(
            ParquetFileChunkMetadata, row_group_start=0, row_group_end=2
        ),
        create_chunk_metadata(
            ParquetFileChunkMetadata, row_group_start=2, row_group_end=4
        ),
    ]
    size = os.path.getsize(path)
    manifest = _make_manifest([str(path), str(path)], [size, size], chunks)

    def read_all(reader_cls):
        reader = reader_cls(
            filesystem=LocalFileSystem(),
            target_block_size=128 * 1024 * 1024,
            include_row_hash=True,
        )
        return pa.concat_tables(list(reader.read(manifest))).sort_by("id")

    rs_tbl = read_all(ArrowRsParquetFileReader)
    pa_tbl = read_all(ParquetFileReader)

    assert "row_hash" in rs_tbl.column_names
    assert rs_tbl.num_rows == table.num_rows
    assert rs_tbl.equals(pa_tbl)


def test_native_read_include_paths_parity(tmp_path):
    """``include_paths`` synthesis (the ``path`` column) must match PyArrow when
    the file is read natively. Driven on the same whole-file manifest through
    both readers."""
    from pyarrow.fs import LocalFileSystem

    from ray.data._internal.datasource_v2.readers.arrow_rs_parquet_file_reader import (
        ArrowRsParquetFileReader,
    )
    from ray.data._internal.datasource_v2.readers.parquet_file_reader import (
        ParquetFileReader,
    )

    path = tmp_path / "data.parquet"
    table = _flat_table()
    pq.write_table(table, str(path), write_page_index=True)
    manifest = _make_manifest([str(path)], [os.path.getsize(path)], [None])

    def read_all(reader_cls):
        reader = reader_cls(
            filesystem=LocalFileSystem(),
            target_block_size=128 * 1024 * 1024,
            include_paths=True,
        )
        return pa.concat_tables(list(reader.read(manifest))).sort_by("id")

    rs_tbl = read_all(ArrowRsParquetFileReader)
    pa_tbl = read_all(ParquetFileReader)

    assert "path" in rs_tbl.column_names
    assert set(rs_tbl.column("path").to_pylist()) == {str(path)}
    assert rs_tbl.equals(pa_tbl)


def test_native_read_partitioning_parity(tmp_path, restore_ctx):
    """A partition column (encoded in the directory path, absent from the file's
    on-disk schema) must be synthesized identically on the native path.

    This exercises native-path-specific planning: ``_plan_native_read`` derives
    ``on_disk_names`` from the *crate's* footer schema, which won't contain the
    partition column, so it must land in the synthesize set (not be read from the
    file). End-to-end through ``read_parquet`` so Ray's hive-partition detection
    drives the layout.
    """
    base = tmp_path / "parts"
    table = _flat_table(6_000)
    for g in range(3):
        sub = base / f"grp={g}"
        sub.mkdir(parents=True)
        part = table.slice(g * 2_000, 2_000)
        pq.write_table(part, str(sub / "data.parquet"), write_page_index=True)

    pa_tbl = _read_sorted(base, False, restore_ctx)
    rs_tbl = _read_sorted(base, True, restore_ctx)

    assert "grp" in rs_tbl.column_names
    assert pa_tbl.equals(rs_tbl)


def test_count_is_metadata_only_under_arrow_rs(tmp_path, restore_ctx):
    """``ds.count()`` is answered from listing metadata and never invokes the
    reader — so it is correct under the arrow-rs flag by construction, with zero
    native decode (a count scan reads no data columns, so there is no working set
    to shrink; handling it natively would buy no memory). This guards that the
    metadata short-circuit stays intact when the flag is on."""
    from ray.data._internal.datasource_v2.readers import (
        arrow_rs_parquet_file_reader as mod,
    )

    path = tmp_path / "data.parquet"
    table = _flat_table()
    pq.write_table(table, str(path), write_page_index=True)

    read_calls = {"n": 0}
    orig_read = mod.ArrowRsParquetFileReader.read

    def spy_read(self, *a, **k):
        read_calls["n"] += 1
        return orig_read(self, *a, **k)

    mod.ArrowRsParquetFileReader.read = spy_read
    try:
        restore_ctx.use_arrow_rs_parquet_reader = True
        count = ray.data.read_parquet(str(path)).count()
    finally:
        mod.ArrowRsParquetFileReader.read = orig_read

    assert count == table.num_rows
    assert read_calls["n"] == 0, "count unexpectedly invoked the reader"


def test_empty_projection_counts_natively_with_zero_decode(tmp_path, monkeypatch):
    """A column-less read (empty projection, no predicate) is answered from the
    footer row counts alone: no crate decode, no ``pds.dataset`` — strictly
    less work than PyArrow's stub-column scan. The yielded tables are
    zero-column with the right ``num_rows``; ``_postprocess``'s stub guard
    re-adds the row-preserving stub, exactly as on the base path."""
    import pyarrow.dataset as pds
    from pyarrow.fs import LocalFileSystem

    from ray.data._internal.datasource_v2.readers.arrow_rs_parquet_file_reader import (
        ArrowRsParquetFileReader,
    )

    path = tmp_path / "data.parquet"
    table = _flat_table()
    pq.write_table(table, str(path), write_page_index=True)
    manifest = _make_manifest([str(path)], [os.path.getsize(path)], [None])

    calls = {"decode": 0, "pds": 0}
    orig_decode = ray_data_arrow_rs.read_row_groups
    orig_dataset = pds.dataset

    def decode_spy(*a, **k):
        calls["decode"] += 1
        return orig_decode(*a, **k)

    def dataset_spy(*a, **k):
        calls["pds"] += 1
        return orig_dataset(*a, **k)

    monkeypatch.setattr(ray_data_arrow_rs, "read_row_groups", decode_spy)
    monkeypatch.setattr(pds, "dataset", dataset_spy)

    reader = ArrowRsParquetFileReader(
        filesystem=LocalFileSystem(),
        target_block_size=128 * 1024 * 1024,
        columns=[],
    )
    tables = list(reader.read(manifest))
    assert sum(t.num_rows for t in tables) == table.num_rows
    # The stub-column guard in ``_postprocess`` preserves row counts.
    assert all(len(t.column_names) == 1 for t in tables)
    # The whole answer came from the footer: nothing was decoded, and pyarrow
    # never opened the file.
    assert calls["decode"] == 0
    assert calls["pds"] == 0


def test_native_fragment_read_retries_transient_error(tmp_path, monkeypatch):
    """A transient I/O failure during native decode must be retried and
    recovered exactly like the PyArrow path — the native ``_NativeParquetFragment``
    flows through the same ``iterate_with_retry`` wrapper in
    ``_read_fragments_sequential``. We inject a one-shot retryable error (matching
    a default ``retried_io_errors`` pattern) into the crate's ``read_row_groups``
    and assert the read still returns byte-correct data and that the crate was
    re-invoked (the retry fired)."""
    from pyarrow.fs import LocalFileSystem

    from ray.data._internal.datasource_v2.readers.arrow_rs_parquet_file_reader import (
        ArrowRsParquetFileReader,
    )

    path = tmp_path / "data.parquet"
    table = _flat_table()
    pq.write_table(table, str(path), write_page_index=True)
    manifest = _make_manifest([str(path)], [os.path.getsize(path)], [None])

    calls = {"n": 0}
    orig = ray_data_arrow_rs.read_row_groups

    def flaky_read_row_groups(*a, **k):
        calls["n"] += 1
        if calls["n"] == 1:
            # A default-retryable message (context.DEFAULT_RETRIED_IO_ERRORS), so
            # no context mutation is needed. Raised before any batch is yielded.
            raise OSError("AWS Error SLOW_DOWN: injected transient failure")
        return orig(*a, **k)

    monkeypatch.setattr(ray_data_arrow_rs, "read_row_groups", flaky_read_row_groups)

    reader = ArrowRsParquetFileReader(
        filesystem=LocalFileSystem(), target_block_size=128 * 1024 * 1024
    )
    got = pa.concat_tables(list(reader.read(manifest)))

    assert calls["n"] >= 2, "native read was not retried after a transient error"
    assert got.sort_by("id").equals(table.sort_by("id"))


def test_filter_pushdown_prunes_row_groups(tmp_path, restore_ctx):
    """A pushed-down predicate is lowered to the native pruning IR and handed to
    the crate, which drops the row groups whose footer statistics prove no row
    can match — replacing PyArrow's ``fragment.subset``. On a sorted ``id`` over
    4 row groups, ``id >= 3000`` reaches the crate as a non-None ``predicate_json``
    that prunes to row group ``[3]`` (verified via ``select_row_groups``); a
    predicate no row group can satisfy yields an empty result. Results stay
    byte-correct because the Python post-filter is the final authority."""
    import pyarrow.dataset as pds
    from pyarrow.fs import LocalFileSystem

    from ray.data._internal.datasource_v2.readers.arrow_rs_parquet_file_reader import (
        ArrowRsParquetFileReader,
    )
    from ray.data.expressions import col

    path = tmp_path / "sorted.parquet"
    n = 4000
    table = pa.table(
        {
            "id": pa.array(np.arange(n, dtype=np.int64)),
            "x": pa.array(np.arange(n) * 0.5),
        }
    )
    pq.write_table(table, str(path), write_page_index=True, row_group_size=1000)

    # `read_row_groups(path, row_groups, columns, batch_size, budget, k,
    # split_threshold, predicate_json)` — capture the row_groups handed in and
    # the predicate_json (8th positional) the reader lowered.
    seen = []
    orig = ray_data_arrow_rs.read_row_groups

    def wrapped(path_, row_groups, *a, **k):
        predicate_json = a[5] if len(a) > 5 else k.get("predicate_json")
        seen.append((row_groups, predicate_json))
        return orig(path_, row_groups, *a, **k)

    def _run(predicate):
        # Mirror `read()`: the reader's Ray `Expr` predicate drives native
        # pruning, and its pyarrow form is the scanner filter used for the
        # post-decode row filter.
        reader = ArrowRsParquetFileReader(
            filesystem=LocalFileSystem(),
            target_block_size=128 * 1024 * 1024,
            predicate=predicate,
        )
        dataset = pds.dataset(str(path), format="parquet", filesystem=LocalFileSystem())
        fragment = next(dataset.get_fragments())
        batch_size = reader._resolve_batch_size(dataset)
        return list(
            reader._iter_fragment_tables(
                fragment,
                {
                    "columns": None,
                    "filter": predicate.to_pyarrow(),
                    "batch_size": batch_size,
                },
            )
        )

    ray_data_arrow_rs.read_row_groups = wrapped
    try:
        got = pa.concat_tables(_run(col("id") >= 3000))
        pruned_all = _run(col("id") >= 10**9)
    finally:
        ray_data_arrow_rs.read_row_groups = orig

    # The crate receives the fragment's full row-group list plus the lowered
    # predicate; pruning happens *inside* the crate now, not before the call.
    (rg0, pj0), (rg1, pj1) = seen
    assert rg0 == [0, 1, 2, 3], f"expected all row groups handed to crate: {rg0}"
    assert pj0 is not None, "predicate did not lower to a pruning IR"
    # Prove the IR actually prunes to the single satisfying row group.
    assert ray_data_arrow_rs.select_row_groups(str(path), pj0) == [3]
    assert got.sort_by("id").equals(table.slice(3000))
    # A fully-unsatisfiable predicate prunes every group inside the crate, so
    # the stream is empty (the crate is still invoked — one cheap footer read).
    assert pruned_all == []
    assert ray_data_arrow_rs.select_row_groups(str(path), pj1) == []


def test_filter_pushdown_e2e_parity(tmp_path, restore_ctx):
    """``ds.filter(expr=...)`` goes through the PredicatePushdown rule into the
    read; both readers must agree (and match ground truth) on a sorted
    multi-row-group file where pruning actually kicks in."""
    path = tmp_path / "sorted_e2e.parquet"
    n = 4000
    table = pa.table(
        {
            "id": pa.array(np.arange(n, dtype=np.int64)),
            "x": pa.array(np.arange(n) * 0.5),
        }
    )
    pq.write_table(table, str(path), write_page_index=True, row_group_size=1000)

    def _read(use_arrow_rs):
        restore_ctx.use_arrow_rs_parquet_reader = use_arrow_rs
        ds = ray.data.read_parquet(str(path)).filter(expr="id >= 3500")
        return pa.Table.from_pandas(ds.to_pandas()).sort_by("id")

    pa_tbl = _read(False)
    rs_tbl = _read(True)
    assert pa_tbl.num_rows == rs_tbl.num_rows == 500
    assert pa_tbl.equals(rs_tbl)


@pytest.mark.parametrize(
    "expr",
    [
        # Partial pruning across the middle two row groups (0 and 3 prune).
        "id >= 1500 and id < 2500",
        # Compound over multiple types, with a string equality conjunct.
        'id >= 1500 and id < 2500 and g == "g2"',
        # OR of two disjoint ranges — neither end row group prunes.
        "id < 500 or id >= 3500",
        # Float comparison drives the pruning column.
        "x >= 900.0 and x < 1100.0",
        # A predicate no row group can satisfy (fully pruned → empty).
        "id >= 100000000",
    ],
)
def test_filter_pushdown_compound_parity(tmp_path, restore_ctx, expr):
    """Native row-group pruning + Python post-filter must be byte-identical to
    the PyArrow v2 reader across compound predicates spanning int/float/string
    columns and multiple row groups. This is the backstop for native pruning
    being the sole mechanism: any over-pruning bug would drop rows PyArrow keeps
    and fail here.
    """
    path = tmp_path / "compound.parquet"
    n = 4000
    table = pa.table(
        {
            "id": pa.array(np.arange(n, dtype=np.int64)),
            "x": pa.array(np.arange(n) * 0.5),
            "g": pa.array([f"g{i % 5}" for i in range(n)]),
        }
    )
    pq.write_table(table, str(path), write_page_index=True, row_group_size=1000)

    def _read(use_arrow_rs):
        restore_ctx.use_arrow_rs_parquet_reader = use_arrow_rs
        # take_all() over sort() preserves the schema even for empty results
        # (to_pandas() on an empty dataset yields a 0-column frame).
        ds = ray.data.read_parquet(str(path)).filter(expr=expr).sort("id")
        return ds.take_all()

    pa_rows = _read(False)
    rs_rows = _read(True)
    # Row-for-row parity across every column (dicts compare all keys/values).
    assert rs_rows == pa_rows, (
        f"arrow-rs diverged from pyarrow for `{expr}`: "
        f"{len(rs_rows)} vs {len(pa_rows)} rows"
    )


def _gate_verdict(path, read_columns=None):
    """The support gate's verdict for a file, via a real fragment."""
    import pyarrow.dataset as pds
    from pyarrow.fs import LocalFileSystem

    from ray.data._internal.datasource_v2.readers.arrow_rs_parquet_file_reader import (
        ArrowRsParquetFileReader,
    )

    reader = ArrowRsParquetFileReader(filesystem=LocalFileSystem())
    frag = next(
        pds.dataset(
            str(path), format="parquet", filesystem=LocalFileSystem()
        ).get_fragments()
    )
    return reader._arrow_rs_supported(frag, read_columns)


@pytest.mark.parametrize(
    "colname,builder",
    [
        ("vals", lambda n: pa.array([[i, i + 1, None] for i in range(n)])),
        (
            "st",
            lambda n: pa.StructArray.from_arrays(
                [pa.array(np.arange(n)), pa.array(np.arange(n) * 0.5)],
                names=["a", "b"],
            ),
        ),
        (
            "st_nested",
            lambda n: pa.array(
                [{"a": [i, i + 1], "b": {"c": f"row-{i}"}} for i in range(n)]
            ),
        ),
    ],
)
def test_nested_column_native_parity(tmp_path, restore_ctx, colname, builder):
    """List, struct, and deeper struct/list nesting decode NATIVELY (the gate
    admits them) and stay byte-identical to PyArrow."""
    path = tmp_path / f"{colname}.parquet"
    n = 2000
    table = pa.table(
        {"id": pa.array(np.arange(n, dtype=np.int64)), colname: builder(n)}
    )
    pq.write_table(table, str(path), write_page_index=True)

    assert _gate_verdict(path) is True, f"{colname} should be native now"
    pa_tbl = _read_sorted(path, False, restore_ctx)
    rs_tbl = _read_sorted(path, True, restore_ctx)
    assert pa_tbl.equals(rs_tbl)


def _list_col_from_lengths(lengths, *, value_type=pa.int64(), null_rows=None, seed=0):
    """Build a ``list<value_type>`` array where row ``i`` holds ``lengths[i]``
    elements (deterministic but arbitrary values). ``null_rows`` is an optional
    boolean mask marking rows that are NULL lists — distinct from empty lists;
    those rows are forced to zero length because Parquet cannot store a null list
    that spans elements."""
    lengths = np.asarray(lengths, dtype=np.int64).copy()
    if null_rows is not None:
        null_rows = np.asarray(null_rows, dtype=bool)
        lengths[null_rows] = 0
    offsets = np.zeros(len(lengths) + 1, dtype=np.int32)
    np.cumsum(lengths, out=offsets[1:])
    total = int(offsets[-1])
    rng = np.random.default_rng(seed)
    if pa.types.is_string(value_type):
        values = pa.array([f"e{v}" for v in rng.integers(0, 10**6, total)])
    else:
        values = pa.array(rng.integers(0, 10**9, total), type=value_type)
    mask = None if null_rows is None else pa.array(null_rows)
    return pa.ListArray.from_arrays(pa.array(offsets), values, mask=mask)


@pytest.mark.parametrize(
    "shape,lengths_fn",
    [
        ("empty", lambda n: np.zeros(n, dtype=np.int64)),
        ("singleton", lambda n: np.ones(n, dtype=np.int64)),
        ("small_fixed", lambda n: np.full(n, 4, dtype=np.int64)),
        ("big_fixed", lambda n: np.full(n, 512, dtype=np.int64)),
        ("ascending", lambda n: np.arange(n, dtype=np.int64)),
        ("descending", lambda n: np.arange(n, dtype=np.int64)[::-1]),
        ("random", lambda n: np.random.default_rng(0).integers(0, 128, n)),
    ],
)
def test_list_length_shapes_native_parity(tmp_path, restore_ctx, shape, lengths_fn):
    """``list<int64>`` columns across every length distribution — all-empty,
    singleton, small/big fixed width, monotonically ascending and descending
    ramps, and random sizes — decode NATIVELY and stay byte-identical to PyArrow.

    The 2k-row fixed-length-3 parity test alone leaves the offset handling barely
    exercised: it never sees wide lists, empty rows, or non-uniform offsets, which
    is exactly where the crate's offset buffer and its footer bytes-per-row
    estimate are most likely to slip."""
    n = 1000
    table = pa.table(
        {
            "id": pa.array(np.arange(n, dtype=np.int64)),
            "vals": _list_col_from_lengths(lengths_fn(n)),
        }
    )
    path = tmp_path / f"list_{shape}.parquet"
    pq.write_table(table, str(path), write_page_index=True)

    assert _gate_verdict(path) is True, f"list<{shape}> should decode natively"
    pa_tbl = _read_sorted(path, False, restore_ctx)
    rs_tbl = _read_sorted(path, True, restore_ctx)
    assert pa_tbl.equals(rs_tbl)


def test_list_null_rows_and_string_values_parity(tmp_path, restore_ctx):
    """Null lists (list-level nulls, distinct from empty lists) and a
    ``list<string>`` column with variable sizes both decode natively and match
    PyArrow — covering non-int element types and the validity buffer alongside
    the offsets."""
    n = 500
    lengths = np.random.default_rng(1).integers(0, 20, n)
    null_rows = np.arange(n) % 7 == 0  # every 7th row is a NULL list
    table = pa.table(
        {
            "id": pa.array(np.arange(n, dtype=np.int64)),
            "ints": _list_col_from_lengths(lengths, null_rows=null_rows, seed=2),
            "strs": _list_col_from_lengths(
                lengths, value_type=pa.string(), null_rows=null_rows, seed=3
            ),
        }
    )
    path = tmp_path / "list_nulls.parquet"
    pq.write_table(table, str(path), write_page_index=True)

    assert _gate_verdict(path) is True
    pa_tbl = _read_sorted(path, False, restore_ctx)
    rs_tbl = _read_sorted(path, True, restore_ctx)
    assert pa_tbl.equals(rs_tbl)


def test_large_nested_byte_budget_batching(tmp_path, monkeypatch):
    """A LARGE nested read (variable-length lists + struct-of-string, decoded
    size many times the decode budget, one lone row group) must stream through
    the byte-budget path as many small batches — not one giant table — while
    staying byte-identical and in row order.

    The small parity tests (2k rows) fit in a single budget batch, so they never
    prove the batching math holds for nested types, where the footer's
    bytes-per-row estimate is coarser than for flat columns. The budget is
    shrunk to 1 MiB via monkeypatch (it's a module global read at call time), so
    this must run the reader in-process rather than through Ray workers.
    """
    import pyarrow.dataset as pds
    from pyarrow.fs import LocalFileSystem

    from ray.data._internal.datasource_v2.readers import (
        arrow_rs_parquet_file_reader as reader_mod,
    )

    n = 200_000
    rng = np.random.default_rng(0)
    # Variable-length list<int64> (avg ~6 elems, some empty) + struct{int, str}.
    lens = rng.integers(0, 12, n)
    offsets = np.zeros(n + 1, dtype=np.int32)
    np.cumsum(lens, out=offsets[1:])
    values = pa.array(rng.integers(0, 10**9, offsets[-1]), type=pa.int64())
    table = pa.table(
        {
            "id": pa.array(np.arange(n, dtype=np.int64)),
            "vals": pa.ListArray.from_arrays(pa.array(offsets), values),
            "st": pa.StructArray.from_arrays(
                [
                    pa.array(np.arange(n, dtype=np.int64)),
                    pa.array([f"payload-string-{i:012d}" for i in range(n)]),
                ],
                names=["a", "b"],
            ),
        }
    )
    path = tmp_path / "large_nested.parquet"
    # One row group covering all rows: the whole decode must be paced by the
    # byte budget, with no row-group boundaries helping out.
    pq.write_table(table, str(path), write_page_index=True, row_group_size=n)
    assert pq.ParquetFile(str(path)).num_row_groups == 1

    budget = 1024 * 1024
    monkeypatch.setattr(reader_mod, "_ARROW_RS_DECODE_BUDGET_BYTES", budget)
    assert table.nbytes > 10 * budget  # "large": decoded size >> budget

    calls = {"n": 0}
    orig = ray_data_arrow_rs.read_row_groups

    def wrapped(*a, **k):
        calls["n"] += 1
        return orig(*a, **k)

    monkeypatch.setattr(ray_data_arrow_rs, "read_row_groups", wrapped)
    reader = reader_mod.ArrowRsParquetFileReader(
        filesystem=LocalFileSystem(), target_block_size=128 * 1024 * 1024
    )
    dataset = pds.dataset(str(path), format="parquet", filesystem=LocalFileSystem())
    fragment = next(dataset.get_fragments())
    scanner_kwargs = {
        "columns": None,
        "filter": None,
        "batch_size": reader._resolve_batch_size(dataset),
    }
    batches = list(reader._iter_fragment_tables(fragment, scanner_kwargs))

    assert calls["n"] > 0, "native read_row_groups was not called (fell back)"
    # Streaming, not slurping: many batches, each near the budget (generous 8x
    # slack because the crate sizes rows from the footer's bytes-per-row, which
    # is approximate for variable-width nested data).
    assert len(batches) >= 5, f"expected many budget batches, got {len(batches)}"
    assert max(b.nbytes for b in batches) <= 8 * budget
    got = pa.concat_tables(batches)
    # In-order and byte-identical (no sort: order is part of the contract).
    assert got.equals(table)


def test_scanner_leak_signature_and_arrow_rs_avoids_it(tmp_path, monkeypatch):
    """Reproduce the PyArrow accumulation behind ray#49158 / apache/arrow#39808 and
    show the arrow-rs reader sidesteps it.

    The reported "leak" is ``pyarrow.dataset`` ``to_batches`` (the Scanner)
    accumulating ~the whole file in the Arrow allocator *regardless of batch_size*,
    whereas ``pq.ParquetFile.iter_batches`` (what the V2 reader uses) holds only ~a
    row group. We measure exactly as the issue did: ``pa.total_allocated_bytes()``
    tracked as a running max across the batch iteration.

    The arrow-rs reader decodes in Rust and hands batches across a zero-copy FFI
    boundary, so decoded data never enters the Arrow allocator at all — its
    Arrow-side peak is ~0, categorically below the row-group floor iter_batches
    pays. (This asserts Arrow-allocator accumulation only, which is deterministic;
    the Rust working set is invisible here and is validated by RSS/USS in the
    bench suite. It is a regression guard: the pyarrow tiers must stay ordered and
    arrow-rs must not start buffering decoded data on the Arrow side.)
    """
    import pyarrow.dataset as pds
    from pyarrow.fs import LocalFileSystem

    from ray.data._internal.datasource_v2.readers import (
        arrow_rs_parquet_file_reader as reader_mod,
    )

    # 800k rows of fat strings split into many small row groups, so "whole file"
    # (to_batches) and "one row group" (iter_batches) are clearly different scales.
    n = 800_000
    rng = np.random.default_rng(0)
    cols = {"id": pa.array(np.arange(n, dtype=np.int64))}
    for i in range(4):
        cols[f"s{i}"] = pa.array(
            [f"val-{rng.integers(0, 10**6)}-{'x' * 8}" for _ in range(n)]
        )
    table = pa.table(cols)
    file_mb = table.nbytes / 1024 / 1024
    path = tmp_path / "leaky.parquet"
    pq.write_table(
        table,
        str(path),
        row_group_size=50_000,
        write_page_index=True,
        compression="snappy",
    )
    n_rg = pq.ParquetFile(str(path)).num_row_groups
    assert n_rg >= 8  # need many groups for file-scale vs row-group-scale to differ
    del table, cols
    import gc

    gc.collect()

    def _peak_mb(make_iter):
        """Max Arrow-allocator bytes held at once across the iteration (MB) — the
        issue's own metric. Baseline-subtracted so unrelated allocations don't
        count; each batch is dropped so only what the reader *retains* is seen."""
        base = pa.total_allocated_bytes()
        mx = 0
        for batch in make_iter():
            mx = max(mx, pa.total_allocated_bytes() - base)
            del batch
        return mx / 1024 / 1024

    def _fragment():
        return next(pds.dataset(str(path), format="parquet").get_fragments())

    # (1) Scanner/to_batches accumulates ~the whole file, and does NOT shrink with
    #     batch_size — the leak signature.
    to_small = _peak_mb(lambda: _fragment().to_batches(batch_size=256))
    to_big = _peak_mb(lambda: _fragment().to_batches(batch_size=2048))
    assert to_small > 0.4 * file_mb, (to_small, file_mb)
    assert abs(to_small - to_big) < 0.3 * to_small, (to_small, to_big)

    # (2) ParquetFile.iter_batches (the V2 read path) holds only ~a row group —
    #     materially below to_batches. Better, but still a whole-row-group floor.
    it_small = _peak_mb(lambda: pq.ParquetFile(str(path)).iter_batches(batch_size=256))
    assert it_small < 0.6 * to_small, (it_small, to_small)

    # (3) arrow-rs: decoded data is Rust-owned and crosses via zero-copy FFI, so ~0
    #     lands in the Arrow allocator — below even the row-group floor.
    monkeypatch.setattr(reader_mod, "_ARROW_RS_DECODE_BUDGET_BYTES", 1024 * 1024)
    reader = reader_mod.ArrowRsParquetFileReader(
        filesystem=LocalFileSystem(), target_block_size=128 * 1024 * 1024
    )
    dataset = pds.dataset(str(path), format="parquet", filesystem=LocalFileSystem())
    frag = next(dataset.get_fragments())
    scanner_kwargs = {
        "columns": None,
        "filter": None,
        "batch_size": reader._resolve_batch_size(dataset),
    }
    rs_peak = _peak_mb(lambda: reader._iter_fragment_tables(frag, scanner_kwargs))
    assert rs_peak < 0.5 * it_small, (rs_peak, it_small)


def test_map_column_native_parity(tmp_path, restore_ctx):
    """Map columns decode NATIVELY (the gate admits them) and byte-identically to
    PyArrow — including null and empty map entries spread across multiple row
    groups. Verified empirically: the crate emits an identical ``MapType`` (same
    key/value field names) and the same values, so ``pa.Table.equals`` holds."""
    n = 500
    map_path = tmp_path / "map.parquet"
    map_table = pa.table(
        {
            "id": pa.array(np.arange(n, dtype=np.int64)),
            "m": pa.array(
                [
                    None
                    if i % 50 == 0
                    else ([] if i % 9 == 0 else [(f"k{i}", i), ("b", i * 2)])
                    for i in range(n)
                ],
                type=pa.map_(pa.string(), pa.int64()),
            ),
        }
    )
    pq.write_table(map_table, str(map_path), write_page_index=True, row_group_size=100)
    assert _gate_verdict(map_path) is True
    pa_tbl = _read_sorted(map_path, False, restore_ctx)
    rs_tbl = _read_sorted(map_path, True, restore_ctx)
    assert pa_tbl.equals(rs_tbl)


def test_dictionary_column_native_parity(tmp_path, restore_ctx):
    """A *naturally* dictionary-typed column (the file embeds an arrow dictionary
    type) decodes natively and byte-identically to PyArrow, even with per-row-group
    dictionaries and nulls — Parquet dictionaries are per-row-group, the classic
    index-divergence trap, so this spans several row groups on purpose.

    Distinct from the ``dictionary_columns`` *forced* read (columns coerced to
    dictionary output at read time), which the planned path handles via an
    alignment cast (see ``test_forced_dictionary_columns_native_parity``)."""
    n = 500
    path = tmp_path / "dict.parquet"
    vals = [None if i % 37 == 0 else f"cat{(i * 7) % 13}" for i in range(n)]
    table = pa.table(
        {
            "id": pa.array(np.arange(n, dtype=np.int64)),
            "d": pa.array(vals, type=pa.dictionary(pa.int32(), pa.string())),
        }
    )
    pq.write_table(table, str(path), write_page_index=True, row_group_size=100)
    assert _gate_verdict(path) is True
    pa_tbl = _read_sorted(path, False, restore_ctx)
    rs_tbl = _read_sorted(path, True, restore_ctx)
    assert pa_tbl.equals(rs_tbl)


def test_extension_types_native_parity(tmp_path, restore_ctx):
    """Extension types decode NATIVELY and byte-identically to PyArrow: Ray's
    tensor extension, its variable-shaped (ragged) tensor, and pyarrow's canonical
    ``fixed_shape_tensor``.

    The crate carries the embedded arrow-schema field metadata
    (``ARROW:extension:name`` / ``:metadata``) straight through the C data
    interface, so pyarrow reconstructs the *registered* extension identically on
    the native and PyArrow paths. (This is the case a blanket ``extension_name``
    rejection used to fall back — empirically it round-trips, so the gate now
    recurses into the storage type instead.)"""
    from ray.data.extensions import ArrowTensorArray, ArrowVariableShapedTensorArray

    n = 400

    # Ray fixed-shape tensor, spanning several row groups.
    tpath = tmp_path / "tensor.parquet"
    tens = ArrowTensorArray.from_numpy(
        np.arange(4 * n, dtype=np.float32).reshape(n, 2, 2)
    )
    pq.write_table(
        pa.table({"id": pa.array(np.arange(n, dtype=np.int64)), "t": tens}),
        str(tpath),
        write_page_index=True,
        row_group_size=100,
    )
    assert _gate_verdict(tpath) is True
    assert _read_arrow_sorted(tpath, False, restore_ctx).equals(
        _read_arrow_sorted(tpath, True, restore_ctx)
    )

    # Ray variable-shaped (ragged) tensor — storage is a struct(data, shape).
    vpath = tmp_path / "vtensor.parquet"
    ragged = np.array(
        [np.arange(i % 5 + 1, dtype=np.int64) for i in range(n)], dtype=object
    )
    vst = ArrowVariableShapedTensorArray.from_numpy(ragged)
    pq.write_table(
        pa.table({"id": pa.array(np.arange(n, dtype=np.int64)), "t": vst}),
        str(vpath),
        write_page_index=True,
        row_group_size=100,
    )
    assert _gate_verdict(vpath) is True
    assert _read_arrow_sorted(vpath, False, restore_ctx).equals(
        _read_arrow_sorted(vpath, True, restore_ctx)
    )

    # pyarrow canonical fixed_shape_tensor (not isinstance(ExtensionType) on some
    # versions — caught via extension_name).
    if hasattr(pa, "fixed_shape_tensor"):
        cpath = tmp_path / "canonical.parquet"
        flat = pa.array(np.arange(n * 4, dtype=np.float32), type=pa.float32())
        storage = pa.FixedSizeListArray.from_arrays(flat, 4)
        tarr = pa.ExtensionArray.from_storage(
            pa.fixed_shape_tensor(pa.float32(), [4]), storage
        )
        pq.write_table(
            pa.table({"id": pa.array(np.arange(n, dtype=np.int64)), "t": tarr}),
            str(cpath),
            write_page_index=True,
            row_group_size=100,
        )
        assert _gate_verdict(cpath) is True
        assert _read_arrow_sorted(cpath, False, restore_ctx).equals(
            _read_arrow_sorted(cpath, True, restore_ctx)
        )

    # There is no per-type support gate to unit-check anymore: every
    # Parquet-encodable type is admitted (proven by the parity reads above), and
    # Arrow's in-memory-only types (union, list_view, run_end_encoded, ...) have
    # no Parquet encoding — PyArrow refuses to write them ("Unhandled type for
    # Arrow to Parquet schema conversion") — so they can never appear in a
    # footer-derived schema. The old ``_arrow_rs_type_supported`` gate on them
    # was unreachable dead code, removed 2026-07-28.
    union_array = pa.UnionArray.from_dense(
        pa.array([0], pa.int8()),
        pa.array([0], pa.int32()),
        [pa.array([1], pa.int64())],
    )
    with pytest.raises(pa.lib.ArrowNotImplementedError, match="Unhandled type"):
        pq.write_table(pa.table({"u": union_array}), str(tmp_path / "union.parquet"))


def test_arrow_rs_supported_gate(tmp_path):
    """Unit-check the fallback gate: local flat AND struct/list = supported;
    empty projection / unknown-not-on-disk column (no unified schema to
    null-fill from) / non-local filesystem = unsupported."""
    import pyarrow.dataset as pds
    from pyarrow.fs import LocalFileSystem

    from ray.data._internal.datasource_v2.readers.arrow_rs_parquet_file_reader import (
        ArrowRsParquetFileReader,
    )

    flat = tmp_path / "flat.parquet"
    pq.write_table(_flat_table(1000), str(flat), write_page_index=True)
    nested = tmp_path / "nested.parquet"
    pq.write_table(
        pa.table({"id": pa.array([1, 2]), "v": pa.array([[1], [2]])}), str(nested)
    )

    reader = ArrowRsParquetFileReader(filesystem=LocalFileSystem())

    flat_frag = next(
        pds.dataset(
            str(flat), format="parquet", filesystem=LocalFileSystem()
        ).get_fragments()
    )
    nested_frag = next(
        pds.dataset(
            str(nested), format="parquet", filesystem=LocalFileSystem()
        ).get_fragments()
    )

    assert reader._arrow_rs_supported(flat_frag, None) is True
    assert reader._arrow_rs_supported(flat_frag, ["id", "x"]) is True
    # Empty projection → fall back.
    assert reader._arrow_rs_supported(flat_frag, []) is False
    # List column → native (ungated 2026-07-21).
    assert reader._arrow_rs_supported(nested_frag, None) is True
    # A requested column that isn't on disk (here a dotted name — in real reads
    # ``_split_columns`` filters these out before the gate) is treated as a
    # missing column; with no unified schema to null-fill from → fall back.
    assert reader._arrow_rs_supported(nested_frag, ["v.item"]) is False

    # Unknown filesystem (neither local nor S3) → fall back.
    reader_no_fs = ArrowRsParquetFileReader(filesystem=None)
    assert reader_no_fs._arrow_rs_supported(flat_frag, None) is False


# ---------------------------------------------------------------------------
# read_metadata FFI (Track 1): arrow-rs owns the footer read. The crate returns
# the Arrow schema (via the C-schema PyCapsule) plus per-row-group counts, so the
# Python reader no longer has to build a PyArrow dataset to learn the layout.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("row_group_size", [20_000, 5_000])
def test_read_metadata_matches_pyarrow(tmp_path, row_group_size):
    import ray_data_arrow_rs

    table = _flat_table(20_000)
    path = tmp_path / "meta.parquet"
    pq.write_table(
        table, str(path), row_group_size=row_group_size, write_page_index=True
    )

    pf = pq.ParquetFile(str(path))
    md = ray_data_arrow_rs.read_metadata(str(path))

    # Schema round-trips exactly through __arrow_c_schema__ (types + names).
    assert pa.schema(md).equals(pf.schema_arrow)
    assert md.num_rows == pf.metadata.num_rows
    assert md.num_row_groups == pf.metadata.num_row_groups
    assert md.row_group_num_rows == [
        pf.metadata.row_group(i).num_rows for i in range(pf.metadata.num_row_groups)
    ]
    assert len(md.row_group_byte_sizes) == pf.metadata.num_row_groups
    assert all(b > 0 for b in md.row_group_byte_sizes)


# ---------------------------------------------------------------------------
# Native predicate pushdown, part 1: statistics-based row-group pruning.
#
# These cover the seam the Rust unit tests (which use synthetic ColStats) can't:
# lowering a Ray Expr to the IR (`_predicate_to_ir`) and reading *real* Parquet
# statistics into that pruning (`select_row_groups` / `read_row_groups`'s
# `predicate_json`). Pruning is row-group granular — a surviving group is decoded
# whole; row-level filtering is the reader's post-decode job, exercised
# separately by the end-to-end filter parity tests above.
# ---------------------------------------------------------------------------


def _sorted_rg_table(n=4000):
    """A sorted int ``id`` (+ float ``x``, string ``g``) so that with
    ``row_group_size=1000`` each of the 4 row groups holds a disjoint id range,
    making stats pruning observable."""
    return pa.table(
        {
            "id": pa.array(np.arange(n, dtype=np.int64)),
            "x": pa.array(np.arange(n) * 0.5),
            "g": pa.array([f"k{i // 1000}" for i in range(n)]),  # k0..k3 per group
        }
    )


def _ir(expr):
    from ray.data._internal.datasource_v2.readers.arrow_rs_parquet_file_reader import (
        _predicate_to_ir,
    )

    return _predicate_to_ir(expr)


def test_predicate_to_ir_lowering():
    """The Ray Expr -> IR lowering produces the expected shapes, flips the op
    when the column is on the right, and degrades unrepresentable nodes to
    ``unknown`` (keeping the rest of a conjunction prunable)."""
    from ray.data.expressions import col, lit

    assert _ir(col("id") >= 3000) == {
        "t": "cmp",
        "col": "id",
        "op": "ge",
        "value": {"vt": "int", "v": 3000},
    }
    # literal-on-left flips ge -> le
    assert _ir(lit(3000) <= col("id")) == {
        "t": "cmp",
        "col": "id",
        "op": "ge",
        "value": {"vt": "int", "v": 3000},
    }
    # conjunction; float + string literals tagged by type
    assert _ir((col("x") < 1.5) & (col("g") == "k2")) == {
        "t": "and",
        "preds": [
            {"t": "cmp", "col": "x", "op": "lt", "value": {"vt": "float", "v": 1.5}},
            {"t": "cmp", "col": "g", "op": "eq", "value": {"vt": "str", "v": "k2"}},
        ],
    }
    assert _ir(col("id").is_null()) == {"t": "is_null", "col": "id"}
    assert _ir(col("id").is_in([1, 2, 3])) == {
        "t": "in",
        "col": "id",
        "values": [
            {"vt": "int", "v": 1},
            {"vt": "int", "v": 2},
            {"vt": "int", "v": 3},
        ],
        "negated": False,
    }
    # a UDF conjunct is unrepresentable -> unknown, but the other conjunct stays.
    part = _ir((col("id") >= 3000) & (col("x").abs() > 1.0))
    assert part["t"] == "and"
    assert part["preds"][0] == {
        "t": "cmp",
        "col": "id",
        "op": "ge",
        "value": {"vt": "int", "v": 3000},
    }
    assert part["preds"][1] == {"t": "unknown"}


def test_predicate_json_skips_when_nothing_prunable():
    from ray.data._internal.datasource_v2.readers.arrow_rs_parquet_file_reader import (
        _predicate_json,
    )
    from ray.data.expressions import col

    assert _predicate_json(None) is None
    # A bare UDF predicate lowers entirely to unknown -> no pushdown arg.
    assert _predicate_json(col("x").abs() > 1.0) is None
    assert _predicate_json(col("id") >= 3000) is not None


def test_select_row_groups_pruning_on_real_stats(tmp_path):
    """``select_row_groups`` prunes using the file's actual footer statistics,
    across int / float / string columns, and never over-prunes."""
    import json

    path = tmp_path / "sorted.parquet"
    table = _sorted_rg_table(4000)
    pq.write_table(table, str(path), write_page_index=True, row_group_size=1000)

    def sel(expr):
        return ray_data_arrow_rs.select_row_groups(str(path), json.dumps(_ir(expr)))

    from ray.data.expressions import col

    # No predicate -> all four groups.
    assert ray_data_arrow_rs.select_row_groups(str(path), None) == [0, 1, 2, 3]
    # id in [0,999],[1000,1999],[2000,2999],[3000,3999] per group.
    assert sel(col("id") >= 3000) == [3]
    assert sel(col("id") >= 3500) == [3]  # group-granular: whole group 3 survives
    assert sel(col("id") < 1000) == [0]
    assert sel(col("id") >= 10**9) == []  # nothing can match
    assert sel((col("id") >= 1500) & (col("id") < 2500)) == [1, 2]
    # float column (x = id*0.5, so group 3 is [1500, 1999.5])
    assert sel(col("x") >= 1800.0) == [3]
    # string column pruning (g = "k0".."k3")
    assert sel(col("g") == "k2") == [2]
    assert sel(col("g") == "zzz") == []
    # is_in over the string groups
    assert sel(col("g").is_in(["k0", "k3"])) == [0, 3]


def test_read_row_groups_predicate_json_decodes_only_surviving_groups(tmp_path):
    """End to end through the crate: ``predicate_json`` prunes row groups before
    decode, so the stream contains exactly the surviving groups' rows (whole
    groups — row-level filtering is applied by the reader, not here) and stays
    byte-correct and in order."""
    import json

    path = tmp_path / "sorted.parquet"
    table = _sorted_rg_table(4000)
    pq.write_table(table, str(path), write_page_index=True, row_group_size=1000)

    from ray.data.expressions import col

    # id >= 3000 keeps only group 3 (rows 3000..3999).
    got = _read_crate_stream(path, predicate_json=json.dumps(_ir(col("id") >= 3000)))
    assert got.equals(table.slice(3000, 1000))

    # id >= 3500 keeps group 3 whole (pruning is row-group granular).
    got = _read_crate_stream(path, predicate_json=json.dumps(_ir(col("id") >= 3500)))
    assert got.equals(table.slice(3000, 1000))

    # A fully-pruning predicate yields an empty (schema-correct) stream.
    got = _read_crate_stream(path, predicate_json=json.dumps(_ir(col("id") >= 10**9)))
    assert got.num_rows == 0
    assert got.column_names == ["id", "x", "g"]


# ---------------------------------------------------------------------------
# S3 (moto server). The crate reads S3 through the Rust `object_store` client,
# so it needs a real HTTP endpoint — Ray Data's `s3_server` fixture (a moto
# server) provides one. These prove the native path (a) connects with the same
# endpoint/credentials/region PyArrow used (recovered from the S3FileSystem via
# `_s3_config`), and (b) returns byte-identical data.
# ---------------------------------------------------------------------------


def _s3_write(table, s3_fs, s3_path, name="data.parquet"):
    """Write ``table`` as one file into the moto S3 dir, return the s3:// URI."""
    base = _unwrap_protocol(s3_path)  # strip s3:// → bucket/key
    key = os.path.join(base, name)
    pq.write_table(table, key, filesystem=s3_fs, write_page_index=True)
    return f"s3://{key}"


def _read_s3_sorted(uri, s3_fs, use_arrow_rs, restore_ctx, **read_kwargs):
    restore_ctx.use_arrow_rs_parquet_reader = use_arrow_rs
    ds = ray.data.read_parquet(uri, filesystem=s3_fs, **read_kwargs)
    return pa.Table.from_pandas(ds.to_pandas()).sort_by("id")


def test_s3_config_recovers_endpoint_and_creds(s3_fs):
    """`_s3_config` must recover the moto endpoint, static creds, region, and
    (critically) allow_http from an http:// endpoint whose `scheme` field still
    reads 'https' — otherwise the crate can't reach moto/MinIO."""
    from ray.data._internal.datasource_v2.readers.arrow_rs_parquet_file_reader import (
        _s3_config,
    )

    cfg = _s3_config(s3_fs)
    assert cfg["endpoint"] and cfg["endpoint"].startswith("http://")
    assert cfg["allow_http"] is True
    assert cfg["region"] == "us-west-2"
    assert cfg["access_key_id"] == "testing"
    assert cfg["secret_access_key"] == "testing"
    assert cfg["anonymous"] is False


def test_read_metadata_s3_matches_pyarrow(s3_fs, s3_path):
    """`read_metadata_s3` fetches the footer over the moto endpoint (same config
    recovery as the data path) and returns the same schema + row-group counts as
    PyArrow reading the same object."""
    import ray_data_arrow_rs

    from ray.data._internal.datasource_v2.readers.arrow_rs_parquet_file_reader import (
        _s3_config,
    )

    table = _flat_table(20_000)
    uri = _s3_write(table, s3_fs, s3_path, name="meta.parquet")
    bucket, _, key = _unwrap_protocol(uri).partition("/")

    pf = pq.ParquetFile(_unwrap_protocol(uri), filesystem=s3_fs)

    cfg = _s3_config(s3_fs)
    md = ray_data_arrow_rs.read_metadata_s3(
        bucket,
        key,
        cfg["region"],
        cfg["anonymous"],
        endpoint=cfg["endpoint"],
        access_key_id=cfg["access_key_id"],
        secret_access_key=cfg["secret_access_key"],
        session_token=cfg["session_token"],
        allow_http=cfg["allow_http"],
        virtual_hosted_style=cfg["virtual_hosted_style"],
    )

    assert pa.schema(md).equals(pf.schema_arrow)
    assert md.num_rows == pf.metadata.num_rows
    assert md.num_row_groups == pf.metadata.num_row_groups
    assert md.row_group_num_rows == [
        pf.metadata.row_group(i).num_rows for i in range(pf.metadata.num_row_groups)
    ]


def test_arrow_rs_s3_parity(s3_fs, s3_path, restore_ctx):
    """Full-scan parity reading from (moto) S3 via the native path vs PyArrow."""
    table = _flat_table()
    uri = _s3_write(table, s3_fs, s3_path)

    pa_tbl = _read_s3_sorted(uri, s3_fs, False, restore_ctx)
    rs_tbl = _read_s3_sorted(uri, s3_fs, True, restore_ctx)

    assert rs_tbl.num_rows == table.num_rows
    assert pa_tbl.equals(rs_tbl)


def test_arrow_rs_s3_parity_with_projection(s3_fs, s3_path, restore_ctx):
    table = _flat_table()
    uri = _s3_write(table, s3_fs, s3_path)

    pa_tbl = _read_s3_sorted(uri, s3_fs, False, restore_ctx, columns=["id", "x"])
    rs_tbl = _read_s3_sorted(uri, s3_fs, True, restore_ctx, columns=["id", "x"])
    assert rs_tbl.column_names == ["id", "x"]
    assert pa_tbl.equals(rs_tbl)


def test_arrow_rs_s3_native_path_runs(s3_fs, s3_path):
    """Confirm an S3 fragment actually goes through the native crate's S3 entry
    point (``read_row_groups_s3``), not a silent PyArrow fallback. Exercised via
    the reader directly (in-process) so the monkeypatch can observe the call —
    a ``ray.data.read_parquet`` read would run in a worker the driver can't patch.
    """
    import pyarrow.dataset as pds

    from ray.data._internal.datasource_v2.readers.arrow_rs_parquet_file_reader import (
        ArrowRsParquetFileReader,
    )

    table = _flat_table()
    _s3_write(table, s3_fs, s3_path)
    base = _unwrap_protocol(s3_path)

    calls = {"n": 0}
    orig = ray_data_arrow_rs.read_row_groups_s3

    def wrapped(*a, **k):
        calls["n"] += 1
        return orig(*a, **k)

    ray_data_arrow_rs.read_row_groups_s3 = wrapped
    try:
        reader = ArrowRsParquetFileReader(
            filesystem=s3_fs, target_block_size=128 * 1024 * 1024
        )
        dataset = pds.dataset(base, format="parquet", filesystem=s3_fs)
        fragment = next(dataset.get_fragments())
        scanner_kwargs = {
            "columns": None,
            "filter": None,
            "batch_size": reader._resolve_batch_size(dataset),
        }
        got = pa.concat_tables(
            list(reader._iter_fragment_tables(fragment, scanner_kwargs))
        )
    finally:
        ray_data_arrow_rs.read_row_groups_s3 = orig

    assert calls["n"] > 0, "native read_row_groups_s3 was not called (fell back)"
    assert got.sort_by("id").equals(table.sort_by("id"))


def test_arrow_rs_s3_sum(s3_fs, s3_path, restore_ctx):
    """The `ds.sum()` aggregation workload (§3.3) over S3 must match PyArrow and
    ground truth — decode-heavy / output-light through the native S3 path."""
    table = _flat_table()
    uri = _s3_write(table, s3_fs, s3_path)
    expected = pc.sum(table["id"]).as_py()

    restore_ctx.use_arrow_rs_parquet_reader = False
    pa_sum = ray.data.read_parquet(uri, filesystem=s3_fs).sum("id")
    restore_ctx.use_arrow_rs_parquet_reader = True
    rs_sum = ray.data.read_parquet(uri, filesystem=s3_fs).sum("id")

    assert rs_sum == pa_sum == expected


@pytest.mark.parametrize("k", [2, 4, 8])
def test_arrow_rs_s3_kspilt_windowed_order(s3_fs, s3_path, k):
    """The windowed K-split S3 path (lone big row group → K concurrent GET streams,
    plus a small fetch window slicing each range into sub-windows) must return rows
    in EXACT file order — not just the right set. Forces the path by calling the
    native entry point directly with a tiny split threshold and window; a monotone
    `id` column then makes any range/window mis-ordering a hard failure.
    """
    from ray.data._internal.datasource_v2.readers.arrow_rs_parquet_file_reader import (
        _s3_config,
    )

    n = 50_000
    table = pa.table({"id": pa.array(np.arange(n, dtype=np.int64))})
    _s3_write(table, s3_fs, s3_path, name="mono.parquet")

    base = _unwrap_protocol(s3_path)
    bucket, _, key = os.path.join(base, "mono.parquet").partition("/")
    cfg = _s3_config(s3_fs)

    stream = ray_data_arrow_rs.read_row_groups_s3(
        bucket,
        key,
        cfg["region"],
        cfg["anonymous"],
        endpoint=cfg["endpoint"],
        access_key_id=cfg["access_key_id"],
        secret_access_key=cfg["secret_access_key"],
        session_token=cfg["session_token"],
        allow_http=cfg["allow_http"],
        virtual_hosted_style=cfg["virtual_hosted_style"],
        row_groups=[0],
        columns=["id"],
        batch_size=4096,
        fetch_window_mb=1,  # force sub-window slicing within each range
        k=k,
        split_threshold_bytes=1,  # force the K-split path on a small file
    )
    got = pa.RecordBatchReader.from_stream(stream).read_all()
    assert got.num_rows == n
    # Exact order: id must be 0,1,2,...,n-1 with no reordering across K ranges.
    assert got["id"].to_pylist() == list(range(n))


@pytest.mark.parametrize("prefetch_windows", [1, 2, 4])
def test_arrow_rs_s3_window_prefetch_order(s3_fs, s3_path, prefetch_windows):
    """The window-prefetch pipeline (K=1 single stream, many fetch windows) must
    return rows in EXACT file order at every prefetch depth. This is the common S3
    path the knob targets: one row group sliced into many small windows, with up to
    ``prefetch_windows`` of them fetching+decoding concurrently. depth=1 is the old
    strictly-serial behavior; depth>1 overlaps window N+1's fetch with window N's
    decode. A monotone ``id`` makes any window mis-ordering a hard failure, so this
    guards that the concurrent look-ahead never lets a later window's rows overtake
    an earlier one's.
    """
    from ray.data._internal.datasource_v2.readers.arrow_rs_parquet_file_reader import (
        _s3_config,
    )

    n = 50_000
    table = pa.table({"id": pa.array(np.arange(n, dtype=np.int64))})
    _s3_write(table, s3_fs, s3_path, name="mono_pf.parquet")

    base = _unwrap_protocol(s3_path)
    bucket, _, key = os.path.join(base, "mono_pf.parquet").partition("/")
    cfg = _s3_config(s3_fs)

    stream = ray_data_arrow_rs.read_row_groups_s3(
        bucket,
        key,
        cfg["region"],
        cfg["anonymous"],
        endpoint=cfg["endpoint"],
        access_key_id=cfg["access_key_id"],
        secret_access_key=cfg["secret_access_key"],
        session_token=cfg["session_token"],
        allow_http=cfg["allow_http"],
        virtual_hosted_style=cfg["virtual_hosted_style"],
        row_groups=[0],
        columns=["id"],
        batch_size=4096,
        fetch_window_mb=1,  # force many sub-windows within the single stream
        k=1,  # single stream: isolate the prefetch pipeline from K-split
        prefetch_windows=prefetch_windows,
    )
    got = pa.RecordBatchReader.from_stream(stream).read_all()
    assert got.num_rows == n
    assert got["id"].to_pylist() == list(range(n))


def test_arrow_rs_s3_struct_parity(s3_fs, s3_path, restore_ctx):
    """A struct column over S3 decodes natively (the gate admits struct now,
    on S3 exactly as it does locally) and stays byte-identical to PyArrow."""
    table = pa.table(
        {
            "id": pa.array(np.arange(2000, dtype=np.int64)),
            "st": pa.StructArray.from_arrays(
                [pa.array(np.arange(2000)), pa.array(np.arange(2000) * 0.5)],
                names=["a", "b"],
            ),
        }
    )
    uri = _s3_write(table, s3_fs, s3_path, name="struct.parquet")

    restore_ctx.use_arrow_rs_parquet_reader = False
    pa_tbl = pa.Table.from_pandas(
        ray.data.read_parquet(uri, filesystem=s3_fs).to_pandas()
    ).sort_by("id")
    restore_ctx.use_arrow_rs_parquet_reader = True
    rs_tbl = pa.Table.from_pandas(
        ray.data.read_parquet(uri, filesystem=s3_fs).to_pandas()
    ).sort_by("id")
    assert pa_tbl.equals(rs_tbl)


@pytest.mark.parametrize(
    "target_chunk_size",
    [0, 6_000, 11_000, 10 * 1024**3],
    ids=["one_rg_per_chunk", "bundle_pairs_a", "bundle_pairs_b", "whole_file"],
)
def test_chunker_native_metadata_matches_pyarrow(
    tmp_path, restore_ctx, target_chunk_size
):
    """Step 3: with the arrow-rs flag on, ``ParquetFileChunker`` reads the footer
    through the native crate instead of PyArrow. The crate's
    ``row_group_compressed_sizes`` equals PyArrow's ``sum(col.total_compressed_size)``,
    so the greedy row-group bundling produces byte-for-byte identical chunk ranges
    *and* chunk sizes whether the flag is on or off."""
    from pyarrow.fs import LocalFileSystem

    from ray.data._internal.datasource_v2.chunkers.file_chunker import (
        ParquetFileChunker,
    )

    path = tmp_path / "data.parquet"
    table = _flat_table(num_rows=4_000)
    pq.write_table(table, str(path), write_page_index=True, row_group_size=1_000)
    size = path.stat().st_size
    fs = LocalFileSystem()

    def chunks_for(use_arrow_rs):
        restore_ctx.use_arrow_rs_parquet_reader = use_arrow_rs
        chunker = ParquetFileChunker(target_chunk_size=target_chunk_size)
        return [
            (m["row_group_start"], m["row_group_end"], sz)
            for m, sz in chunker.generate_chunk_metadatas(str(path), size, fs)
        ]

    assert chunks_for(True) == chunks_for(False)


def test_chunker_native_metadata_makes_no_pyarrow_footer_read(tmp_path, restore_ctx):
    """With the flag on, the chunker must not touch PyArrow's ``read_metadata`` —
    the whole supported-file footer read goes through the arrow-rs crate."""
    import pyarrow.parquet as pqmod
    from pyarrow.fs import LocalFileSystem

    from ray.data._internal.datasource_v2.chunkers.file_chunker import (
        ParquetFileChunker,
    )

    path = tmp_path / "data.parquet"
    pq.write_table(
        _flat_table(num_rows=4_000),
        str(path),
        write_page_index=True,
        row_group_size=1_000,
    )
    size = path.stat().st_size
    fs = LocalFileSystem()

    calls = {"n": 0}
    orig = pqmod.read_metadata

    def _spy(*args, **kwargs):
        calls["n"] += 1
        return orig(*args, **kwargs)

    pqmod.read_metadata = _spy
    try:
        restore_ctx.use_arrow_rs_parquet_reader = True
        list(ParquetFileChunker().generate_chunk_metadatas(str(path), size, fs))
        assert calls["n"] == 0, "flag ON must not call pyarrow.parquet.read_metadata"

        restore_ctx.use_arrow_rs_parquet_reader = False
        list(ParquetFileChunker().generate_chunk_metadatas(str(path), size, fs))
        assert calls["n"] == 1, "flag OFF must use pyarrow.parquet.read_metadata"
    finally:
        pqmod.read_metadata = orig


def _write_int96(path, embed_arrow_schema):
    """Write a Parquet file storing the timestamp column as the legacy INT96
    physical type. ``embed_arrow_schema=False`` mimics Spark/Hive/Impala (no
    embedded Arrow schema); ``True`` mimics a PyArrow writer, which pins the
    source ``timestamp[us]`` unit in the file's key-value metadata."""
    import datetime

    ts = pa.array(
        [
            datetime.datetime(2021, 6, 1) + datetime.timedelta(minutes=i)
            for i in range(2000)
        ],
        type=pa.timestamp("us"),
    )
    table = pa.table({"id": pa.array(np.arange(2000, dtype=np.int64)), "t": ts})
    pq.write_table(
        table,
        str(path),
        use_deprecated_int96_timestamps=True,
        store_schema=embed_arrow_schema,
        write_page_index=True,
    )
    return table


def _read_manifest_with_path_verdict(path, monkeypatch):
    """Drive ``ArrowRsParquetFileReader.read`` in-process over a whole-file
    manifest and report ``(table, took_native_decode)``.

    In-process (not via ``ray.data.read_parquet``) so the crate/pyarrow spies
    actually observe the decode — Ray would run it in a worker where a
    driver-side monkeypatch is invisible. ``took_native_decode`` is True iff the
    crate's ``read_row_groups`` ran (the file took the native path); a fallback
    instead opens the file via ``pyarrow.dataset.dataset``."""
    from pyarrow.fs import LocalFileSystem

    from ray.data._internal.datasource_v2.readers.arrow_rs_parquet_file_reader import (
        ArrowRsParquetFileReader,
    )

    calls = {"read_row_groups": 0}
    orig = ray_data_arrow_rs.read_row_groups

    def spy(*a, **k):
        calls["read_row_groups"] += 1
        return orig(*a, **k)

    monkeypatch.setattr(ray_data_arrow_rs, "read_row_groups", spy)

    reader = ArrowRsParquetFileReader(
        filesystem=LocalFileSystem(), target_block_size=128 * 1024 * 1024
    )
    manifest = _make_manifest([str(path)], [os.path.getsize(path)], [None])
    table = pa.concat_tables(list(reader.read(manifest))).sort_by("id")
    return table, calls["read_row_groups"] > 0


def test_int96_no_arrow_hint_reads_native_as_ns(tmp_path, restore_ctx, monkeypatch):
    """A Spark/Hive/Impala-style INT96 file (no embedded Arrow schema) decodes to
    ``timestamp[ns]`` on both paths, so the crate handles it *natively* and stays
    byte-identical to PyArrow — bringing the common INT96 case onto the
    memory-efficient native path."""
    path = tmp_path / "spark_int96.parquet"
    _write_int96(path, embed_arrow_schema=False)

    # The crate's footer read reports the column as INT96 and decodes it to ns.
    md = ray_data_arrow_rs.read_metadata(str(path))
    assert "t" in list(md.int96_columns)
    assert pa.schema(md).field("t").type == pa.timestamp("ns")

    rs_tbl, took_native = _read_manifest_with_path_verdict(path, monkeypatch)
    pa_tbl = pa.Table.from_pandas(ray.data.read_parquet(str(path)).to_pandas()).sort_by(
        "id"
    )

    assert took_native, "INT96/ns file should take the native decode path"
    assert rs_tbl.schema.field("t").type == pa.timestamp("ns")
    assert pa_tbl.equals(rs_tbl)


def test_int96_with_non_ns_hint_realigns_natively(tmp_path, restore_ctx, monkeypatch):
    """A PyArrow-written INT96 file embeds a ``timestamp[us]`` hint that the crate
    honors (→ us) but PyArrow ignores (always ns). The planned native path stays
    native and *realigns*: the plan-time ``_ColumnAlignment`` casts the decoded
    column to ``ns`` so the result is byte-identical to PyArrow — no fallback."""
    path = tmp_path / "pyarrow_int96.parquet"
    _write_int96(path, embed_arrow_schema=True)

    # Crate decodes to us (honoring the embedded hint) — the divergence the
    # alignment cast repairs.
    assert pa.schema(ray_data_arrow_rs.read_metadata(str(path))).field(
        "t"
    ).type == pa.timestamp("us")

    rs_tbl, took_native = _read_manifest_with_path_verdict(path, monkeypatch)
    pa_tbl = pa.Table.from_pandas(ray.data.read_parquet(str(path)).to_pandas()).sort_by(
        "id"
    )

    assert took_native, "INT96/non-ns-hint file should decode natively + realign"
    assert rs_tbl.schema.field("t").type == pa.timestamp("ns")
    assert pa_tbl.equals(rs_tbl)


def test_int96_gate_rejects_non_ns_in_columns_supported(tmp_path):
    """Unit-level: ``_columns_supported`` (the per-fragment re-gate, which
    requires a *no-op* alignment) stays True for an INT96 column already at
    ns/no-tz and False for a non-ns unit — the latter now means "needs an
    alignment cast", which the planned ``read()`` handles natively while the
    pyarrow-fragment path conservatively falls back."""
    from pyarrow.fs import LocalFileSystem

    from ray.data._internal.datasource_v2.readers.arrow_rs_parquet_file_reader import (
        ArrowRsParquetFileReader,
    )

    reader = ArrowRsParquetFileReader(filesystem=LocalFileSystem())
    ns_schema = pa.schema([("id", pa.int64()), ("t", pa.timestamp("ns"))])
    us_schema = pa.schema([("id", pa.int64()), ("t", pa.timestamp("us"))])

    # 't' flagged as INT96: ns is admitted, us (non-ns) is rejected.
    assert reader._columns_supported(ns_schema, ["id", "t"], ["t"]) is True
    assert reader._columns_supported(us_schema, ["id", "t"], ["t"]) is False
    # A genuine (non-INT96) timestamp[us] column stays supported — only INT96
    # columns are unit-gated.
    assert reader._columns_supported(us_schema, ["id", "t"], []) is True
    # Projecting away the INT96 column sidesteps the gate.
    assert reader._columns_supported(us_schema, ["id"], ["t"]) is True


# ---------------------------------------------------------------------------
# Column alignment: gates closed by the per-file post-decode fixup plan
# (schema-evolution null-fill, unified-schema cast, INT96 coercion, forced
# dictionary_columns). Each test compares the native reader against the base
# PyArrow reader IN-PROCESS over the same manifest, with a crate spy proving
# the native decode actually ran (no silent fallback).
# ---------------------------------------------------------------------------


def _read_both_in_process(paths, monkeypatch, **reader_kwargs):
    """Read ``paths`` with the arrow-rs reader and the base PyArrow reader over
    an identical whole-file manifest; return ``(rs_table, pa_table,
    native_decodes)`` where ``native_decodes`` counts crate decode calls."""
    from pyarrow.fs import LocalFileSystem

    from ray.data._internal.datasource_v2.readers.arrow_rs_parquet_file_reader import (
        ArrowRsParquetFileReader,
    )
    from ray.data._internal.datasource_v2.readers.parquet_file_reader import (
        ParquetFileReader,
    )

    calls = {"decode": 0}
    orig = ray_data_arrow_rs.read_row_groups

    def spy(*a, **k):
        calls["decode"] += 1
        return orig(*a, **k)

    monkeypatch.setattr(ray_data_arrow_rs, "read_row_groups", spy)

    paths = [str(p) for p in paths]
    manifest = _make_manifest(
        paths, [os.path.getsize(p) for p in paths], [None] * len(paths)
    )
    rs = pa.concat_tables(
        list(
            ArrowRsParquetFileReader(
                filesystem=LocalFileSystem(), **reader_kwargs
            ).read(manifest)
        )
    )
    pa_tbl = pa.concat_tables(
        list(
            ParquetFileReader(filesystem=LocalFileSystem(), **reader_kwargs).read(
                manifest
            )
        )
    )
    return rs, pa_tbl, calls["decode"]


def _evolved_fixture(tmp_path):
    """Two-file dataset with schema evolution: file A has (id, x, s[large]);
    file B lacks ``x`` and stores ``s`` as plain ``string`` (type drift). The
    unified schema is A's."""
    a = tmp_path / "evo_a.parquet"
    b = tmp_path / "evo_b.parquet"
    pq.write_table(
        pa.table(
            {
                "id": pa.array([1, 2], pa.int64()),
                "x": pa.array([1.5, 2.5], pa.float64()),
                "s": pa.array(["a", "b"], pa.large_string()),
            }
        ),
        str(a),
        write_page_index=True,
    )
    pq.write_table(
        pa.table(
            {
                "id": pa.array([3, 4], pa.int64()),
                "s": pa.array(["c", "d"], pa.string()),
            }
        ),
        str(b),
        write_page_index=True,
    )
    unified = pa.schema(
        [("id", pa.int64()), ("x", pa.float64()), ("s", pa.large_string())]
    )
    return a, b, unified


def test_schema_evolution_null_fill_native_parity(tmp_path, monkeypatch):
    """A file missing a unified-schema column (schema evolution) plus per-file
    type drift decodes NATIVELY: the alignment null-fills the missing column
    with the unified type and casts the drifted one, byte-matching PyArrow's
    pinned-schema scan. Both files must take the crate path."""
    a, b, unified = _evolved_fixture(tmp_path)

    rs, pa_tbl, native_decodes = _read_both_in_process(
        [a, b], monkeypatch, schema=unified
    )
    assert native_decodes >= 2, "both files should decode natively"
    assert rs.sort_by("id").equals(pa_tbl.sort_by("id"))
    assert rs.schema == pa_tbl.schema

    # Same with an explicit projection that includes the evolved column.
    rs, pa_tbl, native_decodes = _read_both_in_process(
        [a, b], monkeypatch, schema=unified, columns=["x", "id"]
    )
    assert native_decodes >= 2
    assert rs.sort_by("id").equals(pa_tbl.sort_by("id"))


def test_filter_on_evolved_column_native_parity(tmp_path, monkeypatch):
    """A pushed predicate over the null-filled (evolved) column evaluates on
    the ALIGNED batch, so rows from the file lacking the column drop exactly
    like PyArrow's null-comparison semantics."""
    from ray.data.expressions import col

    a, b, unified = _evolved_fixture(tmp_path)
    rs, pa_tbl, native_decodes = _read_both_in_process(
        [a, b], monkeypatch, schema=unified, predicate=(col("x") > 2.0)
    )
    assert native_decodes >= 1
    assert rs.sort_by("id").equals(pa_tbl.sort_by("id"))
    assert rs.num_rows == 1  # only id=2 (x=2.5) survives; file B is all-null x


def test_coerce_int96_timestamp_unit_falls_back(tmp_path, monkeypatch):
    """A file decoding INT96 under ``coerce_int96_timestamp_unit`` FALLS BACK
    (with or without an embedded arrow-schema hint): pyarrow's decode-time
    coercion floors (parquet types.h divides the unsigned nanos-of-day) while
    a post-decode cast truncates toward zero — one unit apart on every
    pre-1970 value, so no cast reproduces the kwarg. Parity still holds
    because the fallback IS pyarrow for that file."""
    for embed in (False, True):
        path = tmp_path / f"i96_{embed}.parquet"
        _write_int96(path, embed_arrow_schema=embed)
        rs, pa_tbl, native_decodes = _read_both_in_process(
            [path],
            monkeypatch,
            parquet_format_kwargs={"coerce_int96_timestamp_unit": "ms"},
        )
        assert native_decodes == 0, f"embed={embed} must fall back under the kwarg"
        assert rs.schema.field("t").type == pa.timestamp("ms")
        assert rs.sort_by("id").equals(pa_tbl.sort_by("id"))


def test_forced_dictionary_columns_native_parity(tmp_path, monkeypatch):
    """A forced ``dictionary_columns`` read no longer falls back: the crate
    decodes the plain column and the alignment dictionary-casts it to exactly
    PyArrow's forced-dict output (``dictionary<values=string, indices=int32>``)."""
    path = tmp_path / "forced_dict.parquet"
    pq.write_table(
        pa.table(
            {
                "id": pa.array([1, 2, 3], pa.int64()),
                "s": pa.array(["x", "y", "x"]),
            }
        ),
        str(path),
        write_page_index=True,
    )
    rs, pa_tbl, native_decodes = _read_both_in_process(
        [path], monkeypatch, parquet_format_kwargs={"dictionary_columns": ["s"]}
    )
    assert native_decodes >= 1
    assert rs.schema.field("s").type == pa.dictionary(pa.int32(), pa.string())
    assert rs.sort_by("id").equals(pa_tbl.sort_by("id"))


def test_dotted_nested_projection_native_parity(tmp_path, monkeypatch):
    """Dotted (nested-field) projection like ``user.name`` stays NATIVE and
    matches the base reader exactly — which today means the dotted column is
    silently DROPPED by both paths.

    V2 discards dotted names *before* any reader sees them:
    ``FileReader._split_columns`` classifies ``user.name`` as not-on-disk (the
    footer schema has only the root ``user``), routing it to the synthesize
    bucket, where nothing synthesizes it and ``_postprocess`` drops it. The raw
    PyArrow scanner *could* resolve it (yielding a leaf column named ``name``),
    but Ray never passes it through. So true nested projection is a platform
    feature V2 lacks on the PyArrow path too — implementing a Rust
    ``ProjectionMask`` for it would be new functionality, not migration parity.
    This test pins the parity: no fallback, identical (dropped-column) output.
    """
    path = tmp_path / "nested_proj.parquet"
    pq.write_table(
        pa.table(
            {
                "id": pa.array([1, 2], pa.int64()),
                "user": pa.array(
                    [{"name": "a", "age": 30}, {"name": "b", "age": 40}],
                    type=pa.struct([("name", pa.string()), ("age", pa.int64())]),
                ),
            }
        ),
        str(path),
        write_page_index=True,
    )

    # Dotted projection: native decode, and both readers drop the dotted name.
    rs, pa_tbl, native_decodes = _read_both_in_process(
        [path], monkeypatch, columns=["user.name", "id"]
    )
    assert native_decodes >= 1, "dotted projection must not force a fallback"
    assert rs.column_names == ["id"]
    assert rs.equals(pa_tbl)

    # Whole-struct projection (the supported way to read nested data): native
    # decode with full byte parity.
    rs, pa_tbl, native_decodes = _read_both_in_process(
        [path], monkeypatch, columns=["user", "id"]
    )
    assert native_decodes >= 1
    assert rs.column_names == ["user", "id"]
    assert rs.equals(pa_tbl)


def test_flat_column_named_with_dot_native_parity(tmp_path, monkeypatch):
    """A FLAT top-level column literally named ``"user.name"`` (legal in
    Parquet) decodes natively with parity — even when the same file also has a
    struct ``user`` with a ``name`` child. Exact flat-name match wins in both
    the pyarrow scanner and the crate, so the gate must not fall back on a
    dot in a column name (dots only ever mean nested *projection* upstream of
    the reader, where V2 discards them)."""
    path = tmp_path / "flatdot.parquet"
    pq.write_table(
        pa.table(
            {
                "id": pa.array([1, 2], pa.int64()),
                "user.name": pa.array(["flat1", "flat2"]),
                "user": pa.array(
                    [{"name": "nested1"}, {"name": "nested2"}],
                    type=pa.struct([("name", pa.string())]),
                ),
            }
        ),
        str(path),
        write_page_index=True,
    )
    rs, pa_tbl, native_decodes = _read_both_in_process(
        [path], monkeypatch, columns=["user.name", "id"]
    )
    assert native_decodes >= 1, "flat dotted-named column must decode natively"
    assert rs.column_names == ["user.name", "id"]
    assert rs.column("user.name").to_pylist() == ["flat1", "flat2"]
    assert rs.equals(pa_tbl)


def test_perf_only_format_kwargs_stay_native(tmp_path, monkeypatch):
    """I/O-tuning format kwargs (``pre_buffer`` / ``buffer_size`` /
    ``use_buffered_stream``) cannot change decoded bytes, so the native path
    ignores them and stays native; the PyArrow reader honors them and must
    produce identical output."""
    path = tmp_path / "perf_kwargs.parquet"
    pq.write_table(
        pa.table(
            {
                "id": pa.array([1, 2, 3], pa.int64()),
                "s": pa.array(["a", "b", "c"]),
            }
        ),
        str(path),
        write_page_index=True,
    )
    rs, pa_tbl, native_decodes = _read_both_in_process(
        [path],
        monkeypatch,
        parquet_format_kwargs={
            "pre_buffer": False,
            "buffer_size": 64 * 1024,
            "use_buffered_stream": True,
        },
    )
    assert native_decodes >= 1, "perf-only format kwargs must not force fallback"
    assert rs.sort_by("id").equals(pa_tbl.sort_by("id"))


def test_unsupported_format_kwarg_falls_back(tmp_path, monkeypatch):
    """A format kwarg outside the native allowlist forces a PyArrow fallback
    (which honors it) instead of being silently ignored. Exercised with
    ``arrow_extensions_enabled`` — a pyarrow 21+ schema-shaping toggle the
    native path doesn't reproduce (see TODO.md "arrow_extensions_enabled")."""
    path = tmp_path / "audit_kwargs.parquet"
    pq.write_table(
        pa.table({"id": pa.array([1, 2], pa.int64())}), str(path), write_page_index=True
    )

    rs, pa_tbl, native_decodes = _read_both_in_process(
        [path],
        monkeypatch,
        parquet_format_kwargs={"arrow_extensions_enabled": True},
    )
    assert native_decodes == 0, "unsupported format kwarg must force fallback"
    assert rs.equals(pa_tbl)


def test_strict_mode_raises_on_fallback_and_passes_native(tmp_path, monkeypatch):
    """``RAY_DATA_ARROW_RS_STRICT`` turns every decision to serve a read via
    the PyArrow fallback into a hard error — so a large-scale validation run
    can *guarantee* it exercised the native path — while leaving natively
    supported reads untouched."""
    from pyarrow.fs import LocalFileSystem

    from ray.data._internal.datasource_v2.readers.arrow_rs_parquet_file_reader import (
        ArrowRsParquetFileReader,
    )

    path = tmp_path / "strict.parquet"
    pq.write_table(
        pa.table({"id": pa.array([1, 2], pa.int64())}), str(path), write_page_index=True
    )
    monkeypatch.setenv("RAY_DATA_ARROW_RS_STRICT", "1")
    manifest = _make_manifest([str(path)], [os.path.getsize(path)], [None])

    # Native-supported read: strict mode is a no-op.
    table = pa.concat_tables(
        list(ArrowRsParquetFileReader(filesystem=LocalFileSystem()).read(manifest))
    )
    assert table["id"].to_pylist() == [1, 2]

    # Fallback-forcing read (format kwarg outside the allowlist): raises
    # instead of silently serving PyArrow-decoded bytes.
    reader = ArrowRsParquetFileReader(
        filesystem=LocalFileSystem(),
        parquet_format_kwargs={"arrow_extensions_enabled": True},
    )
    with pytest.raises(RuntimeError, match="RAY_DATA_ARROW_RS_STRICT"):
        list(reader.read(manifest))

    # Per-file gate (no plannable alignment): a file whose read requires an
    # unsupported read-time coercion also raises under strict mode.
    reader = ArrowRsParquetFileReader(
        filesystem=LocalFileSystem(),
        parquet_format_kwargs={"dictionary_columns": ["id"]},
        schema=pa.schema([("id", pa.dictionary(pa.int32(), pa.int64()))]),
    )
    try:
        got = list(reader.read(manifest))
    except RuntimeError as e:
        assert "RAY_DATA_ARROW_RS_STRICT" in str(e)
    else:
        # If the alignment can plan this coercion it stays native — equally
        # acceptable; the point is "never a silent fallback under strict".
        assert got


def test_thrift_limits_native_parity(tmp_path, monkeypatch):
    """The thrift footer limits stay NATIVE via the metadata-only pyarrow
    footer probe: a generous limit decodes natively with identical output,
    and a tiny limit raises the same ``OSError`` the base path raises
    (parity-of-error) — from both readers, not just the fallback."""
    from pyarrow.fs import LocalFileSystem

    from ray.data._internal.datasource_v2.readers.parquet_file_reader import (
        ParquetFileReader,
    )

    path = tmp_path / "thrift_limits.parquet"
    pq.write_table(
        pa.table({"id": pa.array([1, 2], pa.int64())}), str(path), write_page_index=True
    )

    # Generous limit: the probe passes and the decode is native.
    rs, pa_tbl, native_decodes = _read_both_in_process(
        [path],
        monkeypatch,
        parquet_format_kwargs={"thrift_string_size_limit": 1 << 20},
    )
    assert native_decodes >= 1, "thrift limits must not force fallback anymore"
    assert rs.equals(pa_tbl)

    # Tiny limit: both readers must reject the footer with the same error.
    tiny = {"thrift_string_size_limit": 10}
    manifest = _make_manifest([str(path)], [os.path.getsize(path)], [None])
    from ray.data._internal.datasource_v2.readers.arrow_rs_parquet_file_reader import (
        ArrowRsParquetFileReader,
    )

    with pytest.raises(OSError):
        list(
            ArrowRsParquetFileReader(
                filesystem=LocalFileSystem(), parquet_format_kwargs=dict(tiny)
            ).read(manifest)
        )
    with pytest.raises(OSError):
        list(
            ParquetFileReader(
                filesystem=LocalFileSystem(), parquet_format_kwargs=dict(tiny)
            ).read(manifest)
        )


def _write_crc_file(path, corrupt=False):
    """Write an uncompressed file with page checksums; optionally flip one
    byte inside a data page (detectable only via CRC verification)."""
    table = pa.table(
        {
            "id": pa.array(range(100), pa.int64()),
            "s": pa.array([f"crc_sentinel_{i:04d}" for i in range(100)]),
        }
    )
    pq.write_table(
        table,
        str(path),
        write_page_index=True,
        compression="NONE",
        write_page_checksum=True,
    )
    if corrupt:
        data = bytearray(path.read_bytes())
        idx = data.find(b"crc_sentinel_0050")
        assert idx > 0
        data[idx] = ord("X")
        path.write_bytes(bytes(data))
    return table


def test_page_checksum_verification_true_native(tmp_path, monkeypatch):
    """``page_checksum_verification=True`` decodes natively: the crate is
    built with parquet's ``crc`` feature and always verifies stored page
    CRCs, so True *is* the native behavior — clean files match byte-for-byte,
    and a corrupt page raises from BOTH readers (parity-of-error)."""
    from pyarrow.fs import LocalFileSystem

    from ray.data._internal.datasource_v2.readers.arrow_rs_parquet_file_reader import (
        ArrowRsParquetFileReader,
    )
    from ray.data._internal.datasource_v2.readers.parquet_file_reader import (
        ParquetFileReader,
    )

    clean = tmp_path / "crc_clean.parquet"
    _write_crc_file(clean)
    rs, pa_tbl, native_decodes = _read_both_in_process(
        [clean],
        monkeypatch,
        parquet_format_kwargs={"page_checksum_verification": True},
    )
    assert native_decodes >= 1, "page_checksum_verification=True must stay native"
    assert rs.equals(pa_tbl)

    corrupt = tmp_path / "crc_corrupt.parquet"
    _write_crc_file(corrupt, corrupt=True)
    manifest = _make_manifest([str(corrupt)], [os.path.getsize(corrupt)], [None])
    kwargs = {"page_checksum_verification": True}
    for reader_cls in (ArrowRsParquetFileReader, ParquetFileReader):
        with pytest.raises((OSError, pa.lib.ArrowInvalid), match="CRC"):
            list(
                reader_cls(
                    filesystem=LocalFileSystem(), parquet_format_kwargs=dict(kwargs)
                ).read(manifest)
            )


def test_page_checksum_verification_false_falls_back(tmp_path, monkeypatch):
    """An explicit ``page_checksum_verification=False`` is the opt-out for
    reading a file *despite* corrupt checksums. The crate build always
    verifies (compile-time ``crc`` feature, no off-switch), so only PyArrow
    can honor the opt-out — the read must fall back and succeed, returning
    the same (corrupted) bytes from both paths."""
    path = tmp_path / "crc_optout.parquet"
    _write_crc_file(path, corrupt=True)
    rs, pa_tbl, native_decodes = _read_both_in_process(
        [path],
        monkeypatch,
        parquet_format_kwargs={"page_checksum_verification": False},
    )
    assert native_decodes == 0, "explicit False must force the PyArrow fallback"
    assert rs.equals(pa_tbl)
    # The corruption is really there — the flipped byte comes through.
    assert rs["s"][50].as_py() == "Xrc_sentinel_0050"


def _schema_shaped_fixture(tmp_path):
    """A file WITHOUT an embedded arrow schema (``store_schema=False``), so
    ``binary_type`` / ``list_type`` genuinely change pyarrow's decoded types
    (with an embedded schema they are inert)."""
    path = tmp_path / "schema_shaped.parquet"
    pq.write_table(
        pa.table(
            {
                "id": pa.array([1, 2], pa.int64()),
                "b": pa.array([b"x", b"y"], pa.binary()),
                "l": pa.array([[1, 2], [3]], pa.list_(pa.int64())),
                "s": pa.array(["a", "bb"], pa.string()),
            }
        ),
        str(path),
        write_page_index=True,
        store_schema=False,
    )
    return path


def test_schema_shaped_kwargs_native_with_pinned_schema(tmp_path, monkeypatch):
    """``binary_type`` / ``list_type`` with a pinned dataset schema decode
    natively: the pin is the output-type authority — on the base path the
    pinned-schema cast silently *undoes* these kwargs (the V2 listing infers
    the schema via ``pq.read_schema``, which is blind to them), and the
    native path's alignment produces the pinned types identically."""
    path = _schema_shaped_fixture(tmp_path)
    unified = pq.read_schema(str(path))
    rs, pa_tbl, native_decodes = _read_both_in_process(
        [path],
        monkeypatch,
        schema=unified,
        parquet_format_kwargs={
            "binary_type": pa.large_binary(),
            "list_type": pa.LargeListType,
        },
    )
    assert native_decodes >= 1, "pinned-schema read must stay native"
    assert rs.equals(pa_tbl)
    # The pin wins: output types are the plain (non-large) footer types.
    assert rs.schema.field("b").type == pa.binary()
    assert rs.schema.field("s").type == pa.string()


def test_schema_shaped_kwargs_fall_back_without_schema(tmp_path, monkeypatch):
    """Without a pinned schema, ``binary_type`` / ``list_type`` genuinely
    change the decoded types (large_binary / large_string / large_list on a
    no-embedded-schema file) — the crate doesn't reproduce that, so the read
    falls back to PyArrow and both paths agree on the large types."""
    path = _schema_shaped_fixture(tmp_path)
    rs, pa_tbl, native_decodes = _read_both_in_process(
        [path],
        monkeypatch,
        parquet_format_kwargs={
            "binary_type": pa.large_binary(),
            "list_type": pa.LargeListType,
        },
    )
    assert native_decodes == 0, "schema-shaping kwargs without a pin must fall back"
    assert rs.equals(pa_tbl)
    assert rs.schema.field("b").type == pa.large_binary()
    assert rs.schema.field("s").type == pa.large_string()
    assert rs.schema.field("l").type == pa.large_list(pa.int64())


def test_arrow_rs_tuning_kwargs_reach_crate(tmp_path, monkeypatch):
    """``arrow_rs_*`` tuning knobs in ``dataset_kwargs`` must (a) reach the
    crate's decode call with the requested values, (b) keep the read native,
    and (c) be ignored by the base PyArrow reader (popped before
    ``pds.ParquetFileFormat`` sees them) — the mirror image of the native
    reader ignoring ``pre_buffer``. The base reader reading the same kwargs
    without a TypeError is assertion (c)."""
    path = tmp_path / "tuning_kwargs.parquet"
    table = _flat_table(10_000)
    pq.write_table(table, str(path), write_page_index=True, row_group_size=10_000)

    # Capture the crate call args underneath the helper's own decode spy.
    # Local signature (positional): (path, row_groups, columns, batch_size,
    # decode_budget_bytes, k, split_threshold_bytes, predicate_json).
    captured = []
    orig = ray_data_arrow_rs.read_row_groups

    def capture(*a, **k):
        captured.append(a)
        return orig(*a, **k)

    monkeypatch.setattr(ray_data_arrow_rs, "read_row_groups", capture)

    rs, pa_tbl, native_decodes = _read_both_in_process(
        [path],
        monkeypatch,
        parquet_format_kwargs={
            "arrow_rs_decode_budget_bytes": 4 * 1024 * 1024,
            "arrow_rs_k": 2,
            "arrow_rs_split_threshold_bytes": 0,  # force the K-split path
        },
    )
    assert native_decodes >= 1, "tuning kwargs must not force fallback"
    assert rs.sort_by("id").equals(pa_tbl.sort_by("id"))
    assert captured, "crate decode call was not captured"
    for args in captured:
        assert args[4] == 4 * 1024 * 1024  # decode_budget_bytes
        assert args[5] == 2  # k
        assert args[6] == 0  # split_threshold_bytes


def test_arrow_rs_tuning_kwarg_typo_raises(tmp_path):
    """A misspelled ``arrow_rs_*`` key must fail loudly at reader construction
    (both readers share the check in ``ParquetFileReader.__init__``), not
    surface as a baffling pyarrow TypeError or a silent native fallback."""
    from pyarrow.fs import LocalFileSystem

    from ray.data._internal.datasource_v2.readers.arrow_rs_parquet_file_reader import (
        ArrowRsParquetFileReader,
    )
    from ray.data._internal.datasource_v2.readers.parquet_file_reader import (
        ParquetFileReader,
    )

    for reader_cls in (ArrowRsParquetFileReader, ParquetFileReader):
        with pytest.raises(ValueError, match="arrow_rs_decode_budget"):
            reader_cls(
                filesystem=LocalFileSystem(),
                parquet_format_kwargs={"arrow_rs_decode_budget": 1},  # typo'd key
            )


@pytest.mark.parametrize("bad_value", [0, "four", True])
def test_arrow_rs_tuning_kwarg_invalid_value_raises(tmp_path, monkeypatch, bad_value):
    """An invalid tuning value (wrong type, below minimum, or a bool sneaking
    in as int) must raise a ValueError naming the knob — a mis-set perf knob
    silently clamped or ignored would corrupt benchmarks."""
    path = tmp_path / "bad_tuning.parquet"
    pq.write_table(
        pa.table({"id": pa.array([1, 2], pa.int64())}), str(path), write_page_index=True
    )
    with pytest.raises(ValueError, match="arrow_rs_k"):
        _read_both_in_process(
            [path],
            monkeypatch,
            parquet_format_kwargs={"arrow_rs_k": bad_value},
        )


def test_arrow_rs_tuning_kwargs_end_to_end(tmp_path, restore_ctx):
    """The knobs survive the full ``read_parquet(dataset_kwargs=...)`` plumbing
    (read_api -> datasource -> scanner -> reader) under BOTH flag settings:
    natively they tune the crate; on the PyArrow reader they're inert — the
    same call is valid either way."""
    path = tmp_path / "tuning_e2e.parquet"
    table = _flat_table(5_000)
    pq.write_table(table, str(path), write_page_index=True)

    kwargs = {"dataset_kwargs": {"arrow_rs_decode_budget_bytes": 4 * 1024 * 1024}}
    pa_tbl = _read_sorted(path, False, restore_ctx, **kwargs)
    rs_tbl = _read_sorted(path, True, restore_ctx, **kwargs)
    assert pa_tbl.equals(rs_tbl)
    assert rs_tbl.num_rows == table.num_rows


def test_unified_only_column_not_dropped_natively(tmp_path, monkeypatch):
    """A unified-schema column absent from EVERY file in the split must still
    surface (all-null), matching the base reader under a pinned schema — the
    column split takes names from the unified schema, not the footers."""
    path = tmp_path / "no_extra.parquet"
    pq.write_table(
        pa.table({"id": pa.array([1, 2], pa.int64())}), str(path), write_page_index=True
    )
    unified = pa.schema([("id", pa.int64()), ("later_col", pa.string())])
    rs, pa_tbl, native_decodes = _read_both_in_process(
        [path], monkeypatch, schema=unified
    )
    assert native_decodes >= 1
    assert "later_col" in rs.column_names
    assert rs.sort_by("id").equals(pa_tbl.sort_by("id"))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
