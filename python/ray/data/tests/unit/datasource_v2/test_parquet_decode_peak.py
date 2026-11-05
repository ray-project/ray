"""Tests for the Parquet decode-peak estimate, against PyArrow's measured peak,
through the footer reader that sizes read tasks by it."""

import math
import os

import numpy as np
import pytest
from packaging.version import parse as parse_version

from ray.data._internal.datasource_v2.formats.parquet.parquet_decode_peak import (
    _nested_or_view,
    estimate_row_group_peak,
)

pa = pytest.importorskip("pyarrow")
pds = pytest.importorskip("pyarrow.dataset")
pq = pytest.importorskip("pyarrow.parquet")

# Modelled on PyArrow 24's reader; 17 writes no SizeStatistics (18 to 22 unchecked).
requires_pyarrow_23 = pytest.mark.skipif(
    parse_version(pa.__version__) < parse_version("23.0.0"),
    reason="Modelled on PyArrow 24",
)

N = 200_000


def _peak(path, columns=None, **format_kwargs):
    """PyArrow's peak bytes scanning ``path`` on one thread, as a Ray read task."""
    from pyarrow.fs import LocalFileSystem

    fragment = pds.ParquetFileFormat(**format_kwargs).make_fragment(
        str(path), filesystem=LocalFileSystem()
    )
    pool = pa.proxy_memory_pool(pa.default_memory_pool())
    scanner = fragment.scanner(
        columns=columns,
        batch_size=N,
        use_threads=False,
        memory_pool=pool,
        fragment_readahead=1,
        fragment_scan_options=pds.ParquetFragmentScanOptions(
            use_buffered_stream=True, buffer_size=8 << 20
        ),
    )
    scanned = list(scanner.to_batches())
    assert sum(piece.num_rows for piece in scanned) == N
    peak = pool.max_memory()
    # Free what the pool allocated while the pool is still alive.
    del scanned, scanner
    return peak


def _read_and_chunk(path, **kwargs):
    from pyarrow.fs import LocalFileSystem

    from ray.data._internal.datasource_v2.formats.parquet.footer_reader import (
        FooterReader,
    )
    from ray.data._internal.datasource_v2.interfaces.file_indexer import FileInfo

    return FooterReader(LocalFileSystem(), **kwargs)._read_and_chunk(
        FileInfo(path=str(path), size=os.path.getsize(path))
    )


def _sizes(path, **kwargs):
    """Each row group's ``size_bytes`` as the footer reader lists it."""
    return [run.size_bytes for run in _read_and_chunk(path, **kwargs).row_groups]


def _strings(distinct):
    return pa.array([f"value-{i % distinct:08d}" for i in range(N)])


def _case(table, write_kwargs=None, format_kwargs=None, columns=None, high=3):
    """A ``_CASES`` entry; ``high`` caps the estimate over the measured peak."""
    return table, write_kwargs or {}, format_kwargs or {}, columns, high


_CASES = {
    "fixed-width": _case(
        pa.table(
            {
                "a": pa.array(np.arange(N, dtype="int64")),
                "b": pa.array(np.random.default_rng(0).random(N)),
            }
        )
    ),
    "int16-with-nulls": _case(
        pa.table(
            {"a": pa.array(np.arange(N) % 100, pa.int16(), mask=np.arange(N) % 7 == 0)}
        )
    ),
    "strings-read-as-dictionary": _case(
        pa.table({"a": _strings(N // 4)}), format_kwargs={"dictionary_columns": ["a"]}
    ),
    # PyArrow miscounts DELTA_BYTE_ARRAY SizeStatistics, so the decoded size is
    # a crude bound here, and the peak inherits it.
    "delta-byte-array": _case(
        pa.table({"a": _strings(N)}),
        write_kwargs={
            "use_dictionary": False,
            "column_encoding": {"a": "DELTA_BYTE_ARRAY"},
        },
        high=20,
    ),
    # The int64 column is read first, so PyArrow reserves it for the list's values.
    "column-after-list-reserve": _case(
        pa.table(
            {
                "l": pa.array([[i, i + 1, i + 2] for i in range(N)]),
                "y": pa.array(np.arange(N, dtype="int64")),
            }
        ),
        columns=["y", "l"],
    ),
}


@requires_pyarrow_23
@pytest.mark.parametrize("label", list(_CASES))
def test_estimate_bounds_measured_peak(tmp_path, label):
    table, write_kwargs, format_kwargs, columns, high = _CASES[label]
    path = tmp_path / f"{label}.parquet"
    pq.write_table(table, path, row_group_size=N, **write_kwargs)
    (estimate,) = _sizes(path, projected_cols=columns, format_kwargs=format_kwargs)
    ratio = estimate / _peak(path=path, columns=columns, **format_kwargs)
    assert 0.95 <= ratio <= high, ratio


def test_a_canonical_extension_casts_as_its_storage():
    """A fixed-shape tensor column, a fixed-size list on disk, copies per piece."""
    assert _nested_or_view(pa.fixed_shape_tensor(pa.float32(), [2, 2]))


# Rows per file here: enough for several row groups.
M = 20_000


def _mixed():
    return pa.table(
        {
            "i": pa.array(np.arange(M, dtype="int64")),
            "s": pa.array([f"s{i % 101}" for i in range(M)]),
        }
    )


@requires_pyarrow_23
@pytest.mark.parametrize(
    "failure, high", [("unwalkable", 3), ("page-read-fails", math.inf)]
)
def test_a_failed_footer_read_still_bounds_the_peak(
    tmp_path, monkeypatch, failure, high
):
    """Without the footer walk the strings fall back to bounds; without the
    dictionary page headers too, to each value as long as the chunk."""
    import ray.data._internal.datasource_v2.formats.parquet.footer_reader as reader

    path = tmp_path / "f.parquet"
    table = pa.table(
        {"i": pa.array(np.arange(N, dtype="int64")), "s": _strings(N // 4)}
    )
    pq.write_table(table, path, row_group_size=N)
    monkeypatch.setattr(reader, "read_footer_stats", lambda metadata: None)
    if failure == "page-read-fails":

        def unreadable(buf):
            raise OSError("unreadable")

        monkeypatch.setattr(reader, "read_dictionary_page", unreadable)

    (estimate,) = _sizes(path)
    assert 0.95 <= estimate / _peak(path=path) <= high


def test_predicate_columns_are_read_first(tmp_path):
    """PyArrow decodes the predicate's columns before the projection's, and the
    estimate reserves each column's buffers by the position it is read in."""
    from ray.data.expressions import col

    path = tmp_path / "f.parquet"
    table = pa.table(
        {
            "l": pa.array([[i, i + 1, i + 2] for i in range(M)]),
            "y": pa.array(np.arange(M, dtype="int64")),
            "z": pa.array(np.arange(M) % 7, pa.int16()),
        }
    )
    pq.write_table(table, path, row_group_size=M)

    sizes = _sizes(path, projected_cols=["z"], filter_expr=col("y") >= 0)

    assert sizes == _sizes(path, projected_cols=["y", "z"])
    assert sizes != _sizes(path, projected_cols=["z", "y"])


def test_unmodelled_schema_falls_back_to_the_encoding_ratio(tmp_path, monkeypatch):
    """Without leaf profiles, the uncompressed size times the fixed ratio stands
    in for the decode's buffers, under the same peak formula."""
    import ray.data._internal.datasource_v2.formats.parquet.footer_reader as reader
    from ray.data._internal.datasource_v2.formats.parquet.parquet_size_estimator import (
        PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT,
    )

    path = tmp_path / "f.parquet"
    pq.write_table(_mixed(), path, row_group_size=M // 4)
    monkeypatch.setattr(reader, "build_leaf_profiles", lambda **kwargs: None)
    metadata = pq.read_metadata(str(path))
    expected = []
    for rg_idx in range(metadata.num_row_groups):
        row_group = metadata.row_group(rg_idx)
        leaves = range(row_group.num_columns)
        uncompressed = sum(row_group.column(j).total_uncompressed_size for j in leaves)
        peak = estimate_row_group_peak(
            row_group=row_group,
            leaf_indices=leaves,
            row_group_work=uncompressed * PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT,
            file_size=os.path.getsize(path),
        )
        expected.append(peak)

    assert _sizes(path) == expected


def test_pinned_schema_adds_the_cast_and_the_added_columns(tmp_path):
    """A read schema the file's types differ from costs a cast; a column the
    file lacks is filled with nulls."""
    path = tmp_path / "f.parquet"
    pq.write_table(
        pa.table({"a": pa.array(np.arange(M), pa.int32())}), path, row_group_size=M // 2
    )
    plain = _sizes(path)
    widened = pa.schema([("a", pa.int64())])
    filled = pa.schema([("a", pa.int32()), ("notes", pa.string())])

    for read_schema in (widened, filled):
        sizes = _sizes(path, read_schema=read_schema)
        assert len(sizes) == len(plain) == 2
        assert all(size > plain_size for size, plain_size in zip(sizes, plain))


def _bin_totals(path, max_bin_bytes):
    """Each bin's bytes: footer read, estimate, coalescing and packing, without Ray."""
    from ray.data._internal.datasource_v2.common.online_bin_packer import (
        OnlineBinPacker,
    )
    from ray.data._internal.datasource_v2.formats.parquet.footer_file_indexer import (
        _chunked_file_to_manifest,
    )

    packer = OnlineBinPacker(max_bin_bytes=max_bin_bytes)
    packer.add_input(_chunked_file_to_manifest(_read_and_chunk(path)))
    packer.finalize()
    manifests = []
    while packer.has_partition():
        manifests.append(packer.next_partition())
    return [sum(c["size_bytes"] for c in m.file_chunk_metadatas) for m in manifests]


def test_bins_stay_under_the_budget(tmp_path):
    """Row groups pack until the next would overflow the budget; one estimated
    over the budget by itself gets a bin of its own."""
    path = tmp_path / "f.parquet"
    pq.write_table(_mixed(), path, row_group_size=M // 10)
    estimates = _sizes(path)
    assert len(estimates) == 10

    budget = sum(estimates) // 4
    totals = _bin_totals(path=path, max_bin_bytes=budget)
    assert sum(totals) == sum(estimates)
    assert all(total <= budget for total in totals)
    assert 3 <= len(totals) <= 6, totals

    totals = _bin_totals(path=path, max_bin_bytes=min(estimates) - 1)
    assert sorted(totals) == sorted(estimates)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
