"""Tests for the Parquet decode-peak estimate, against PyArrow's measured peak."""

import numpy as np
import pytest
from packaging.version import parse as parse_version

from ray.data._internal.datasource_v2.formats.parquet.parquet_decode_peak import (
    estimate_row_group_peak,
    estimate_row_group_work,
    read_data_pages,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_decoded_size import (
    Read,
    build_leaf_profiles,
    dictionary_page_read_size,
    leaf_chunks,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_size_statistics import (
    read_dictionary_page,
    read_size_statistics,
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


def _estimate(path, columns=None, **format_kwargs):
    """The peak estimate for ``path``'s one row group, read as the footer reader does."""
    metadata = pq.read_metadata(str(path))
    profiles = build_leaf_profiles(
        parquet_schema=metadata.schema,
        format_kwargs=format_kwargs,
        file_metadata=metadata,
    )
    assert profiles is not None
    row_group = metadata.row_group(0)
    stats = read_size_statistics(metadata)
    rg_stats = None if stats is None else stats[0]
    pages = {}
    with open(path, "rb") as f:
        for leaf_idx in range(row_group.num_columns):
            chunk = row_group.column(leaf_idx)
            leaf_stats = None if rg_stats is None else rg_stats[leaf_idx]
            nbytes = dictionary_page_read_size(
                profile=profiles[leaf_idx], chunk=chunk, stats=leaf_stats
            )
            if nbytes:
                f.seek(chunk.dictionary_page_offset)
                page = read_dictionary_page(f.read(nbytes))
                assert page is not None
                pages[leaf_idx] = page
    leaves = {
        leaf.index: leaf
        for leaf in leaf_chunks(
            row_group=row_group,
            leaf_profiles=profiles,
            leaf_indices=range(row_group.num_columns),
            size_stats=rg_stats,
            dictionary_pages=pages,
        )
    }
    schema_columns = [metadata.schema.column(j) for j in range(metadata.num_columns)]
    column_leaves = [
        [
            j
            for j, column in enumerate(schema_columns)
            if column.path.split(".")[0] == name
        ]
        for name in columns or metadata.schema.to_arrow_schema().names
    ]
    data_pages = read_data_pages(metadata)
    work = estimate_row_group_work(
        row_group=row_group,
        columns=schema_columns,
        column_leaves=column_leaves,
        read=Read(format_kwargs=format_kwargs),
        leaves=leaves,
        counted=set(),
        data_pages=None if data_pages is None else data_pages[0],
    )
    return estimate_row_group_peak(
        row_group=row_group,
        leaf_indices=sorted(j for leaf_indices in column_leaves for j in leaf_indices),
        row_group_work=work,
    )


def _strings(distinct):
    return pa.array([f"value-{i % distinct:08d}" for i in range(N)])


_CASES = {
    "fixed-width": (
        pa.table(
            {
                "a": pa.array(np.arange(N, dtype="int64")),
                "b": pa.array(np.random.default_rng(0).random(N)),
            }
        ),
        {},
        {},
        None,
        3,
    ),
    "int16-with-nulls": (
        pa.table(
            {"a": pa.array(np.arange(N) % 100, pa.int16(), mask=np.arange(N) % 7 == 0)}
        ),
        {},
        {},
        None,
        3,
    ),
    "unique-strings": (pa.table({"a": _strings(N)}), {}, {}, None, 3),
    "strings-read-as-dictionary": (
        pa.table({"a": _strings(N // 4)}),
        {},
        {"dictionary_columns": ["a"]},
        None,
        3,
    ),
    # PyArrow miscounts DELTA_BYTE_ARRAY SizeStatistics, so the decoded size is
    # a crude bound here, and the peak inherits it.
    "delta-byte-array": (
        pa.table({"a": _strings(N)}),
        {"use_dictionary": False, "column_encoding": {"a": "DELTA_BYTE_ARRAY"}},
        {},
        None,
        20,
    ),
    "list": (
        pa.table({"l": pa.array([[i, i + 1, i + 2] for i in range(N)])}),
        {},
        {},
        None,
        3,
    ),
    # The int64 column is read first, so PyArrow reserves it for the list's values.
    "column-after-list-reserve": (
        pa.table(
            {
                "l": pa.array([[i, i + 1, i + 2] for i in range(N)]),
                "y": pa.array(np.arange(N, dtype="int64")),
            }
        ),
        {},
        {},
        ["y", "l"],
        3,
    ),
}


@requires_pyarrow_23
@pytest.mark.parametrize("label", list(_CASES))
def test_estimate_bounds_measured_peak(tmp_path, label):
    table, write_kwargs, format_kwargs, columns, high = _CASES[label]
    path = tmp_path / f"{label}.parquet"
    pq.write_table(table, path, row_group_size=N, **write_kwargs)
    ratio = _estimate(path=path, columns=columns, **format_kwargs) / _peak(
        path=path, columns=columns, **format_kwargs
    )
    assert 0.95 <= ratio <= high, ratio


@pytest.mark.parametrize("use_dictionary", [False, True])
@pytest.mark.parametrize("data_page_version", ["1.0", "2.0"])
def test_read_data_pages(tmp_path, use_dictionary, data_page_version):
    # A page per 1,000-row write batch; a dictionary page is not a data page.
    path = tmp_path / "pages.parquet"
    table = pa.table(
        {"a": pa.array(np.arange(10_000) % 10), "b": _strings(10)[:10_000]}
    )
    pq.write_table(
        table,
        path,
        row_group_size=5_000,
        data_page_size=1,
        write_batch_size=1_000,
        use_dictionary=use_dictionary,
        data_page_version=data_page_version,
    )
    assert read_data_pages(pq.read_metadata(path)) == [[5, 5], [5, 5]]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
