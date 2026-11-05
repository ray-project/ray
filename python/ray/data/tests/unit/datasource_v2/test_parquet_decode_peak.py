"""Tests for the Parquet decode-peak estimate, against PyArrow's measured peak."""

import numpy as np
import pytest
from packaging.version import parse as parse_version

from ray.data._internal.datasource_v2.formats.parquet.parquet_decode_peak import (
    _nested_or_view,
    estimate_row_group_peak,
    estimate_row_group_work,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_decoded_size import (
    Read,
    build_leaf_profiles,
    dictionary_page_read_size,
    leaf_chunks,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_size_statistics import (
    read_dictionary_page,
    read_footer_stats,
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
    footer = read_footer_stats(metadata)
    rg_stats = None if footer is None else footer.size_statistics[0]
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
    work = estimate_row_group_work(
        row_group=row_group,
        columns=schema_columns,
        column_leaves=column_leaves,
        read=Read(format_kwargs=format_kwargs),
        leaves=leaves,
        counted=set(),
        data_pages=None if footer is None else footer.data_pages[0],
    )
    return estimate_row_group_peak(
        row_group=row_group,
        leaf_indices=sorted(j for leaf_indices in column_leaves for j in leaf_indices),
        row_group_work=work,
        file_size=path.stat().st_size,
    )


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
    ratio = _estimate(path=path, columns=columns, **format_kwargs) / _peak(
        path=path, columns=columns, **format_kwargs
    )
    assert 0.95 <= ratio <= high, ratio


def test_a_canonical_extension_casts_as_its_storage():
    """A fixed-shape tensor column, a fixed-size list on disk, copies per piece."""
    assert _nested_or_view(pa.fixed_shape_tensor(pa.float32(), [2, 2]))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
