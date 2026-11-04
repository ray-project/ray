"""Tests for the Parquet decode-peak estimate, against PyArrow's measured peak,
and for the footer reader that sizes read tasks by it."""

import os

import numpy as np
import pytest
from packaging.version import parse as parse_version

from ray.data._internal.datasource_v2.formats.parquet.parquet_decode_peak import (
    _nested_or_view,
    estimate_cast,
    estimate_row_group_peak,
    estimate_row_group_work,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_decoded_size import (
    Read,
    added_bytes,
    build_leaf_profiles,
    dictionary_page_read_size,
    estimate_added_columns,
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


def _no_footer_stats(metadata):
    """``read_footer_stats`` for a footer the walk can't read."""
    return None


def _dictionary_pages(f, row_group, profiles, rg_stats):
    """The dictionary page headers the estimate asks for, read from ``f``."""
    pages = {}
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
    return pages


def _estimate(
    path,
    columns=None,
    footer_stats=read_footer_stats,
    read_pages=True,
    pinned_schema=None,
    added=(0, 0),
    **format_kwargs,
):
    """The peak estimate of each of ``path``'s row groups, as the footer reader makes it.

    ``columns`` are the columns read, in the order PyArrow reads them.
    """
    metadata = pq.read_metadata(str(path))
    profiles = build_leaf_profiles(
        parquet_schema=metadata.schema,
        format_kwargs=format_kwargs,
        file_metadata=metadata,
    )
    assert profiles is not None
    file_schema = metadata.schema.to_arrow_schema()
    schema_columns = [metadata.schema.column(j) for j in range(metadata.num_columns)]
    column_leaves = {
        name: [
            j
            for j, column in enumerate(schema_columns)
            if column.path.split(".")[0] == name
        ]
        for name in columns or file_schema.names
    }
    leaf_indices = sorted(j for indices in column_leaves.values() for j in indices)
    cast_profiles, cast_leaves = None, {}
    if pinned_schema is not None:
        cast_leaves = {
            name: indices
            for name, indices in column_leaves.items()
            if file_schema.field(name).type != pinned_schema.field(name).type
        }
    if cast_leaves:
        cast_profiles = build_leaf_profiles(
            parquet_schema=metadata.schema,
            pinned_schema=pinned_schema,
            format_kwargs=format_kwargs,
            file_metadata=metadata,
        )
        assert cast_profiles is not None
    read = Read(
        format_kwargs=format_kwargs,
        added_row_bytes=added[0],
        added_row_group_bytes=added[1],
    )
    footer = footer_stats(metadata)
    estimates = []
    with open(path, "rb") as f:
        for rg_idx in range(metadata.num_row_groups):
            row_group = metadata.row_group(rg_idx)
            rg_stats = None if footer is None else footer.size_statistics[rg_idx]
            pages = (
                _dictionary_pages(f, row_group, profiles, rg_stats)
                if read_pages
                else {}
            )
            leaves = {
                leaf.index: leaf
                for leaf in leaf_chunks(
                    row_group=row_group,
                    leaf_profiles=profiles,
                    leaf_indices=leaf_indices,
                    size_stats=rg_stats,
                    dictionary_pages=pages,
                )
            }
            work = estimate_row_group_work(
                row_group=row_group,
                columns=schema_columns,
                column_leaves=list(column_leaves.values()),
                read=read,
                leaves=leaves,
                counted=set(),
                data_pages=None if footer is None else footer.data_pages[rg_idx],
            )
            if pinned_schema is None:
                work += estimate_added_columns(row_group=row_group, read=read)
            else:
                cast_chunks = (
                    {}
                    if cast_profiles is None
                    else {
                        name: leaf_chunks(
                            row_group=row_group,
                            leaf_profiles=cast_profiles,
                            leaf_indices=indices,
                            size_stats=rg_stats,
                            dictionary_pages=pages,
                        )
                        for name, indices in cast_leaves.items()
                    }
                )
                work += estimate_cast(
                    row_group=row_group,
                    column_leaves=cast_chunks,
                    file_schema=file_schema,
                    pinned_schema=pinned_schema,
                    pieces=1,
                    read=read,
                )
            estimates.append(
                estimate_row_group_peak(
                    row_group=row_group,
                    leaf_indices=leaf_indices,
                    row_group_work=work,
                    file_size=os.path.getsize(path),
                )
            )
    return estimates


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
    ratio = sum(_estimate(path=path, columns=columns, **format_kwargs)) / _peak(
        path=path, columns=columns, **format_kwargs
    )
    assert 0.95 <= ratio <= high, ratio


def test_a_canonical_extension_casts_as_its_storage():
    """A fixed-shape tensor column, a fixed-size list on disk, copies per piece."""
    assert _nested_or_view(pa.fixed_shape_tensor(pa.float32(), [2, 2]))


# ---------------------------------------------------------------------------
# The footer reader sizes read tasks by the estimate
# ---------------------------------------------------------------------------

# Rows per file here: enough for several row groups, small enough to write
# several files.
M = 20_000


def _mixed():
    return pa.table(
        {
            "i": pa.array(np.arange(M, dtype="int64")),
            "s": pa.array([f"s{i % 101}" for i in range(M)]),
        }
    )


def _read_and_chunk(path, **kwargs):
    from pyarrow.fs import LocalFileSystem

    from ray.data._internal.datasource_v2.formats.parquet.footer_reader import (
        FooterReader,
    )
    from ray.data._internal.datasource_v2.interfaces.file_indexer import FileInfo

    return FooterReader(LocalFileSystem(), **kwargs)._read_and_chunk(
        FileInfo(path=str(path), size=os.path.getsize(path))
    )


def _sizes_through_footer_reader(path, **kwargs):
    """Each row group's ``size_bytes`` as the footer reader lists it."""
    return [run.size_bytes for run in _read_and_chunk(path, **kwargs).row_groups]


@pytest.mark.parametrize("footer", ["walked", "unwalkable", "page-read-fails"])
def test_footer_reader_sizes_row_groups_by_the_estimate(tmp_path, monkeypatch, footer):
    """The reader puts the estimate together from the footer walk, the dictionary
    page headers and the file's size; a failed walk or page read only loosens it."""
    import ray.data._internal.datasource_v2.formats.parquet.footer_reader as reader

    path = tmp_path / "f.parquet"
    pq.write_table(_mixed(), path, row_group_size=M // 4)
    footer_stats = read_footer_stats
    if footer != "walked":
        footer_stats = _no_footer_stats
        monkeypatch.setattr(reader, "read_footer_stats", footer_stats)
    if footer == "page-read-fails":

        def unreadable(buf):
            raise OSError("unreadable")

        monkeypatch.setattr(reader, "read_dictionary_page", unreadable)

    sizes = _sizes_through_footer_reader(path)

    assert len(sizes) == 4
    assert sizes == _estimate(
        path, footer_stats=footer_stats, read_pages=footer != "page-read-fails"
    )


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

    sizes = _sizes_through_footer_reader(
        path, projected_cols=["z"], filter_expr=col("y") >= 0
    )

    assert sizes == _estimate(path, columns=["y", "z"])
    assert sizes != _estimate(path, columns=["z", "y"])


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
        uncompressed = sum(
            row_group.column(j).total_uncompressed_size
            for j in range(row_group.num_columns)
        )
        expected.append(
            estimate_row_group_peak(
                row_group=row_group,
                leaf_indices=range(row_group.num_columns),
                row_group_work=uncompressed * PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT,
                file_size=os.path.getsize(path),
            )
        )

    assert _sizes_through_footer_reader(path) == expected


def test_pinned_schema_adds_the_cast_and_the_added_columns(tmp_path):
    """A read schema the file's types differ from costs a cast; a column the
    file lacks is filled with nulls."""
    path = tmp_path / "f.parquet"
    pq.write_table(
        pa.table({"a": pa.array(np.arange(M), pa.int32())}), path, row_group_size=M // 2
    )
    plain = _estimate(path)

    widened = pa.schema([("a", pa.int64())])
    sizes = _sizes_through_footer_reader(path, read_schema=widened)
    assert sizes == _estimate(path, pinned_schema=widened)
    assert all(size > plain_size for size, plain_size in zip(sizes, plain))

    filled = pa.schema([("a", pa.int32()), ("notes", pa.string())])
    sizes = _sizes_through_footer_reader(path, read_schema=filled)
    added = added_bytes(added=[filled.field("notes")], path=str(path))
    assert sizes == _estimate(path, pinned_schema=filled, added=added)
    assert all(size > plain_size for size, plain_size in zip(sizes, plain))


def _pack_through_footer_reader(paths, max_bin_bytes):
    """Footer read, estimate, coalescing and packing, without a Ray runtime."""
    from ray.data._internal.datasource_v2.common.online_bin_packer import (
        OnlineBinPacker,
    )
    from ray.data._internal.datasource_v2.formats.parquet.footer_file_indexer import (
        _chunked_file_to_manifest,
    )

    packer = OnlineBinPacker(max_bin_bytes=max_bin_bytes)
    manifests = []

    def drain():
        while packer.has_partition():
            manifests.append(packer.next_partition())

    for path in paths:
        packer.add_input(_chunked_file_to_manifest(_read_and_chunk(path)))
        drain()
    packer.finalize()
    drain()
    return manifests


def _bin_totals(manifests):
    return [
        sum(meta["size_bytes"] for meta in manifest.file_chunk_metadatas)
        for manifest in manifests
    ]


def test_bins_stay_under_the_budget(tmp_path):
    """Row groups pack until the next would overflow the budget; one estimated
    over the budget by itself gets a bin of its own."""
    path = tmp_path / "f.parquet"
    pq.write_table(_mixed(), path, row_group_size=M // 10)
    estimates = _estimate(path)
    assert len(estimates) == 10

    budget = sum(estimates) // 4
    totals = _bin_totals(_pack_through_footer_reader([path], budget))
    assert sum(totals) == sum(estimates)
    assert all(total <= budget for total in totals)
    assert 3 <= len(totals) <= 6, totals

    totals = _bin_totals(_pack_through_footer_reader([path], min(estimates) - 1))
    assert sorted(totals) == sorted(estimates)


def test_dictionary_strings_sized_on_disk_would_share_one_bin(tmp_path):
    """On uncompressed bytes, low-cardinality strings would pack into one read task."""
    path = tmp_path / "f.parquet"
    table = pa.table({"a": pa.array([f"v{i % 5}" for i in range(M)])})
    pq.write_table(table, path, row_group_size=M // 10)
    metadata = pq.read_metadata(str(path))
    uncompressed = sum(
        metadata.row_group(rg).column(0).total_uncompressed_size
        for rg in range(metadata.num_row_groups)
    )
    budget = sum(_estimate(path)) // 4

    assert uncompressed < budget
    assert len(_pack_through_footer_reader([path], budget)) > 1


def test_files_share_bins(tmp_path):
    paths = [tmp_path / f"part{i}.parquet" for i in range(4)]
    for path in paths:
        pq.write_table(_mixed(), path, row_group_size=M // 4)
    per_file = sum(_estimate(paths[0]))

    manifests = _pack_through_footer_reader(paths, per_file * 2)
    totals = _bin_totals(manifests)

    assert sum(totals) == per_file * 4
    assert all(total <= per_file * 2 for total in totals)
    # Every row group lands in exactly one bin.
    covered = [
        (str(path), rg_id)
        for manifest in manifests
        for path, meta in zip(manifest.paths, manifest.file_chunk_metadatas)
        for rg_id in meta["unit_ids"]
    ]
    assert len(covered) == len(set(covered)) == 4 * 4


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
