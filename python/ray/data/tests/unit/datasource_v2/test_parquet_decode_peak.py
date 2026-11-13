"""Tests for the Parquet decode-peak estimate, against PyArrow's measured peak,
through the footer reader that sizes read tasks by it."""

import json
import os
import subprocess
import sys

import numpy as np
import pytest
from packaging.version import parse as parse_version

pa = pytest.importorskip("pyarrow")
pds = pytest.importorskip("pyarrow.dataset")
pq = pytest.importorskip("pyarrow.parquet")

# Modelled on PyArrow 24's reader; 17 writes no SizeStatistics (18 to 22 unchecked).
requires_pyarrow_23 = pytest.mark.skipif(
    parse_version(pa.__version__) < parse_version("23.0.0"),
    reason="Modelled on PyArrow 24",
)

N = 200_000

# One scan as a Ray read task runs it: ``threads`` CPU threads, 16 IO threads, V2's
# scanner options, one batch. The default pool's peak counts the read buffers a
# scanner's own pool misses; it can't be reset, hence a fresh process.
_SCAN = """
import json, sys
import pyarrow as pa
import pyarrow.dataset as pds
from pyarrow.fs import LocalFileSystem

path, columns, format_kwargs, threads = json.loads(sys.argv[1])
pa.set_cpu_count(threads)
pa.set_io_thread_count(16)
pool = pa.default_memory_pool()
base = pool.bytes_allocated()
fragment = pds.ParquetFileFormat(**format_kwargs).make_fragment(
    path, filesystem=LocalFileSystem()
)
scanner = fragment.scanner(
    columns=columns,
    batch_size=1 << 30,
    batch_readahead=8,
    fragment_readahead=1,
    fragment_scan_options=pds.ParquetFragmentScanOptions(
        use_buffered_stream=True, buffer_size=8 << 20
    ),
)
scanned = list(scanner.to_batches())
print(sum(piece.num_rows for piece in scanned), pool.max_memory() - base)
"""


def _peak(path, columns=None, threads=1, **format_kwargs):
    """PyArrow's peak bytes scanning ``path`` as a Ray read task."""
    task = json.dumps([str(path), columns, format_kwargs, threads])
    result = subprocess.run(
        [sys.executable, "-c", _SCAN, task], capture_output=True, text=True, check=True
    )
    rows, peak = map(int, result.stdout.split())
    assert rows == N
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


def _peaks(path, **kwargs):
    """Each row group's ``memory_bytes``, the estimated decode peak, as the footer
    reader lists it."""
    peaks = []
    for run in _read_and_chunk(path, **kwargs).row_groups:
        assert run.memory_bytes is not None
        peaks.append(run.memory_bytes)
    return peaks


def _sizes(path, **kwargs):
    """Each row group's ``size_bytes``, the estimated decoded size, as the footer
    reader lists it."""
    return [run.size_bytes for run in _read_and_chunk(path, **kwargs).row_groups]


def _strings(distinct):
    return pa.array([f"value-{i % distinct:08d}" for i in range(N)])


def _lists():
    return pa.array([[i, i + 1, i + 2] for i in range(N)])


def _case(
    table, write_kwargs=None, format_kwargs=None, columns=None, high=1.5, threads=1
):
    """A ``_CASES`` entry; ``high`` caps the estimate over the measured peak."""
    return table, write_kwargs or {}, format_kwargs or {}, columns, high, threads


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
    "strings": _case(pa.table({"a": _strings(N // 4)})),
    # The int64 column is read first, so PyArrow reserves it for the list's values.
    "column-after-list-reserve": _case(
        pa.table({"l": _lists(), "y": pa.array(np.arange(N, dtype="int64"))}),
        columns=["y", "l"],
    ),
    # One CPU thread frees each column's buffers before the next column decodes.
    "similar-lists": _case(pa.table({f"l{i}": _lists() for i in range(8)})),
    # Twelve decode all eight columns at once.
    "similar-lists-on-12-threads": _case(
        pa.table({f"l{i}": _lists() for i in range(8)}), threads=12
    ),
}


@requires_pyarrow_23
@pytest.mark.parametrize("label", list(_CASES))
def test_estimate_bounds_measured_peak(tmp_path, monkeypatch, label):
    import ray.data._internal.datasource_v2.formats.parquet.footer_reader as reader

    table, write_kwargs, format_kwargs, columns, high, threads = _CASES[label]
    path = tmp_path / f"{label}.parquet"
    pq.write_table(table, path, row_group_size=N, **write_kwargs)
    monkeypatch.setattr(reader, "READER_CPU_COUNT", threads)
    (estimate,) = _peaks(path, projected_cols=columns, format_kwargs=format_kwargs)
    ratio = estimate / _peak(
        path=path, columns=columns, threads=threads, **format_kwargs
    )
    assert 0.95 <= ratio <= high, ratio


# Rows per file here: enough for several row groups.
M = 20_000


def _mixed():
    return pa.table(
        {
            "i": pa.array(np.arange(M, dtype="int64")),
            "s": pa.array([f"s{i % 101}" for i in range(M)]),
        }
    )


@pytest.mark.parametrize(
    "unmodelled", ["file", "strings-without-size-statistics", "delta-byte-array-leaf"]
)
def test_small_unmodelled_row_groups_pay_the_floor(tmp_path, monkeypatch, unmodelled):
    """A file, or a leaf without exact counts, costs each small row group the floor, once."""
    import ray.data._internal.datasource_v2.formats.parquet.footer_reader as reader

    path = tmp_path / "f.parquet"
    write_kwargs = {}
    if unmodelled == "file":
        monkeypatch.setattr(reader, "build_leaf_profiles", lambda **kwargs: None)
    elif unmodelled == "strings-without-size-statistics":
        monkeypatch.setattr(reader, "read_footer_stats", lambda metadata: None)
    else:
        write_kwargs = {
            "use_dictionary": False,
            "column_encoding": {"s": "DELTA_BYTE_ARRAY"},
        }
    pq.write_table(_mixed(), path, row_group_size=M // 4, **write_kwargs)
    charge = reader._UNMODELLED_MIN_BYTES
    sizes = _peaks(path)
    assert len(sizes) == 4
    assert all(charge < size < charge + (1 << 20) for size in sizes), sizes


def test_large_unmodelled_leaves_pay_by_their_uncompressed_bytes(tmp_path, monkeypatch):
    """Past the floor, the charge is a multiple of the unsized leaves' uncompressed
    bytes; the sized ``int64`` column isn't in it."""
    import ray.data._internal.datasource_v2.formats.parquet.footer_reader as reader

    monkeypatch.setattr(reader, "read_footer_stats", lambda metadata: None)
    path = tmp_path / "f.parquet"
    table = pa.table(
        {
            "i": pa.array(np.arange(N, dtype="int64")),
            "s": pa.array([f"distinct-value-{i:024d}" for i in range(N)]),
        }
    )
    # The dictionary outgrows its page and falls back to PLAIN; its page still
    # leaves the strings unsized.
    pq.write_table(table, path, row_group_size=N)
    strings = pq.read_metadata(path).row_group(0).column(1).total_uncompressed_size
    charge = reader._UNMODELLED_RATIO * strings
    assert charge > reader._UNMODELLED_MIN_BYTES
    (size,) = _peaks(path)
    assert charge < size < charge + (8 << 20), (size, charge)


def test_unmodelled_file_counts_the_added_columns(tmp_path, monkeypatch):
    """A file the estimator doesn't model still pays for a column Ray fills."""
    import ray.data._internal.datasource_v2.formats.parquet.footer_reader as reader

    monkeypatch.setattr(reader, "build_leaf_profiles", lambda **kwargs: None)
    path = tmp_path / "f.parquet"
    table = _mixed()
    pq.write_table(table, path, row_group_size=M // 4)
    filled = table.schema.append(pa.field("notes", pa.string()))
    plain = _peaks(path, read_schema=table.schema)
    assert all(a > b for a, b in zip(_peaks(path, read_schema=filled), plain))


def test_filtered_read_without_the_file_schema_sizes_every_column(
    tmp_path, monkeypatch
):
    """When the file's Arrow schema can't be built, an unprojected read with a
    filter still sizes every column it decodes, not just the filter's."""
    import ray.data._internal.datasource_v2.formats.parquet.footer_reader as reader
    from ray.data.expressions import col

    class _NoSchema:
        def __init__(self, fragment):
            self._fragment = fragment

        @property
        def physical_schema(self):
            raise pa.ArrowInvalid("unsupported type")

    sizing = reader.FooterReader._file_sizing
    monkeypatch.setattr(
        reader.FooterReader,
        "_file_sizing",
        lambda self, file, fragment, metadata: sizing(
            self, file=file, fragment=_NoSchema(fragment), metadata=metadata
        ),
    )
    path = tmp_path / "f.parquet"
    table = pa.table(
        {
            "i": pa.array(np.arange(N, dtype="int64")),
            "s": pa.array([f"distinct-value-{i:024d}" for i in range(N)]),
        }
    )
    pq.write_table(table, path, row_group_size=N, use_dictionary=False)
    assert _peaks(path, filter_expr=col("i") > 0) == _peaks(path)


def test_pinned_schema_adds_the_cast_and_the_added_columns(tmp_path):
    """A read schema the file's types differ from costs a cast; a column the
    file lacks is filled with nulls."""
    path = tmp_path / "f.parquet"
    pq.write_table(
        pa.table({"a": pa.array(np.arange(M), pa.int32())}), path, row_group_size=M // 2
    )
    plain = _peaks(path)
    widened = pa.schema([("a", pa.int64())])
    filled = pa.schema([("a", pa.int32()), ("notes", pa.string())])

    for read_schema in (widened, filled):
        sizes = _peaks(path, read_schema=read_schema)
        assert len(sizes) == len(plain) == 2
        assert all(size > plain_size for size, plain_size in zip(sizes, plain))


def test_pin_leaves_out_the_columns_the_reader_appends():
    """The pin is the reader's: no partition key or synthesized column, and none at
    all, type options kept, for a schema of nothing else."""
    from pyarrow.fs import LocalFileSystem

    from ray.data._internal.datasource_v2.formats.parquet.footer_reader import (
        FooterReader,
    )
    from ray.data.datasource.partitioning import Partitioning, PartitionStyle

    x, year, path = (
        pa.field("x", pa.int64()),
        pa.field("year", pa.string()),
        pa.field("path", pa.string()),
    )
    options = {"dictionary_columns": ["x"]}

    def pin(fields, partition_values):
        reader = FooterReader(
            LocalFileSystem(),
            partitioning=Partitioning(PartitionStyle.HIVE),
            read_schema=pa.schema(fields),
            synthesized_columns=["path"],
            format_kwargs=options,
        )
        return reader._pin(partition_values)

    assert pin([x, year, path], {"year": "2024"}) == (pa.schema([x]), {})
    assert pin([x, year, path], {}) == (pa.schema([x, year]), {})
    assert pin([year, path], {"year": "2024"}) == (None, options)


def test_partition_column_is_sized_by_its_value(tmp_path):
    """A partition column's text is the path's value: a longer one sizes larger, and
    neither counts for a read without partitioning."""
    from ray.data.datasource.partitioning import Partitioning, PartitionStyle

    table = pa.table({"x": pa.array(np.arange(M, dtype="int64"))})
    schema = table.schema.append(pa.field("year", pa.string()))
    paths = []
    for value in ("2024", "20242024"):
        directory = tmp_path / f"year={value}"
        directory.mkdir()
        paths.append(directory / "data.parquet")
        pq.write_table(table, paths[-1])
    short, long = (
        _sizes(path, read_schema=schema, partitioning=Partitioning(PartitionStyle.HIVE))
        for path in paths
    )
    assert all(a < b for a, b in zip(short, long))
    assert _sizes(paths[0], read_schema=schema) == _sizes(paths[1], read_schema=schema)


def test_dotted_column_beside_its_prefix_counts_once(tmp_path):
    """A flat ``user.id`` beside ``user`` is sized once, as ``user_id`` would be."""
    sizes = []
    for second in ("user.id", "user_id"):
        path = tmp_path / f"{second}.parquet"
        values = pa.array(np.arange(M, dtype="int64"))
        pq.write_table(pa.table({"user": values, second: values}), path)
        sizes.append(_peaks(path))
    assert sizes[0] == sizes[1]


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


@requires_pyarrow_23
def test_size_is_the_decoded_size_under_the_peak(tmp_path):
    """``size_bytes`` estimates the decoded row group, which the decode peak exceeds."""
    path = tmp_path / "f.parquet"
    pq.write_table(_mixed(), path, row_group_size=M // 4)
    parquet = pq.ParquetFile(path)
    true = [parquet.read_row_group(i).nbytes for i in range(parquet.num_row_groups)]
    sizes, peaks = _sizes(path), _peaks(path)
    assert all(0.95 <= size / t <= 1.05 for size, t in zip(sizes, true)), (sizes, true)
    assert all(size < peak for size, peak in zip(sizes, peaks))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
