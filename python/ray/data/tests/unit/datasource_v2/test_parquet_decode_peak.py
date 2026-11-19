"""Tests for the Parquet decode-peak estimate, against PyArrow's measured peak,
through the footer reader that sizes read tasks by it."""

import functools
import json
import os
import subprocess
import sys
from decimal import Decimal
from types import SimpleNamespace
from typing import cast

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyarrow.parquet import ColumnChunkMetaData

from ray.data._internal.datasource_v2.formats.parquet.decoded_size import (
    build_leaf_profiles,
    leaf_chunks,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_decode_peak import (
    _SAFETY_FACTOR,
    _dictionary_bytes,
    estimate_cast,
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

path, columns, threads = json.loads(sys.argv[1])
pa.set_cpu_count(threads)
pa.set_io_thread_count(16)
pool = pa.default_memory_pool()
base = pool.bytes_allocated()
fragment = pds.ParquetFileFormat().make_fragment(path, filesystem=LocalFileSystem())
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


def _peak(path, rows, columns=None, threads=1):
    """PyArrow's peak bytes scanning ``path`` as a Ray read task."""
    task = json.dumps([str(path), columns, threads])
    result = subprocess.run(
        [sys.executable, "-c", _SCAN, task], capture_output=True, text=True, check=True
    )
    scanned, peak = map(int, result.stdout.split())
    assert scanned == rows
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


def _ints(rows=N):
    return pa.array(np.arange(rows, dtype="int64"))


def _strings(distinct):
    drawn = np.random.default_rng(0).integers(0, distinct, N)
    return pa.array([f"value-{i:08d}" for i in drawn])


def _decimals():
    return pa.array([Decimal(i % 1000) for i in range(N)], pa.decimal128(20, 2))


@functools.cache
def _eight_lists():
    lists = pa.array([[i, i + 1, i + 2] for i in range(N)])
    return pa.table({f"l{i}": lists for i in range(8)})


def _narrow_columns():
    """400 columns whose chunks, 5.6 KiB each, fit in the gaps ``pre_buffer`` reads."""
    rng = np.random.default_rng(0)
    return pa.table({f"c{i:03d}": rng.integers(0, 2**62, 700) for i in range(400)})


def _case(make, rows=N, write_kwargs=None, columns=None, high=1.5, threads=1):
    """A ``_CASES`` entry: ``make`` builds the table of ``rows`` rows, and ``high``
    caps the estimate over the measured peak."""
    return SimpleNamespace(
        make=make,
        rows=rows,
        write_kwargs=write_kwargs or {},
        columns=columns,
        high=high,
        threads=threads,
    )


_CASES = {
    "fixed-width": _case(
        lambda: pa.table({"a": _ints(), "b": np.random.default_rng(0).random(N)})
    ),
    # Measured 1.56: the values and levels are charged at twice their slots, the most
    # doubling leaves, and these buffers were reserved to fit.
    "int16-with-nulls": _case(
        lambda: pa.table(
            {"a": pa.array(np.arange(N) % 100, pa.int16(), mask=np.arange(N) % 7 == 0)}
        ),
        high=2.0,
    ),
    # Measured 1.82: a dictionary's entries are counted as 4 bytes each, and these
    # are 18.
    "strings": _case(lambda: pa.table({"a": _strings(N // 4)}), high=2.0),
    # Measured 1.75: the builder's copy is charged at the decoded size, offsets
    # included, and its offsets again at twice their slots.
    "dictionary-strings": _case(lambda: pa.table({"a": _strings(1000)}), high=2.0),
    "uncompressed": _case(
        lambda: pa.table({"a": _ints(), "s": _strings(1000)}),
        write_kwargs=dict(compression="none"),
    ),
    # Compressed strings without a dictionary measured 1.96 to 2.05: the
    # decompression buffer is charged at the chunk's size, and holds a page.
    "delta-length-byte-array": _case(
        lambda: pa.table({"a": _strings(N // 4)}),
        write_kwargs=dict(
            use_dictionary=False, column_encoding={"a": "DELTA_LENGTH_BYTE_ARRAY"}
        ),
        high=2.2,
    ),
    # A dictionary that is most of its chunk and compresses better than the rest.
    "sorted-distinct-values": _case(
        lambda: pa.table({"a": _ints(125_000)}),
        rows=125_000,
        write_kwargs=dict(compression="zstd"),
    ),
    # A decimal128 is stored as FIXED_LEN_BYTE_ARRAY. Measured 1.63: its values are
    # charged at twice their slots.
    "fixed-len-byte-array": _case(
        lambda: pa.table({"a": _decimals()}),
        write_kwargs=dict(use_dictionary=False),
        high=2.0,
    ),
    "dictionary-fixed-len-byte-array": _case(lambda: pa.table({"a": _decimals()})),
    # Under 48 KiB, so the footer read takes the whole file.
    "small-file": _case(lambda: pa.table({"a": _ints(1000)}), rows=1000),
    # Every other column: ``pre_buffer`` reads the skipped ones between them too.
    "projection-over-small-chunks": _case(
        _narrow_columns, rows=700, columns=[f"c{i:03d}" for i in range(0, 400, 2)]
    ),
    # The int64 column is read first, so PyArrow reserves it for the list's values.
    "column-after-list-reserve": _case(
        lambda: pa.table({"l": _eight_lists()["l0"], "y": _ints()}),
        columns=["y", "l"],
    ),
    # One CPU thread frees each column's buffers before the next column decodes.
    "similar-lists": _case(_eight_lists),
    # Twelve decode all eight columns at once. On a busy machine fewer overlap, down
    # to one thread's peak, which the eight columns' estimate is 2.9 times.
    "similar-lists-on-12-threads": _case(_eight_lists, high=3.0, threads=12),
}


def _write(tmp_path, label):
    case = _CASES[label]
    path = tmp_path / f"{label}.parquet"
    pq.write_table(case.make(), path, row_group_size=case.rows, **case.write_kwargs)
    return path


@pytest.mark.parametrize("label", list(_CASES))
def test_estimate_bounds_measured_peak(tmp_path, monkeypatch, label):
    import ray.data._internal.datasource_v2.formats.parquet.footer_reader as reader

    case = _CASES[label]
    path = _write(tmp_path, label)
    monkeypatch.setattr(reader, "READER_CPU_COUNT", case.threads)
    (estimate,) = _peaks(path, projected_cols=case.columns)
    peak = _peak(path=path, rows=case.rows, columns=case.columns, threads=case.threads)
    # The estimate carries a flat safety factor over the pool's peak.
    ratio = estimate / (_SAFETY_FACTOR * peak)
    assert 1.0 <= ratio <= case.high, ratio


def test_no_thread_count_charges_every_columns_buffers(tmp_path, monkeypatch):
    import ray.data._internal.datasource_v2.formats.parquet.footer_reader as reader

    path = _write(tmp_path, "similar-lists")
    peaks = []
    for threads in (1, 8, 0):
        monkeypatch.setattr(reader, "READER_CPU_COUNT", threads)
        peaks.extend(_peaks(path))
    assert peaks[0] < peaks[1] == peaks[2]


def test_dictionary_page_without_an_offset_is_charged():
    """A writer can leave ``dictionary_page_offset`` unset on a dictionary chunk."""
    chunk = cast(
        ColumnChunkMetaData,
        SimpleNamespace(has_dictionary_page=False, total_uncompressed_size=5 << 20),
    )
    assert _dictionary_bytes(chunk=chunk, encodings={"PLAIN", "RLE"}) == 0
    assert _dictionary_bytes(chunk=chunk, encodings={"RLE_DICTIONARY"}) == 2 << 20


def test_cast_charges_the_columns_it_reallocates(tmp_path):
    table = pa.table(
        {
            "t": pa.array(np.arange(N), pa.timestamp("us")),
            "d": pa.array(np.arange(N, dtype="int32")),
            "i": pa.array(np.arange(N, dtype="int32")),
        }
    )
    pinned = pa.schema(
        [("t", pa.timestamp("us", tz="UTC")), ("d", pa.date32()), ("i", pa.int64())]
    )
    path = tmp_path / "cast.parquet"
    pq.write_table(table, path)
    metadata = pq.read_metadata(str(path))
    profiles = build_leaf_profiles(
        parquet_schema=metadata.schema,
        pinned_schema=pinned,
        format_kwargs={},
        file_metadata=metadata,
    )
    assert profiles is not None
    row_group = metadata.row_group(0)
    leaves = leaf_chunks(
        row_group=row_group,
        leaf_profiles=profiles,
        leaf_indices=range(3),
        size_stats=None,
    )
    estimate = estimate_cast(
        row_group=row_group,
        cast_leaves={name: {j: leaves[j]} for j, name in enumerate(table.schema.names)},
        file_schema=table.schema,
        pinned_schema=pinned,
    )
    before = pa.total_allocated_bytes()
    retyped = table.cast(pinned)
    allocated = pa.total_allocated_bytes() - before
    assert retyped.schema == pinned
    # Only int32 to int64 allocates: 8 bytes a value, charged 1.5 times.
    assert 8 * N <= allocated <= estimate == int(1.5 * 8 * N)


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
    "unmodelled", ["file", "strings-without-statistics", "delta-byte-array-leaf"]
)
def test_small_unmodelled_row_groups_pay_the_floor(tmp_path, monkeypatch, unmodelled):
    """A file, or a leaf without exact counts, costs each small row group the floor, once."""
    import ray.data._internal.datasource_v2.formats.parquet.footer_reader as reader

    path = tmp_path / "f.parquet"
    write_kwargs = {}
    if unmodelled == "file":
        monkeypatch.setattr(reader, "build_leaf_profiles", lambda **kwargs: None)
    elif unmodelled == "strings-without-statistics":
        # Neither SizeStatistics nor a min and max to size the dictionary's strings.
        monkeypatch.setattr(reader, "read_footer_stats", lambda metadata: None)
        write_kwargs = {"write_statistics": ["i"]}
    else:
        write_kwargs = {
            "use_dictionary": False,
            "column_encoding": {"s": "DELTA_BYTE_ARRAY"},
        }
    pq.write_table(_mixed(), path, row_group_size=M // 4, **write_kwargs)
    charge = _SAFETY_FACTOR * reader._UNMODELLED_MIN_BYTES
    sizes = _peaks(path)
    assert len(sizes) == 4
    assert all(charge < size < charge + (1 << 20) for size in sizes), sizes


@pytest.mark.parametrize("retyped", [False, True])
def test_large_unmodelled_leaves_pay_by_their_uncompressed_bytes(
    tmp_path, monkeypatch, retyped
):
    """Past the floor, the charge is a multiple of the unsized leaves' uncompressed
    bytes, once even when the read retypes them; the sized ``int64`` column isn't
    in it."""
    import ray.data._internal.datasource_v2.formats.parquet.footer_reader as reader

    monkeypatch.setattr(reader, "read_footer_stats", lambda metadata: None)
    path = tmp_path / "f.parquet"
    table = pa.table(
        {
            "i": pa.array(np.arange(N, dtype="int64")),
            "s": pa.array([f"distinct-value-{i:024d}" for i in range(N)]),
        }
    )
    # The dictionary outgrows its page and falls back to PLAIN; without a min and
    # max its page still leaves the strings unsized.
    pq.write_table(table, path, row_group_size=N, write_statistics=["i"])
    strings = pq.read_metadata(path).row_group(0).column(1).total_uncompressed_size
    charge = _SAFETY_FACTOR * reader._UNMODELLED_RATIO * strings
    assert charge > _SAFETY_FACTOR * reader._UNMODELLED_MIN_BYTES
    read_schema = None
    if retyped:
        read_schema = pa.schema([("i", pa.int64()), ("s", pa.large_string())])
    (size,) = _peaks(path, read_schema=read_schema)
    assert charge < size < charge + _SAFETY_FACTOR * (12 << 20), (size, charge)


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
    sys.exit(pytest.main(["-v", __file__]))
