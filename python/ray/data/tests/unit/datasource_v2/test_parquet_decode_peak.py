"""Tests for the Parquet decode-peak estimate, against PyArrow's measured peak."""

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
    estimate_row_group_peak,
    estimate_row_group_work,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_size_statistics import (
    read_footer_stats,
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
        [sys.executable, "-c", _SCAN, task],
        capture_output=True,
        text=True,
        check=True,
        # One allocator, whatever the caller's environment selects.
        env={**os.environ, "ARROW_DEFAULT_MEMORY_POOL": "system"},
    )
    scanned, peak = map(int, result.stdout.split())
    assert scanned == rows
    return peak


def _estimate(path, columns=None, threads=1):
    """The peak estimates of ``path``'s row groups, summed as a read task's are,
    read as the footer reader does."""
    metadata = pq.read_metadata(str(path))
    profiles = build_leaf_profiles(
        parquet_schema=metadata.schema, format_kwargs={}, file_metadata=metadata
    )
    assert profiles is not None
    stats = read_footer_stats(metadata)
    schema_columns = [metadata.schema.column(j) for j in range(metadata.num_columns)]
    names = columns or metadata.schema.to_arrow_schema().names
    column_leaves = [
        [
            j
            for j, column in enumerate(schema_columns)
            if column.path == name or column.path.startswith(f"{name}.")
        ]
        for name in names
    ]
    estimate = 0
    for i in range(metadata.num_row_groups):
        row_group = metadata.row_group(i)
        leaves = leaf_chunks(
            row_group=row_group,
            leaf_profiles=profiles,
            leaf_indices=range(row_group.num_columns),
            size_stats=None if stats is None else stats[i],
        )
        decoded, buffers = estimate_row_group_work(
            row_group=row_group,
            columns=schema_columns,
            column_leaves=column_leaves,
            leaves=leaves,
            decode_threads=threads,
        )
        estimate += estimate_row_group_peak(
            row_group=row_group,
            leaf_indices=sorted(j for column in column_leaves for j in column),
            row_group_work=decoded + buffers,
            file_size=path.stat().st_size,
        )
    return estimate


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


def _case(
    make, rows=N, row_groups=1, write_kwargs=None, columns=None, high=1.5, threads=1
):
    """A ``_CASES`` entry: ``make`` builds the table of ``rows`` rows, written as
    ``row_groups`` row groups, and ``high`` caps the estimate over the measured peak."""
    return SimpleNamespace(
        make=make,
        rows=rows,
        row_groups=row_groups,
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
    # Only the int64 column, which follows the list in the file.
    "column-alone-after-list": _case(
        lambda: pa.table({"l": _eight_lists()["l0"], "y": _ints()}),
        columns=["y"],
    ),
    # Measured 1.62: the levels and values are charged at twice their slots.
    "nested-lists": _case(
        lambda: pa.table({"n": pa.array([[[i, i + 1], [i + 2]] for i in range(N)])}),
        high=2.0,
    ),
    # A read task's estimate is the sum of its row groups'. Measured 1.89: each is
    # charged its own footer read and buffers, which one CPU thread holds in turn.
    "four-row-groups": _case(
        lambda: pa.table({"a": _ints(), "s": _strings(1000)}),
        row_groups=4,
        high=2.0,
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
    pq.write_table(
        case.make(),
        path,
        row_group_size=case.rows // case.row_groups,
        **case.write_kwargs,
    )
    return path


@pytest.mark.parametrize("label", list(_CASES))
def test_estimate_bounds_measured_peak(tmp_path, label):
    case = _CASES[label]
    path = _write(tmp_path, label)
    estimate = _estimate(path=path, columns=case.columns, threads=case.threads)
    peak = _peak(path=path, rows=case.rows, columns=case.columns, threads=case.threads)
    # The estimate carries a flat safety factor over the pool's peak.
    ratio = estimate / (_SAFETY_FACTOR * peak)
    assert 1.0 <= ratio <= case.high, ratio


def test_no_thread_count_charges_every_columns_buffers(tmp_path):
    path = _write(tmp_path, "similar-lists")
    assert (
        _estimate(path=path, threads=1)
        < _estimate(path=path, threads=8)
        == _estimate(path=path, threads=0)
    )


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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
