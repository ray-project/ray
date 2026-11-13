"""Tests for the Parquet decode-peak estimate, against PyArrow's measured peak."""

import json
import subprocess
import sys

import numpy as np
import pytest

from ray.data._internal.datasource_v2.formats.parquet.decoded_size import (
    build_leaf_profiles,
    leaf_chunks,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_decode_peak import (
    estimate_row_group_peak,
    estimate_row_group_work,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_size_statistics import (
    read_footer_stats,
)

pa = pytest.importorskip("pyarrow")
pds = pytest.importorskip("pyarrow.dataset")
pq = pytest.importorskip("pyarrow.parquet")

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


def _estimate(path, columns=None, threads=1, **format_kwargs):
    """The peak estimate for ``path``'s one row group, read as the footer reader does."""
    metadata = pq.read_metadata(str(path))
    profiles = build_leaf_profiles(
        parquet_schema=metadata.schema,
        format_kwargs=format_kwargs,
        file_metadata=metadata,
    )
    assert profiles is not None
    row_group = metadata.row_group(0)
    stats = read_footer_stats(metadata)
    leaves = leaf_chunks(
        row_group=row_group,
        leaf_profiles=profiles,
        leaf_indices=range(row_group.num_columns),
        size_stats=None if stats is None else stats[0],
    )
    schema_columns = [metadata.schema.column(j) for j in range(metadata.num_columns)]
    tops = [column.path.split(".")[0] for column in schema_columns]
    names = columns or metadata.schema.to_arrow_schema().names
    column_leaves = [[j for j, top in enumerate(tops) if top == name] for name in names]
    decoded, buffers = estimate_row_group_work(
        row_group=row_group,
        columns=schema_columns,
        column_leaves=column_leaves,
        leaves=leaves,
        counted=set(),
        decode_threads=threads,
    )
    return estimate_row_group_peak(
        row_group=row_group,
        leaf_indices=sorted(j for leaf_indices in column_leaves for j in leaf_indices),
        row_group_work=decoded + buffers,
        file_size=path.stat().st_size,
    )


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


@pytest.mark.parametrize("label", list(_CASES))
def test_estimate_bounds_measured_peak(tmp_path, label):
    table, write_kwargs, format_kwargs, columns, high, threads = _CASES[label]
    path = tmp_path / f"{label}.parquet"
    pq.write_table(table, path, row_group_size=N, **write_kwargs)
    kwargs = dict(path=path, columns=columns, threads=threads, **format_kwargs)
    ratio = _estimate(**kwargs) / _peak(**kwargs)
    assert 0.95 <= ratio <= high, ratio


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
