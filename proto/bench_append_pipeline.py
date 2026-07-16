"""Prototype + benchmark: appending columns across a pipeline of stages.

Motivation
----------
A common actor-pipeline shape is: stage 0 produces a (wide) table, and each
subsequent stage appends a derived column and hands the result to the next
stage. Arrow tables are immutable graphs of independent Buffers, and
`Table.append_column` is zero-copy w.r.t. the existing columns. But every
transport we have (plasma, and the current shm/vm Flight backends) serializes
the whole table into ONE contiguous blob, so each stage re-copies the entire
(growing) table even though the original columns already live in shared memory.

This script compares two ways for a stage to hand its output to the next stage,
both using real anonymous shared memory (ray._raylet.shm_create_buffer):

  copy  : reconstruct table, append column, re-serialize the ENTIRE table into a
          new shm region. Bytes copied per stage grow with the whole table.
  share : write ONLY the new column into a small new shm region and extend a
          manifest that still references the base region(s). Reconstruction is
          zero-copy across N+1 mmaps. Bytes copied per stage ~ new column only.

It reports bytes copied and wall-clock per strategy, and verifies both produce
a table equal to the ground-truth in-memory append chain.

Run:
    python proto/bench_append_pipeline.py --rows 1250000 --base-cols 12 --stages 4
"""

import argparse
import mmap
import os
import time
from typing import List, Tuple

import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc

from ray._raylet import shm_create_buffer

# A materialized shared-memory region: (fd, mmap, size).
Region = Tuple[int, "mmap.mmap", int]


def _serialize_table(table: "pa.Table") -> "pa.Buffer":
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, table.schema) as w:
        w.write_table(table)
    return sink.getvalue()


def _write_region(buf) -> Region:
    """Copy a buffer-like into a fresh anonymous shm region."""
    src = memoryview(buf).cast("B")
    size = src.nbytes
    fd = shm_create_buffer(size)
    mm = mmap.mmap(fd, size)
    dst = memoryview(mm).cast("B")
    dst[:] = src
    dst.release()
    return (fd, mm, size)


def _open_region_table(region: Region) -> "pa.Table":
    """Reconstruct the pa.Table stored in `region`, zero-copy (views the mmap)."""
    _fd, mm, _size = region
    return ipc.open_stream(pa.py_buffer(mm)).read_all()


def _close_region(region: Region) -> None:
    fd, mm, _size = region
    try:
        mm.close()
    finally:
        os.close(fd)


class SharedTable:
    """A table represented as a manifest of shm regions:

      - one base region holding the full base table's IPC stream, plus
      - zero or more appended regions, each holding a single-column IPC stream.

    The logical table is base + appended columns, reconstructed zero-copy by
    opening each region (which views its mmap) and chaining append_column.
    Appending a column is O(new column) — the base region is referenced, never
    re-copied.
    """

    def __init__(self, base: Region):
        self._base = base
        self._appends: List[Tuple[str, Region]] = []
        # Regions this manifest owns and must keep alive / clean up. Appended
        # manifests share the same base + prior append regions by reference.
        self._owned: List[Region] = [base]

    @classmethod
    def from_table(cls, table: "pa.Table") -> "SharedTable":
        return cls(_write_region(_serialize_table(table)))

    def append_column_shared(self, name: str, column: "pa.Array") -> "SharedTable":
        """Return a new manifest = self + one column, writing ONLY the new
        column to shm. The base and prior append regions are shared (referenced),
        not copied."""
        col_table = pa.table({name: column})
        region = _write_region(_serialize_table(col_table))
        out = SharedTable(self._base)
        out._appends = self._appends + [(name, region)]
        out._owned = [region]  # only the newly written region is ours to free
        return out

    def open(self) -> "pa.Table":
        """Reconstruct the logical pa.Table, zero-copy across all regions.

        The returned table transitively references every region's mmap (via
        pa.py_buffer), keeping them mapped as long as the table is alive.
        """
        table = _open_region_table(self._base)
        for name, region in self._appends:
            col_table = _open_region_table(region)
            table = table.append_column(name, col_table.column(0))
        return table

    def num_regions(self) -> int:
        return 1 + len(self._appends)

    def bytes_resident(self) -> int:
        return self._base[2] + sum(r[2] for _n, r in self._appends)


def _make_base_table(rows: int, base_cols: int) -> "pa.Table":
    rng = np.random.default_rng(0)
    return pa.table({f"c{i}": rng.standard_normal(rows) for i in range(base_cols)})


def _derived_column(rows: int, stage: int) -> "pa.Array":
    # A per-stage derived column, same #rows as the table.
    rng = np.random.default_rng(1000 + stage)
    return pa.array(rng.standard_normal(rows))


def run_copy(base: "pa.Table", stages: int, expected: "pa.Table"):
    """Each stage: reconstruct -> append -> re-serialize the WHOLE table.

    Returns (elapsed_s, bytes_copied, correct, regions). Correctness is checked
    here (not returned as a table) so we can drop the zero-copy views before
    unmapping the regions.
    """
    rows = base.num_rows
    bytes_copied = 0
    table = final = None
    t0 = time.perf_counter()

    region = _write_region(_serialize_table(base))
    regions = [region]
    try:
        for stage in range(stages):
            table = _open_region_table(region)
            table = table.append_column(f"d{stage}", _derived_column(rows, stage))
            region = _write_region(_serialize_table(table))
            regions.append(region)
            bytes_copied += region[2]  # entire (growing) table re-copied
        final = _open_region_table(region)
        correct = final.equals(expected)
        elapsed = time.perf_counter() - t0
        return elapsed, bytes_copied, correct, 1
    finally:
        # Drop zero-copy views before unmapping (mmap.close errors otherwise).
        table = final = None
        for r in regions:
            _close_region(r)


def run_share(base: "pa.Table", stages: int, expected: "pa.Table"):
    """Each stage: write ONLY the new column; manifest references the base."""
    rows = base.num_rows
    bytes_copied = 0
    final = None
    t0 = time.perf_counter()

    shared = SharedTable.from_table(base)
    owned: List[Region] = [shared._base]
    try:
        for stage in range(stages):
            before = shared.bytes_resident()
            shared = shared.append_column_shared(
                f"d{stage}", _derived_column(rows, stage)
            )
            owned.extend(shared._owned)
            bytes_copied += shared.bytes_resident() - before  # only the new column
        num_regions = shared.num_regions()
        final = shared.open()
        correct = final.equals(expected)
        elapsed = time.perf_counter() - t0
        return elapsed, bytes_copied, correct, num_regions
    finally:
        final = None
        # De-dup regions before closing (manifests share the base by reference).
        seen = set()
        for r in owned:
            if r[0] not in seen:
                seen.add(r[0])
                _close_region(r)


def _expected(base: "pa.Table", stages: int) -> "pa.Table":
    rows = base.num_rows
    t = base
    for stage in range(stages):
        t = t.append_column(f"d{stage}", _derived_column(rows, stage))
    return t


def parse_args():
    p = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument("--rows", type=int, default=1_250_000)
    p.add_argument("--base-cols", type=int, default=12)
    p.add_argument("--stages", type=int, default=4)
    p.add_argument("--repeat", type=int, default=3, help="best-of-N timing")
    return p.parse_args()


def main():
    args = parse_args()
    base = _make_base_table(args.rows, args.base_cols)
    base_mb = base.nbytes / 1e6
    col_mb = _derived_column(args.rows, 0).nbytes / 1e6

    expected = _expected(base, args.stages)

    def best(fn):
        # (elapsed, bytes, correct, regions); keep the min-elapsed run.
        result = None
        for _ in range(args.repeat):
            out = fn()
            if result is None or out[0] < result[0]:
                result = out
        return result

    copy_res = best(lambda: run_copy(base, args.stages, expected))
    share_res = best(lambda: run_share(base, args.stages, expected))

    copy_t, copy_bytes, copy_ok, _ = copy_res
    share_t, share_bytes, share_ok, share_regions = share_res

    print(
        f"Base table:     {base_mb:.1f} MB  ({args.base_cols} cols x {args.rows} rows)"
    )
    print(f"Appended col:   {col_mb:.1f} MB each,  {args.stages} stages")
    print(
        f"Final table:    {expected.nbytes / 1e6:.1f} MB  ({expected.num_columns} cols)"
    )
    print()
    print(
        f"{'strategy':8s}  {'stage bytes copied':>20s}  {'wall (ms)':>10s}  regions  correct"
    )
    print(
        f"{'copy':8s}  {copy_bytes / 1e6:>17.1f} MB  {copy_t * 1e3:>10.2f}  "
        f"{'1':>7s}  {copy_ok}"
    )
    print(
        f"{'share':8s}  {share_bytes / 1e6:>17.1f} MB  {share_t * 1e3:>10.2f}  "
        f"{share_regions:>7d}  {share_ok}"
    )
    print()
    if share_bytes > 0:
        print(
            f"share copies {copy_bytes / max(share_bytes, 1):.1f}x fewer bytes; "
            f"{copy_t / max(share_t, 1e-9):.1f}x faster wall-clock"
        )

    if not (copy_ok and share_ok):
        raise SystemExit("CORRECTNESS FAILURE")


if __name__ == "__main__":
    main()
