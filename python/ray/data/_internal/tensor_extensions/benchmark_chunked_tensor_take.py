#!/usr/bin/env python3
"""Simple pyarrow-API reproduction + before/after benchmark for PR #64961.

Issue #64960 / PR #64961 avoid concatenating eligible multi-chunk Ray tensor
extension columns before a row take. This script reproduces the problem with a
small, self-contained pyarrow/numpy workload (no Ray runtime is started) and
reports the wall-clock and peak-memory cost before vs after the optimization.

Run::

    python python/ray/data/_internal/tensor_extensions/benchmark_chunked_tensor_take.py
    python .../benchmark_chunked_tensor_take.py --rows 100000 --width 3000 --chunks 32

The before/after switch is the PR's own operational flag,
``RAY_DATA_ENABLE_CHUNKED_TENSOR_TAKE``: the module-level
``ENABLE_CHUNKED_TENSOR_TAKE`` bool is read on every take, so toggling it in
process reproduces (before) the pre-PR full-column-concat + take path and
(after) the per-chunk direct-gather fast path. This is the same idiom the PR's
own ``test_chunked_tensor_take_can_be_disabled`` uses.
"""

import argparse
import math
import os
import resource
import statistics
import subprocess
import sys
import time

# Running this script by path puts its directory (this package) on sys.path[0],
# which shadows the installed `pandas` package with Ray's package-internal
# `pandas.py` shim (imported transitively when `ray.data` loads). Drop that
# entry so bare `import pandas` resolves to the installed package; `ray`
# itself resolves via PYTHONPATH or the installed package.
_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path[:] = [
    p for p in sys.path if p not in ("", ".") and os.path.abspath(p) != _HERE
]

import numpy as np  # noqa: E402
import pyarrow as pa  # noqa: E402

from ray.data._internal.arrow_ops.transform_pyarrow import take_table  # noqa: E402
from ray.data._internal.tensor_extensions import chunked_tensor_take  # noqa: E402

# Eligibility floor from chunked_tensor_take: a row must be at least
# _MIN_FAST_ROW_BYTES wide for the fast path to engage. float32 needs width>=64.
MIN_FAST_ROW_BYTES = chunked_tensor_take._MIN_FAST_ROW_BYTES


def _tensor_array(tensor_type, values):
    """Build a fixed-shape Ray tensor extension array (mirrors the test helper)."""
    values = np.ascontiguousarray(values)
    flat_values = values.reshape(-1)
    scalar_type = tensor_type.storage_type.value_type
    data = pa.Array.from_buffers(
        scalar_type,
        flat_values.size,
        [None, pa.py_buffer(flat_values)],
    )
    values_per_row = math.prod(tensor_type.shape)
    offsets = np.arange(
        0,
        (len(values) + 1) * values_per_row,
        values_per_row,
        dtype=np.dtype(tensor_type.OFFSET_DTYPE.to_pandas_dtype()),
    )
    storage = pa.Array.from_buffers(
        tensor_type.storage_type,
        len(values),
        [None, pa.py_buffer(offsets)],
        children=[data],
    )
    return tensor_type.wrap_array(storage)


def make_chunked_tensor_table(rows, width, chunks):
    """A multi-chunk ``ArrowTensorTypeV2`` column + the dense reference values.

    Empty slices are inserted (as in the PR tests) so the chunk layout is
    realistic: a leading empty chunk, a mid empty chunk, and a trailing one.
    """
    from ray.data._internal.tensor_extensions.arrow import ArrowTensorTypeV2

    tensor_type = ArrowTensorTypeV2((width,), pa.float32())
    values = np.arange(rows * width, dtype=np.float32).reshape(rows, width)
    array = _tensor_array(tensor_type, values)
    boundaries = np.linspace(0, rows, chunks + 1, dtype=np.int64)
    arrays = [
        array.slice(int(boundaries[i]), int(boundaries[i + 1] - boundaries[i]))
        for i in range(chunks)
    ]
    arrays.insert(0, array.slice(0, 0))
    arrays.insert(len(arrays) // 2, array.slice(rows // 2, 0))
    arrays.append(array.slice(rows, 0))
    column = pa.chunked_array(arrays, type=tensor_type)
    return pa.table({"tensor": column}), values


def _set_fast_path(enabled):
    chunked_tensor_take.ENABLE_CHUNKED_TENSOR_TAKE = enabled


def _time(fn, iters, warmup):
    for _ in range(warmup):
        fn()
    samples = []
    for _ in range(iters):
        start = time.perf_counter()
        fn()
        samples.append(time.perf_counter() - start)
    return statistics.mean(samples), min(samples)


def _take_once(table, indices):
    # Pin the result so the work is not optimized away; return for correctness.
    return take_table(table, indices)


def correctness_check(table, values, indices):
    """Both arms must produce identical tensor rows."""
    _set_fast_path(True)
    after = _take_once(table, indices)
    _set_fast_path(False)
    before = _take_once(table, indices)
    _set_fast_path(True)
    np.testing.assert_array_equal(
        before.column("tensor").combine_chunks().to_numpy(),
        after.column("tensor").combine_chunks().to_numpy(),
    )
    np.testing.assert_array_equal(
        after.column("tensor").combine_chunks().to_numpy(),
        values[indices],
    )
    return "correctness: BEFORE == AFTER == reference  OK"


def bench_timing(args):
    table, values = make_chunked_tensor_table(args.rows, args.width, args.chunks)
    rng = np.random.default_rng(0)
    indices = rng.integers(0, args.rows, size=args.batch, dtype=np.int64)

    row_bytes = args.width * 4

    _set_fast_path(False)
    before_mean, before_min = _time(
        lambda: _take_once(table, indices), args.iters, args.warmup
    )
    _set_fast_path(True)
    after_mean, after_min = _time(
        lambda: _take_once(table, indices), args.iters, args.warmup
    )

    lines = [
        f"config: rows={args.rows} width={args.width} "
        f"({row_bytes} B/row, fast-path-eligible={row_bytes >= MIN_FAST_ROW_BYTES}) "
        f"chunks={table.column('tensor').num_chunks} batch={args.batch} "
        f"iters={args.iters}",
        correctness_check(table, values, indices),
        "",
        f"{'arm':<22}{'mean (ms)':>12}{'min (ms)':>12}",
        f"{'before (full-concat)':<22}{before_mean * 1e3:>12.2f}{before_min * 1e3:>12.2f}",
        f"{'after (per-chunk)':<22}{after_mean * 1e3:>12.2f}{after_min * 1e3:>12.2f}",
        f"\nspeedup: {before_mean / after_mean:.2f}x  "
        f"({before_mean * 1e3:.2f} ms -> {after_mean * 1e3:.2f} ms)",
    ]
    return "\n".join(lines)


def _peak_rss_kb():
    # ru_maxrss is KiB on Linux (high-water mark of process resident memory).
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss


def _mem_arm(args, arm):
    """Run one arm alone in this process and print peak RSS so the parent parses it.

    The arm is chosen by the PR's own switch ``RAY_DATA_ENABLE_CHUNKED_TENSOR_TAKE``
    (read at import), not by an in-process toggle, so the child reproduces the
    pre-PR path (``0``) or the fast path (``1``) exactly as a reviewer would by
    setting the env var.

    ru_maxrss never decreases and freed-but-unreturned heap pages let a later,
    smaller allocation reuse already-reserved RSS. So: build the table, return
    freed pages to the OS with malloc_trim + gc.collect, record a steady-state
    baseline, then run ONE take while holding its result and read the peak. The
    before arm's full-column concat then shows up as a transient ~column-sized
    bump; the after arm only allocates the output + bounded scratch.
    """
    import ctypes
    import gc

    enabled = chunked_tensor_take.ENABLE_CHUNKED_TENSOR_TAKE
    table, _ = make_chunked_tensor_table(args.rows, args.width, args.chunks)
    rng = np.random.default_rng(0)
    indices = rng.integers(0, args.rows, size=args.batch, dtype=np.int64)
    gc.collect()
    try:
        ctypes.CDLL(None).malloc_trim(0)
    except Exception:  # pragma: no cover - non-Linux or libc unavailable
        pass
    baseline = _peak_rss_kb()
    result = _take_once(table, indices)  # hold the result so its memory stays live
    peak = _peak_rss_kb()
    _ = result  # keep reference alive until after the peak read
    print(f"MEM {arm} enabled={int(enabled)} {peak} {baseline}")


def bench_memory(args):
    """Spawn a fresh process per arm so each peak RSS is measured independently.

    Each child is run with the PR's own switch ``RAY_DATA_ENABLE_CHUNKED_TENSOR_TAKE``
    set in its environment (``0`` = before / pre-PR full-concat path, ``1`` =
    after / per-chunk fast path), exactly as a reviewer would. This avoids any
    in-process toggle and gives each arm a clean address space.
    """
    rows = [
        ("--rows", str(args.rows)),
        ("--width", str(args.width)),
        ("--chunks", str(args.chunks)),
        ("--batch", str(args.batch)),
        ("--iters", str(args.iters)),
    ]
    results = {}
    for arm, val in (("before", "0"), ("after", "1")):
        env = {**os.environ, "RAY_DATA_ENABLE_CHUNKED_TENSOR_TAKE": val}
        out = subprocess.run(
            [
                sys.executable,
                os.path.abspath(__file__),
                "--mem-arm",
                arm,
                *sum(rows, ()),
            ],
            check=True,
            capture_output=True,
            text=True,
            env=env,
        )
        for line in out.stdout.splitlines():
            if line.startswith("MEM "):
                _, a, _enabled, peak, base = line.split()
                results[a] = (int(peak), int(base))
    if not results:
        return "\n(peak-memory measurement unavailable)"
    b_peak, b_base = results["before"]
    a_peak, a_base = results["after"]
    lines = [
        "",
        "peak RSS (process high-water mark, incl. the table itself):",
        f"  before: {b_peak} KiB  after: {a_peak} KiB  "
        f"ratio (after/before): {a_peak / b_peak:.3f}",
    ]
    inc_b = b_peak - b_base
    inc_a = a_peak - a_base
    if inc_b > 0:
        lines.append(
            f"  incremental over the loaded table: before +{inc_b} KiB, "
            f"after +{inc_a} KiB (ratio {inc_a / inc_b:.3f})"
        )
    return "\n".join(lines)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--rows", type=int, default=50_000)
    p.add_argument("--width", type=int, default=1024)
    p.add_argument("--chunks", type=int, default=32)
    p.add_argument("--batch", type=int, default=8192)
    p.add_argument("--iters", type=int, default=15)
    p.add_argument("--warmup", type=int, default=3)
    p.add_argument(
        "--no-mem",
        action="store_true",
        help="skip the per-arm peak-RSS subprocess measurement",
    )
    p.add_argument(
        "--mem-arm", choices=["before", "after"], default=None, help=argparse.SUPPRESS
    )  # internal: run one arm for memory
    args = p.parse_args()

    if args.mem_arm:
        _mem_arm(args, args.mem_arm)
        return

    # Measure memory while this process is still lean: the per-arm peak is read
    # in fresh child processes, but a child's ru_maxrss can inherit this
    # process's resident set, so compute the memory string before the timing
    # loop bloats this process, then print timing first and memory second.
    mem_output = bench_memory(args) if not args.no_mem else ""
    timing_output = bench_timing(args)
    print(timing_output)
    if mem_output:
        print(mem_output)


if __name__ == "__main__":
    main()
