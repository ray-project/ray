"""Benchmark chunked tensor row takes from narrow through very wide rows.

Each invocation measures one configuration selected by ``--fast-path`` so the
release system can compare the same metrics across builds. The width sweep uses
takes of up to 128 rows at 128 B, 1 KiB, 8 KiB, 64 KiB, 512 KiB, 4 MiB, and
32 MiB per row. Source payload is capped at about 128 MiB; very wide cases
reduce their source rows, output rows, and nonempty chunks while retaining at
least two chunks so they still exercise the multi-chunk path.

A small-take boundary case uses 1 KiB rows, 8 MiB of source payload, 32 chunks,
and one output row. It guards a small-output configuration close to the
per-chunk amortization boundary.

A single-chunk control tracks the path used by ineligible columns. A complete
ShufflingBatcher lifecycle uses a production-derived ``(2000, 1697)`` tensor
shape with 320 rows, 32 source chunks, and 128-row batches to cover one-row
scratch subbatching and prepared-plan reuse. A smaller streaming case interleaves
adds and takes and forces multiple buffer compactions, including carry-over rows.
"""

import argparse
import gc
import json
import math
import os
import statistics
import subprocess
import sys
import threading
import time

import numpy as np
import pyarrow as pa

from ray.data._internal import batcher as batcher_module
from ray.data._internal.arrow_ops.transform_pyarrow import take_table
from ray.data._internal.batcher import ShufflingBatcher
from ray.data._internal.tensor_extensions import chunked_tensor_take
from ray.data._internal.tensor_extensions.arrow import ArrowTensorTypeV2


def _tensor_array(tensor_type, values):
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


def _make_table(rows, width, chunks, *, single_chunk):
    tensor_type = ArrowTensorTypeV2((width,), pa.float32())
    values = np.arange(rows * width, dtype=np.float32).reshape(rows, width)
    array = _tensor_array(tensor_type, values)
    if single_chunk:
        return pa.table({"tensor": array}), values

    boundaries = np.linspace(0, rows, chunks + 1, dtype=np.int64)
    arrays = [
        array.slice(
            int(boundaries[index]),
            int(boundaries[index + 1] - boundaries[index]),
        )
        for index in range(chunks)
    ]
    # Include empty slices to cover the layouts produced by real block pipelines.
    arrays.insert(0, array.slice(0, 0))
    arrays.insert(len(arrays) // 2, array.slice(rows // 2, 0))
    arrays.append(array.slice(rows, 0))
    return pa.table({"tensor": pa.chunked_array(arrays, type=tensor_type)}), values


def _set_fast_path(enabled):
    chunked_tensor_take.ENABLE_CHUNKED_TENSOR_TAKE = enabled


def _time_operation(operation, *, iterations, warmup):
    for _ in range(warmup):
        operation()

    samples = []
    for _ in range(iterations):
        start = time.perf_counter()
        result = operation()
        samples.append(time.perf_counter() - start)
        # Keep each result alive until its elapsed time has been recorded.
        del result
    return {
        "mean_s": statistics.mean(samples),
        "min_s": min(samples),
        "median_s": statistics.median(samples),
        "p90_s": float(np.percentile(samples, 90)),
    }


def _make_shuffle_blocks(rows, shape, chunks):
    tensor_type = ArrowTensorTypeV2(tuple(shape), pa.float32())
    values = np.empty((rows, *shape), dtype=np.float32)
    flat_values = values.reshape(rows, -1)
    flat_values[:] = np.arange(rows, dtype=np.float32)[:, None]
    array = _tensor_array(tensor_type, values)
    boundaries = np.linspace(0, rows, chunks + 1, dtype=np.int64)
    return [
        pa.table(
            {
                "tensor": array.slice(
                    int(boundaries[index]),
                    int(boundaries[index + 1] - boundaries[index]),
                )
            }
        )
        for index in range(chunks)
    ]


def _run_shuffle_lifecycle(blocks, *, batch_size, buffer_rows, seed=0):
    original_get_memory = batcher_module.get_total_obj_store_mem_on_node
    try:
        # The benchmark exercises local batching without starting Ray. The value
        # is used only for a spill-risk warning and is outside the take path.
        batcher_module.get_total_obj_store_mem_on_node = lambda: 1 << 60
        batcher = ShufflingBatcher(
            batch_size=batch_size,
            shuffle_buffer_min_size=buffer_rows,
            shuffle_seed=seed,
        )
    finally:
        batcher_module.get_total_obj_store_mem_on_node = original_get_memory
    batches = []

    def consume_ready():
        while batcher.has_batch():
            batches.append(batcher.next_batch())

    for block in blocks:
        batcher.add(block)
        consume_ready()
    batcher.done_adding()
    consume_ready()
    if batcher.has_any():
        batches.append(batcher.next_batch())
    return batches


def _shuffle_result_signature(batches):
    """Return schemas, row IDs, and full-row bounds without retaining outputs."""
    schemas = [batch.schema for batch in batches]
    row_ids = []
    payload_bounds = []
    for batch in batches:
        values = (
            batch.column("tensor").combine_chunks().to_numpy().reshape(len(batch), -1)
        )
        row_ids.append(values[:, 0])
        bounds = np.empty((len(values), 2), dtype=values.dtype)
        for index, row in enumerate(values):
            # Scan each full row without allocating a tensor-sized comparison.
            bounds[index] = row.min(), row.max()
        payload_bounds.append(bounds)
    return schemas, np.concatenate(row_ids), np.concatenate(payload_bounds)


def _check_correctness(table, values, indices):
    result = take_table(table, indices)
    result_values = result.column("tensor").combine_chunks().to_numpy()
    np.testing.assert_array_equal(result_values, values[indices])


def _current_rss_kib():
    with open("/proc/self/statm") as statm:
        resident_pages = int(statm.read().split()[1])
    return resident_pages * os.sysconf("SC_PAGE_SIZE") // 1024


def _measure_operation_memory(operation):
    """Measure current RSS while excluding source-construction peak history.

    The source is built before this function, so sampling current RSS instead of
    process-lifetime ``ru_maxrss`` prevents construction temporaries from
    masking the take's incremental memory. The operation runs in this dedicated
    memory-only subprocess; a sampler thread observes transient Arrow buffers.
    """
    gc.collect()
    try:
        import ctypes

        ctypes.CDLL(None).malloc_trim(0)
    except (AttributeError, OSError):
        pass

    peak = [0]
    ready = threading.Event()
    start_sampling = threading.Event()
    finished = threading.Event()

    def sample_rss():
        ready.set()
        start_sampling.wait()
        while not finished.is_set():
            peak[0] = max(peak[0], _current_rss_kib())

    sampler = threading.Thread(target=sample_rss, daemon=True)
    sampler.start()
    ready.wait()
    baseline = _current_rss_kib()
    peak[0] = baseline
    start_sampling.set()
    try:
        result = operation()
        peak[0] = max(peak[0], _current_rss_kib())
        _ = result
    finally:
        finished.set()
        sampler.join()
    return peak[0], baseline


def _run_memory_arm(args):
    if args.memory_case == "shuffle":
        blocks = _make_shuffle_blocks(
            args.shuffle_rows,
            args.shuffle_shape,
            args.shuffle_chunks,
        )

        def operation():
            return _run_shuffle_lifecycle(
                blocks,
                batch_size=args.shuffle_batch_size,
                buffer_rows=args.shuffle_rows,
            )

    else:
        table, _ = _make_table(
            args.rows,
            args.width,
            args.chunks,
            single_chunk=args.single_chunk,
        )
        indices = np.random.default_rng(0).integers(
            0,
            args.rows,
            size=args.batch_size,
            dtype=np.int64,
        )

        def operation():
            return take_table(table, indices)

    peak, baseline = _measure_operation_memory(operation)
    print(f"MEMORY {peak} {baseline}")


def _measure_memory(
    args,
    *,
    rows=None,
    width=None,
    chunks=None,
    batch_size=None,
    single_chunk=False,
    memory_case="take",
):
    rows = args.rows if rows is None else rows
    width = args.width if width is None else width
    chunks = args.chunks if chunks is None else chunks
    batch_size = args.batch_size if batch_size is None else batch_size
    command = [
        sys.executable,
        os.path.abspath(__file__),
        "--fast-path",
        args.fast_path,
        "--rows",
        str(rows),
        "--width",
        str(width),
        "--chunks",
        str(chunks),
        "--batch-size",
        str(batch_size),
        "--shuffle-rows",
        str(args.shuffle_rows),
        "--shuffle-shape",
        *(str(dimension) for dimension in args.shuffle_shape),
        "--shuffle-chunks",
        str(args.shuffle_chunks),
        "--shuffle-batch-size",
        str(args.shuffle_batch_size),
        "--memory-case",
        memory_case,
        "--memory-only",
    ]
    if single_chunk:
        command.append("--single-chunk")

    completed = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
    )
    memory_line = next(
        line for line in completed.stdout.splitlines() if line.startswith("MEMORY ")
    )
    _, peak, baseline = memory_line.split()
    peak = int(peak)
    baseline = int(baseline)
    return {
        "peak_rss_kib": peak,
        "baseline_rss_kib": baseline,
        "incremental_peak_rss_kib": peak - baseline,
    }


def _run_case(args, *, rows, width, batch_size, single_chunk, chunks=None):
    chunks = args.chunks if chunks is None else chunks
    table, values = _make_table(
        rows,
        width,
        chunks,
        single_chunk=single_chunk,
    )
    indices = np.random.default_rng(0).integers(
        0,
        rows,
        size=batch_size,
        dtype=np.int64,
    )
    fast_path_prepared = (
        chunked_tensor_take.try_prepare_chunked_tensor_take(
            table.column("tensor"),
            max_output_rows=batch_size,
        )
        is not None
    )
    _check_correctness(table, values, indices)

    measurement = _time_operation(
        lambda: take_table(table, indices),
        iterations=args.iterations,
        warmup=args.warmup,
    )

    metrics = {
        "time": measurement["median_s"],
        **measurement,
        "rows": rows,
        "row_width": width,
        "row_bytes": width * np.dtype(np.float32).itemsize,
        "source_payload_bytes": rows * width * np.dtype(np.float32).itemsize,
        "source_chunks": table.column("tensor").num_chunks,
        "nonempty_source_chunks": sum(
            len(chunk) > 0 for chunk in table.column("tensor").chunks
        ),
        "batch_size": batch_size,
        "fast_path_prepared": fast_path_prepared,
    }
    if not args.no_memory:
        metrics.update(
            _measure_memory(
                args,
                rows=rows,
                width=width,
                chunks=chunks,
                batch_size=batch_size,
                single_chunk=single_chunk,
            )
        )
    return metrics


def _run_width_sweep(args):
    results = {}
    float32_bytes = np.dtype(np.float32).itemsize
    for row_bytes in args.sweep_row_bytes:
        if row_bytes % float32_bytes != 0:
            raise ValueError(
                f"Sweep row size {row_bytes} is not divisible by float32 size"
            )
        width = row_bytes // float32_bytes
        rows = min(args.sweep_max_rows, args.sweep_max_source_bytes // row_bytes)
        # A row is irreducible, while two nonempty chunks are the minimum source
        # that can exercise the fast path. Reduce the chunk count instead of
        # exceeding the source-payload cap for very wide rows.
        rows = max(2, rows)
        chunks = min(args.chunks, rows)
        results[f"row_bytes_{row_bytes}"] = _run_case(
            args,
            rows=rows,
            width=width,
            chunks=chunks,
            batch_size=min(args.sweep_batch_size, rows),
            single_chunk=False,
        )
    return results


def _run_shuffle_case(
    args,
    *,
    rows,
    shape,
    chunks,
    batch_size,
    buffer_rows,
    measure_memory,
):
    blocks = _make_shuffle_blocks(rows, shape, chunks)

    actual = _run_shuffle_lifecycle(
        blocks,
        batch_size=batch_size,
        buffer_rows=buffer_rows,
    )
    assert all(len(batch) == batch_size for batch in actual[:-1])
    assert 0 < len(actual[-1]) <= batch_size
    actual_schemas, actual_row_ids, actual_bounds = _shuffle_result_signature(actual)
    assert all(schema == blocks[0].schema for schema in actual_schemas)
    np.testing.assert_array_equal(
        np.sort(actual_row_ids),
        np.arange(rows, dtype=np.float32),
    )
    np.testing.assert_array_equal(
        actual_bounds,
        np.repeat(actual_row_ids[:, None], actual_bounds.shape[1], axis=1),
    )
    del actual
    gc.collect()

    measurement = _time_operation(
        lambda: _run_shuffle_lifecycle(
            blocks,
            batch_size=batch_size,
            buffer_rows=buffer_rows,
        ),
        iterations=args.iterations,
        warmup=args.warmup,
    )

    metrics = {
        "time": measurement["median_s"],
        **measurement,
        "rows": rows,
        "row_shape": shape,
        "source_chunks": len(blocks),
        "batch_size": batch_size,
        "shuffle_buffer_rows": buffer_rows,
    }
    # The memory arms run in fresh child processes. Release the parent's large
    # production-derived source first so its resident pages do not overlap the
    # child RSS.
    blocks = None
    gc.collect()
    if measure_memory and not args.no_memory:
        metrics.update(_measure_memory(args, memory_case="shuffle"))
    return metrics


def _parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--fast-path",
        choices=("enabled", "disabled"),
        default="enabled",
        help="Select the implementation measured by this benchmark.",
    )
    parser.add_argument("--rows", type=int, default=8192)
    parser.add_argument("--width", type=int, default=1024)
    parser.add_argument("--chunks", type=int, default=32)
    parser.add_argument("--batch-size", type=int, default=128)
    parser.add_argument("--iterations", type=int, default=15)
    parser.add_argument("--warmup", type=int, default=3)
    parser.add_argument(
        "--sweep-row-bytes",
        type=int,
        nargs="+",
        default=(
            128,
            1024,
            8 * 1024,
            64 * 1024,
            512 * 1024,
            4 * 1024 * 1024,
            32 * 1024 * 1024,
        ),
        help="Payload bytes per float32 row in the direct-take width sweep.",
    )
    parser.add_argument("--sweep-max-rows", type=int, default=8192)
    parser.add_argument(
        "--sweep-max-source-bytes",
        type=int,
        default=128 * 1024 * 1024,
    )
    parser.add_argument("--sweep-batch-size", type=int, default=128)
    parser.add_argument("--shuffle-rows", type=int, default=320)
    parser.add_argument(
        "--shuffle-shape",
        type=int,
        nargs=2,
        default=(2000, 1697),
    )
    parser.add_argument("--shuffle-chunks", type=int, default=32)
    parser.add_argument("--shuffle-batch-size", type=int, default=128)
    parser.add_argument("--streaming-rows", type=int, default=448)
    parser.add_argument("--streaming-width", type=int, default=1024)
    parser.add_argument("--streaming-chunks", type=int, default=28)
    parser.add_argument("--streaming-batch-size", type=int, default=128)
    parser.add_argument("--streaming-buffer-rows", type=int, default=128)
    parser.add_argument("--no-memory", action="store_true")
    parser.add_argument("--single-chunk", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument(
        "--memory-case",
        choices=["take", "shuffle"],
        default="take",
        help=argparse.SUPPRESS,
    )
    parser.add_argument("--memory-only", action="store_true", help=argparse.SUPPRESS)
    return parser.parse_args()


def main():
    args = _parse_args()
    _set_fast_path(args.fast_path == "enabled")
    if args.memory_only:
        _run_memory_arm(args)
        return

    results = {
        "width_sweep": _run_width_sweep(args),
        "small_take_eligibility_boundary": _run_case(
            args,
            rows=8192,
            width=256,
            chunks=32,
            batch_size=1,
            single_chunk=False,
        ),
        "single_chunk_control": _run_case(
            args,
            rows=args.rows,
            width=args.width,
            batch_size=args.batch_size,
            single_chunk=True,
        ),
        "shuffle_lifecycle": _run_shuffle_case(
            args,
            rows=args.shuffle_rows,
            shape=tuple(args.shuffle_shape),
            chunks=args.shuffle_chunks,
            batch_size=args.shuffle_batch_size,
            buffer_rows=args.shuffle_rows,
            measure_memory=True,
        ),
        "streaming_shuffle_lifecycle": _run_shuffle_case(
            args,
            rows=args.streaming_rows,
            shape=(args.streaming_width,),
            chunks=args.streaming_chunks,
            batch_size=args.streaming_batch_size,
            buffer_rows=args.streaming_buffer_rows,
            measure_memory=False,
        ),
    }
    output_path = os.environ.get("TEST_OUTPUT_JSON", "./result.json")
    with open(output_path, "w") as output_file:
        json.dump(results, output_file)
    print(json.dumps(results, indent=2))
    print(f"Metrics written to {output_path}")


if __name__ == "__main__":
    main()
