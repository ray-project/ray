"""Benchmark 2: peak receiver memory, staging buffer versus direct receive.

The straightforward way to sync weights is to ``ray.get`` the remote weights and
then ``copy_`` them into the local model. That is convenient, and it leaves room
for post-processing, but it costs a payload-sized staging buffer on the receiver
at exactly the moment when an inference engine has the least memory to spare.

``ray.experimental.set_target_for_ref`` removes that buffer by telling RDT to
land the incoming data in the local model weights directly, so the RDMA read
writes into its final destination.

Both arms pre-register the receiver's weights, and the payload is a single view,
so the only difference between them is where the data lands. Peak memory is read
from the CUDA caching allocator inside the generator actor, relative to a
baseline taken after the model is already resident.

Usage:
    python rdt_peak_memory_benchmark.py                    # cross-node
    python rdt_peak_memory_benchmark.py --single-node      # local check
"""

import argparse

from rdt_weight_sync_common import (
    GIB,
    MIB,
    add_common_args,
    assert_gpu_headroom,
    describe_setup,
    format_bytes,
    make_actors,
    perf_metric,
    print_table,
    write_csv,
    write_perf_metrics,
)

import ray

PAYLOAD_SIZES = [100 * MIB, 1 * GIB, 10 * GIB]
NUM_VIEWS = 1
DEFAULT_ITERS = 3
MODES = ["staging", "direct"]


def run_mode(single_node, size_bytes, mode, iters):
    """Measure receiver peak memory for one payload size and receive mode."""
    use_target_buffers = mode == "direct"
    trainer, generator = make_actors(single_node)
    trainer_info, generator_info = ray.get(
        [
            trainer.setup.remote(size_bytes, NUM_VIEWS, pre_register=True),
            generator.setup.remote(size_bytes, NUM_VIEWS, pre_register=True),
        ]
    )
    describe_setup(f"trainer ({mode}, {format_bytes(size_bytes)})", trainer_info)
    describe_setup(f"generator ({mode}, {format_bytes(size_bytes)})", generator_info)
    if not use_target_buffers:
        assert_gpu_headroom(
            generator_info,
            trainer_info["payload_bytes"],
            f"staging arm at {format_bytes(size_bytes)}",
        )

    # Warm up so connection setup does not show up as receiver memory. The
    # measured calls empty the cache first, so a warmup staging buffer left in
    # the allocator does not carry over.
    ref = ray.get(trainer.put_views.remote())
    ray.get(generator.sync_weights.remote([ref], use_target_buffers))
    del ref

    rows = []
    for iteration in range(1, iters + 1):
        ref = ray.get(trainer.put_views.remote())
        result = ray.get(
            generator.sync_weights.remote(
                [ref], use_target_buffers, measure_peak_memory=True
            )
        )
        del ref
        rows.append(
            {
                "mode": mode,
                "payload_bytes": trainer_info["payload_bytes"],
                "num_views": NUM_VIEWS,
                "iteration": iteration,
                "baseline_bytes": result["baseline_bytes"],
                "peak_bytes": result["peak_bytes"],
                "peak_over_baseline_bytes": result["peak_over_baseline_bytes"],
                "peak_reserved_bytes": result["peak_reserved_bytes"],
                "seconds": result["seconds"],
            }
        )

    ray.kill(trainer)
    ray.kill(generator)
    return rows


def build_comparison(rows, sizes):
    """Fold the per-iteration rows into one staging-versus-direct row per size."""
    comparison = []
    for size_bytes in sizes:
        entry = {"payload": format_bytes(size_bytes)}
        for mode in MODES:
            subset = [
                row
                for row in rows
                if row["mode"] == mode and row["requested_bytes"] == size_bytes
            ]
            if not subset:
                continue
            entry[f"{mode}_peak_over_baseline"] = format_bytes(
                max(row["peak_over_baseline_bytes"] for row in subset)
            )
            entry[f"{mode}_peak_reserved"] = format_bytes(
                max(row["peak_reserved_bytes"] for row in subset)
            )
        staging = [
            row
            for row in rows
            if row["mode"] == "staging" and row["requested_bytes"] == size_bytes
        ]
        direct = [
            row
            for row in rows
            if row["mode"] == "direct" and row["requested_bytes"] == size_bytes
        ]
        if staging and direct:
            saved = max(row["peak_over_baseline_bytes"] for row in staging) - max(
                row["peak_over_baseline_bytes"] for row in direct
            )
            entry["saved"] = format_bytes(max(saved, 0))
        comparison.append(entry)
    return comparison


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    add_common_args(parser)
    parser.add_argument(
        "--sizes",
        type=int,
        nargs="+",
        default=PAYLOAD_SIZES,
        help="Payload sizes in bytes.",
    )
    parser.add_argument(
        "--iters",
        type=int,
        default=DEFAULT_ITERS,
        help="Measured iterations per configuration.",
    )
    args = parser.parse_args()

    ray.init()

    print("\n" + "=" * 78)
    print("Receiver peak GPU memory: staging copy versus direct receive")
    print("=" * 78)

    rows = []
    for size_bytes in args.sizes:
        for mode in MODES:
            mode_rows = run_mode(args.single_node, size_bytes, mode, args.iters)
            for row in mode_rows:
                row["requested_bytes"] = size_bytes
            rows += mode_rows

    print()
    print_table(
        rows,
        [
            "mode",
            "payload_bytes",
            "iteration",
            "baseline_bytes",
            "peak_bytes",
            "peak_over_baseline_bytes",
            "peak_reserved_bytes",
        ],
    )

    print("\nPeak over baseline, worst iteration per configuration")
    comparison = build_comparison(rows, args.sizes)
    print_table(
        comparison,
        [
            "payload",
            "staging_peak_over_baseline",
            "direct_peak_over_baseline",
            "saved",
            "staging_peak_reserved",
            "direct_peak_reserved",
        ],
    )

    metrics = []
    for size_bytes in args.sizes:
        for mode in MODES:
            subset = [
                row
                for row in rows
                if row["mode"] == mode and row["requested_bytes"] == size_bytes
            ]
            if subset:
                metrics.append(
                    perf_metric(
                        f"peak_memory-{mode}-{format_bytes(size_bytes)}",
                        float(max(row["peak_over_baseline_bytes"] for row in subset)),
                        metric_type="MEMORY",
                    )
                )

    write_csv(args.csv_out, rows)
    write_perf_metrics(metrics)


if __name__ == "__main__":
    main()
