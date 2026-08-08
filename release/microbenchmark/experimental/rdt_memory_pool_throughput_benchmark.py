"""Benchmark 3: transfer throughput with and without the RDT memory pool.

Without the memory pool, RDT sends one NIXL transfer descriptor per tensor, so a
weight split into 10,000 views becomes 10,000 RDMA reads. The bytes on the wire
do not change, but per-read overhead does, so effective throughput falls as the
view count grows.

The memory pool packs the tensors of a single ``ray.put`` into contiguous
pre-registered blocks, collapsing them into one descriptor per block. The
receiver then pulls the whole run in one read and slices it apart locally, so
throughput stays roughly flat in the view count. The cost is a device-to-device
copy on the sender, which shows up in the reported put time.

Arms:

no_pool - weights pre-registered, no pool. One descriptor per view. Registration
is amortized in both arms so this measures descriptor count, not registration.

pool - a pool sized to the payload, weights deliberately not pre-registered. The
pool is only eligible for tensors that carry no NIXL registration, so
pre-registering the weights here would silently disable it.

The receiver uses the plain ``ray.get`` path and skips the copy into its own
weights, so the reported time is transfer only.

Usage:
    python rdt_memory_pool_throughput_benchmark.py                 # cross-node
    python rdt_memory_pool_throughput_benchmark.py --single-node    # local check
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
    summarize,
    write_csv,
    write_perf_metrics,
)

import ray

PAYLOAD_SIZES = [1 * GIB, 10 * GIB]
VIEW_COUNTS = [1, 100, 10000]
DEFAULT_ITERS = 5
MODES = ["no_pool", "pool"]
# Slack over the payload so packing alignment and any transient fragmentation
# cannot exhaust the arena mid-sweep.
POOL_SLACK_BYTES = 64 * MIB


def run_config(single_node, size_bytes, num_views, mode, iters):
    """Time transfers for one payload size, view count and pool mode."""
    use_pool = mode == "pool"
    trainer, generator = make_actors(single_node)
    trainer_setup = trainer.setup.remote(
        size_bytes,
        num_views,
        pre_register=not use_pool,
        pool_bytes=(size_bytes + POOL_SLACK_BYTES) if use_pool else None,
    )
    generator_setup = generator.setup.remote(size_bytes, num_views)
    trainer_info, generator_info = ray.get([trainer_setup, generator_setup])
    label = f"{mode}, {format_bytes(size_bytes)}, {num_views} views"
    describe_setup(f"trainer ({label})", trainer_info)
    describe_setup(f"generator ({label})", generator_info)
    assert_gpu_headroom(
        generator_info, trainer_info["payload_bytes"], f"receiver at {label}"
    )

    # Transfer only: stay on the plain ray.get path and skip the copy into the
    # receiver's own weights, which is not what this benchmark is comparing.
    def receive(object_ref):
        return ray.get(
            generator.sync_weights.remote([object_ref], False, copy_into_weights=False)
        )

    ref = ray.get(trainer.put_views.remote())
    receive(ref)
    del ref
    assert ray.get(
        trainer.wait_pool_drained.remote()
    ), "pool did not drain after warmup"

    rows = []
    for iteration in range(1, iters + 1):
        ref = ray.get(trainer.put_views.remote())
        put_seconds = ray.get(trainer.last_put_seconds.remote())
        num_descs, num_blocks = ray.get(
            [
                trainer.num_xfer_descs.remote(ref.hex()),
                trainer.num_pool_blocks.remote(ref.hex()),
            ]
        )
        result = receive(ref)
        del ref
        assert ray.get(
            trainer.wait_pool_drained.remote()
        ), f"pool did not drain after iteration {iteration}"

        payload_bytes = trainer_info["payload_bytes"]
        rows.append(
            {
                "mode": mode,
                "requested_bytes": size_bytes,
                "payload_bytes": payload_bytes,
                "num_views": num_views,
                "iteration": iteration,
                "num_xfer_descs": num_descs,
                "num_pool_blocks": num_blocks,
                "put_s": put_seconds,
                "get_s": result["seconds"],
                "get_gibps": payload_bytes / result["seconds"] / GIB,
            }
        )

    ray.kill(trainer)
    ray.kill(generator)
    return rows


def build_summary(rows, sizes, view_counts):
    """Median throughput per configuration, with the pool speedup alongside."""
    summary = []
    for size_bytes in sizes:
        for num_views in view_counts:
            entry = {
                "payload": format_bytes(size_bytes),
                "num_views": num_views,
            }
            medians = {}
            for mode in MODES:
                subset = [
                    row
                    for row in rows
                    if row["mode"] == mode
                    and row["requested_bytes"] == size_bytes
                    and row["num_views"] == num_views
                ]
                if not subset:
                    continue
                medians[mode] = summarize([row["get_gibps"] for row in subset])[
                    "median"
                ]
                entry[f"{mode}_gibps"] = medians[mode]
                entry[f"{mode}_descs"] = subset[0]["num_xfer_descs"]
                entry[f"{mode}_put_s"] = summarize([row["put_s"] for row in subset])[
                    "median"
                ]
            if len(medians) == len(MODES) and medians["no_pool"]:
                entry["speedup"] = medians["pool"] / medians["no_pool"]
            summary.append(entry)
    return summary


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
        "--views",
        type=int,
        nargs="+",
        default=VIEW_COUNTS,
        help="View counts to spread each payload over.",
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
    print("Transfer throughput: one descriptor per view versus pooled descriptors")
    print("=" * 78)

    rows = []
    for size_bytes in args.sizes:
        for num_views in args.views:
            for mode in MODES:
                rows += run_config(
                    args.single_node, size_bytes, num_views, mode, args.iters
                )

    print()
    print_table(
        rows,
        [
            "mode",
            "payload_bytes",
            "num_views",
            "iteration",
            "num_xfer_descs",
            "num_pool_blocks",
            "put_s",
            "get_s",
            "get_gibps",
        ],
    )

    print("\nMedian receive throughput (GiB/s)")
    summary = build_summary(rows, args.sizes, args.views)
    print_table(
        summary,
        [
            "payload",
            "num_views",
            "no_pool_descs",
            "no_pool_gibps",
            "pool_descs",
            "pool_gibps",
            "speedup",
            "no_pool_put_s",
            "pool_put_s",
        ],
    )

    metrics = []
    for entry in summary:
        for mode in MODES:
            key = f"{mode}_gibps"
            if key in entry:
                metrics.append(
                    perf_metric(
                        f"pool_throughput-{mode}-{entry['payload']}-"
                        f"{entry['num_views']}views",
                        entry[key],
                        metric_type="THROUGHPUT",
                    )
                )

    write_csv(args.csv_out, rows)
    write_perf_metrics(metrics)


if __name__ == "__main__":
    main()
