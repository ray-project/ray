"""Benchmark 1: what RDT saves on NIXL memory registration.

Weight syncing in RL often sends many small tensors that are really views into
one larger weight. Registering each view separately with NIXL, which is the
common practice when driving NIXL directly, costs one registration per view.
RDT instead registers the base storage once and sends each view as an offset
into it, so the registration cost stops scaling with the view count.

The total payload is pinned at 1 GiB throughout; only the number of views
changes. Bytes registered and bytes transferred are therefore constant across
the sweep, which isolates the cost of registration *count* from registration
*volume*.

Two datasets are produced:

Dataset A - registration cost on its own, per view count: looping
``nixl_agent.register_memory`` over every view, versus the one
``register_nixl_memory`` call RDT needs, which registers the shared base storage
and covers every view at once.

Dataset B - an emulated end-to-end comparison. Weights are pre-registered on
both sides and the generator receives straight into them with
``set_target_for_ref``, so neither side pays registration per transfer. Ten
``ray.get`` iterations are measured per view count. For the last five, a legacy
total is emulated by adding the registration cost that the pre-RDT path would
have paid on each side.

Cold versus steady state: UCX keeps a registration cache, so registering a region
it has already seen is several times cheaper than registering a new one. A weight
sync loop registers the same buffers every step, so both regimes are reported.
Cold is the first registration of a region in the process, which a real job pays
once; warm is the steady-state cost it pays on every step after that. Dataset B
primes the cache before measuring, so its emulated legacy cost is steady state.

Usage:
    python rdt_registration_benchmark.py                    # cross-node
    python rdt_registration_benchmark.py --single-node      # local check
"""

import argparse

from rdt_weight_sync_common import (
    GIB,
    add_common_args,
    describe_setup,
    make_actors,
    perf_metric,
    print_table,
    summarize,
    write_csv,
    write_perf_metrics,
)

import ray

PAYLOAD_BYTES = 1 * GIB
VIEW_COUNTS = [1, 10, 100, 1000, 10000]
DATASET_B_ITERS = 10
DEFAULT_WARM_ITERS = 3
# Emulate the legacy total only for the trailing iterations, so the emulated
# series carries its own variance instead of being a fixed offset of the RDT one.
DATASET_B_EMULATED_TAIL = 5


def _cold_and_warm(method, warm_iters, **kwargs):
    """First call in the process, then the median of ``warm_iters`` repeats."""
    cold = ray.get(method.remote(**kwargs))
    warm = [ray.get(method.remote(**kwargs)) for _ in range(warm_iters)]
    return cold, summarize(warm)["median"]


def run_dataset_a(single_node, view_counts, warm_iters):
    """Measure registration cost alone: raw per-view NIXL versus RDT."""
    print("\n" + "=" * 78)
    print("Dataset A: registration cost for 1 GiB spread over N views")
    print("=" * 78)

    rows = []
    for num_views in view_counts:
        # Fresh actors per view count so the cold sample really is the first
        # registration of these regions in that process.
        trainer, generator = make_actors(single_node)
        info = ray.get(trainer.setup.remote(PAYLOAD_BYTES, num_views))
        describe_setup(f"trainer (views={num_views})", info)

        try:
            raw_cold, raw_warm = _cold_and_warm(
                trainer.time_raw_nixl_registration, warm_iters
            )
        except Exception as exc:
            print(f"  raw NIXL registration failed at {num_views} views: {exc}")
            raw_cold = raw_warm = None
        rdt_cold, rdt_warm = _cold_and_warm(trainer.time_rdt_registration, warm_iters)

        rows.append(
            {
                "dataset": "A",
                "num_views": num_views,
                "payload_bytes": info["payload_bytes"],
                "raw_nixl_cold_s": raw_cold,
                "raw_nixl_warm_s": raw_warm,
                "rdt_cold_s": rdt_cold,
                "rdt_warm_s": rdt_warm,
                "speedup_cold": (raw_cold / rdt_cold) if raw_cold else None,
                "speedup_warm": (raw_warm / rdt_warm) if raw_warm else None,
            }
        )
        ray.kill(trainer)
        ray.kill(generator)

    print()
    print_table(
        rows,
        [
            "num_views",
            "raw_nixl_cold_s",
            "rdt_cold_s",
            "speedup_cold",
            "raw_nixl_warm_s",
            "rdt_warm_s",
            "speedup_warm",
        ],
    )
    return rows


def run_dataset_b(single_node, view_counts):
    """Measure real RDT transfers, then emulate the legacy total."""
    print("\n" + "=" * 78)
    print("Dataset B: 1 GiB transfer with both sides pre-registered")
    print(
        f"  {DATASET_B_ITERS} iterations per view count; legacy total emulated "
        f"for the last {DATASET_B_EMULATED_TAIL}"
    )
    print("=" * 78)

    rows = []
    for num_views in view_counts:
        trainer, generator = make_actors(single_node)
        trainer_info, generator_info = ray.get(
            [
                trainer.setup.remote(PAYLOAD_BYTES, num_views, pre_register=True),
                generator.setup.remote(PAYLOAD_BYTES, num_views, pre_register=True),
            ]
        )
        describe_setup(f"trainer (views={num_views})", trainer_info)
        describe_setup(f"generator (views={num_views})", generator_info)
        ray.get([trainer.prepare_scratch.remote(), generator.prepare_scratch.remote()])

        # Warm up: the first transfer pays remote-agent setup and connection
        # establishment, which would otherwise land in iteration 1.
        ref = ray.get(trainer.put_views.remote())
        ray.get(generator.sync_weights.remote([ref], True))
        del ref

        # Prime the UCX registration cache on both sides so every emulated legacy
        # sample is a steady-state cost, matching what a loop pays per step.
        ray.get(
            [
                trainer.time_raw_nixl_registration.remote(use_scratch=True),
                generator.time_raw_nixl_registration.remote(use_scratch=True),
            ]
        )

        for iteration in range(1, DATASET_B_ITERS + 1):
            ref = ray.get(trainer.put_views.remote())
            result = ray.get(generator.sync_weights.remote([ref], True))
            del ref
            get_seconds = result["seconds"]

            row = {
                "dataset": "B",
                "num_views": num_views,
                "payload_bytes": trainer_info["payload_bytes"],
                "iteration": iteration,
                "rdt_get_s": get_seconds,
                "raw_reg_sender_s": None,
                "raw_reg_receiver_s": None,
                "legacy_emulated_s": None,
                "speedup": None,
            }

            if iteration > DATASET_B_ITERS - DATASET_B_EMULATED_TAIL:
                try:
                    sender_reg, receiver_reg = ray.get(
                        [
                            trainer.time_raw_nixl_registration.remote(use_scratch=True),
                            generator.time_raw_nixl_registration.remote(
                                use_scratch=True
                            ),
                        ]
                    )
                    legacy = get_seconds + sender_reg + receiver_reg
                    row.update(
                        {
                            "raw_reg_sender_s": sender_reg,
                            "raw_reg_receiver_s": receiver_reg,
                            "legacy_emulated_s": legacy,
                            "speedup": legacy / get_seconds,
                        }
                    )
                except Exception as exc:
                    print(
                        f"  raw registration failed at {num_views} views, "
                        f"iteration {iteration}: {exc}"
                    )

            rows.append(row)

        ray.kill(trainer)
        ray.kill(generator)

    print()
    print_table(
        rows,
        [
            "num_views",
            "iteration",
            "rdt_get_s",
            "raw_reg_sender_s",
            "raw_reg_receiver_s",
            "legacy_emulated_s",
            "speedup",
        ],
    )
    return rows


def summarize_dataset_b(rows, view_counts):
    """Print medians per view count for the RDT and emulated legacy series."""
    print("\nDataset B medians")
    summary = []
    for num_views in view_counts:
        subset = [row for row in rows if row["num_views"] == num_views]
        rdt_values = [row["rdt_get_s"] for row in subset]
        legacy_values = [
            row["legacy_emulated_s"]
            for row in subset
            if row["legacy_emulated_s"] is not None
        ]
        entry = {
            "num_views": num_views,
            "rdt_get_s_median": summarize(rdt_values)["median"],
        }
        if legacy_values:
            entry["legacy_emulated_s_median"] = summarize(legacy_values)["median"]
            entry["speedup"] = (
                entry["legacy_emulated_s_median"] / entry["rdt_get_s_median"]
            )
        summary.append(entry)
    print_table(
        summary,
        ["num_views", "rdt_get_s_median", "legacy_emulated_s_median", "speedup"],
    )
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    add_common_args(parser)
    parser.add_argument(
        "--views",
        type=int,
        nargs="+",
        default=VIEW_COUNTS,
        help="View counts to sweep (payload stays at 1 GiB).",
    )
    parser.add_argument(
        "--warm-iters",
        type=int,
        default=DEFAULT_WARM_ITERS,
        help="Repeat registrations per view count to get the steady-state cost.",
    )
    parser.add_argument(
        "--skip-dataset-a",
        action="store_true",
        help="Only run the emulated end-to-end dataset.",
    )
    parser.add_argument(
        "--skip-dataset-b",
        action="store_true",
        help="Only run the registration-cost dataset.",
    )
    args = parser.parse_args()

    ray.init()

    rows = []
    metrics = []

    if not args.skip_dataset_a:
        rows_a = run_dataset_a(args.single_node, args.views, args.warm_iters)
        rows += rows_a
        for row in rows_a:
            for series in ("raw_nixl_cold_s", "raw_nixl_warm_s", "rdt_cold_s"):
                if row[series] is None:
                    continue
                name = series[: -len("_s")].replace("_", "-")
                metrics.append(
                    perf_metric(
                        f"registration-{name}-{row['num_views']}views", row[series]
                    )
                )
            metrics.append(
                perf_metric(
                    f"registration-rdt-warm-{row['num_views']}views",
                    row["rdt_warm_s"],
                )
            )

    if not args.skip_dataset_b:
        rows_b = run_dataset_b(args.single_node, args.views)
        rows += rows_b
        for entry in summarize_dataset_b(rows_b, args.views):
            metrics.append(
                perf_metric(
                    f"transfer-rdt-{entry['num_views']}views",
                    entry["rdt_get_s_median"],
                )
            )
            if "legacy_emulated_s_median" in entry:
                metrics.append(
                    perf_metric(
                        f"transfer-legacy_emulated-{entry['num_views']}views",
                        entry["legacy_emulated_s_median"],
                    )
                )

    write_csv(args.csv_out, rows)
    write_perf_metrics(metrics)


if __name__ == "__main__":
    main()
