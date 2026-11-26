"""Compare the same Ray Data query with outer join simplification off and on."""

import argparse
import json
from statistics import median
from time import perf_counter

import numpy as np
import pyarrow as pa

import ray
from ray.data import DataContext
from ray.data.context import ShuffleStrategy
from ray.data.expressions import col


def make_inputs(rows, payload_bytes, seed):
    rng = np.random.default_rng(seed)
    left, right = [], []
    block_rows = 50_000
    for start in range(0, rows, block_rows):
        keys = np.arange(start, min(start + block_rows, rows), dtype=np.int64)
        payload = pa.Array.from_buffers(
            pa.binary(payload_bytes),
            len(keys),
            [None, pa.py_buffer(rng.bytes(len(keys) * payload_bytes))],
        )
        left.append(pa.table({"k": keys, "sample_id": keys + rows}))
        right.append(pa.table({"k": keys, "score": keys % 100, "payload": payload}))
    unmatched = np.arange(-rows // 20, 0, dtype=np.int64)
    left.append(pa.table({"k": unmatched, "sample_id": unmatched + rows}))
    return ray.data.from_arrow(left), ray.data.from_arrow(right)


def right_shuffle_inputs(stats):
    if "JoinShuffleMapRight" in (stats.base_name or ""):
        return {
            "rows": stats.extra_metrics["num_row_inputs_received"],
            "bytes": stats.extra_metrics["bytes_inputs_received"],
        }
    for parent in stats.parents:
        inputs = right_shuffle_inputs(parent)
        if inputs is not None:
            return inputs
    return None


def check_matches(output, rows, keep_pct, expected_count):
    """Validate the key multiset and projected values outside the timed runs."""
    seen = np.zeros(rows, dtype=bool)
    for batch in output.iter_batches(batch_format="pyarrow"):
        keys = batch["k"].to_numpy()
        assert np.all((0 <= keys) & (keys < rows))
        scores = batch["score"].to_numpy()
        np.testing.assert_array_equal(scores, keys % 100)
        np.testing.assert_array_equal(batch["sample_id"].to_numpy(), keys + rows)
        assert np.all(scores < keep_pct)
        seen[keys] = True
    assert np.count_nonzero(seen) == expected_count


def run_query(left, right, args, keep_pct, optimized, validate=False):
    # The Dataset keeps the DataContext captured at creation time.
    left.context.enable_outer_join_simplification = optimized
    start = perf_counter()
    output = (
        left.join(right, "left_outer", args.partitions, on=("k",))
        .filter(expr=col("score") < keep_pct)
        .materialize()
    )
    elapsed_ms = (perf_counter() - start) * 1_000
    expected_count = args.rows * keep_pct // 100
    assert output.count() == expected_count
    inputs = right_shuffle_inputs(output._raw_stats())
    expected_inputs = expected_count if optimized else args.rows
    assert inputs is not None and inputs["rows"] == expected_inputs, inputs
    if validate:
        check_matches(output, args.rows, keep_pct, expected_count)
    return {"elapsed_ms": round(elapsed_ms, 3), "right_shuffle": inputs}


def paired_summary(pairs, seed):
    baseline = [pair[False]["elapsed_ms"] for pair in pairs]
    rewrite = [pair[True]["elapsed_ms"] for pair in pairs]
    ratios = np.array(rewrite) / np.array(baseline)
    # Resample whole pairs to retain the matching between adjacent runs.
    bootstrap = np.random.default_rng(seed).choice(
        ratios, size=(10_000, len(pairs)), replace=True
    )
    interval = np.quantile(np.median(bootstrap, axis=1), [0.025, 0.975])
    return {
        "baseline_median_ms": median(baseline),
        "rewrite_median_ms": median(rewrite),
        "paired_median_ratio": float(np.median(ratios)),
        "paired_ratio_95pct_bootstrap": interval.tolist(),
        "rewrite_wins": int(np.count_nonzero(ratios < 1)),
        "pairs": len(pairs),
        "right_shuffle_baseline": pairs[-1][False]["right_shuffle"],
        "right_shuffle_rewrite": pairs[-1][True]["right_shuffle"],
    }


def main(args):
    ray.init(
        num_cpus=args.cpus,
        include_dashboard=False,
        log_to_driver=False,
        object_store_memory=args.object_store_mib * (1 << 20),
    )
    try:
        ctx = DataContext.get_current()
        ctx.shuffle_strategy = ShuffleStrategy.SHUFFLE_V2
        ctx.use_disk_based_hash_shuffle = False
        ctx.enable_progress_bars = False
        left, right = make_inputs(args.rows, args.payload_bytes, args.seed)
        print("CONFIG", json.dumps(vars(args)), flush=True)

        keep_pcts = (1, 10, 50, 100)
        for keep_pct in keep_pcts:
            for optimized in (False, True):
                run_query(left, right, args, keep_pct, optimized, validate=True)

        pairs = {keep_pct: [] for keep_pct in keep_pcts}
        rng = np.random.default_rng(args.seed)
        orders = {
            keep_pct: rng.permutation([False, True] * (args.pairs // 2))
            for keep_pct in keep_pcts
        }
        for repetition in range(args.pairs):
            for keep_pct in rng.permutation(keep_pcts).tolist():
                first = bool(orders[keep_pct][repetition])
                pair = {
                    optimized: run_query(left, right, args, keep_pct, optimized)
                    for optimized in (first, not first)
                }
                pairs[keep_pct].append(pair)
                print("PAIR", keep_pct, json.dumps(pair), flush=True)

        for keep_pct, results in pairs.items():
            print(
                "RESULT",
                keep_pct,
                json.dumps(paired_summary(results, args.seed)),
                flush=True,
            )
    finally:
        ray.shutdown()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--payload-bytes", type=int, default=512)
    parser.add_argument("--pairs", type=int, default=12)
    parser.add_argument("--cpus", type=int, default=4)
    parser.add_argument("--partitions", type=int, default=8)
    parser.add_argument("--object-store-mib", type=int, default=2_048)
    parser.add_argument("--seed", type=int, default=542)
    options = parser.parse_args()
    if options.rows < 100 or options.rows % 100:
        parser.error("--rows must be a positive multiple of 100")
    if options.pairs < 2 or options.pairs % 2:
        parser.error("--pairs must be an even integer greater than or equal to 2")
    main(options)
