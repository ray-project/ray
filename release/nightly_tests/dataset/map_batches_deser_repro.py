#!/usr/bin/env python3
"""
Repro: Ray Data map_batches argument-deserialization slowdown.

Four scenarios against the same large (~hundreds of MB) pyarrow Table batches:

  A. Pre-put + tiny arg + ray.get inside __call__   -> baseline (fast deser)
  B. Bare ray.get of the same batch                 -> raw object-store cost
  C. read_parquet -> map_batches                    -> slow path (the regression)
  D. read_parquet().materialize() -> map_batches    -> still slow (proves not upstream)

The parquet must contain an integer column named ``id`` (scenario A indexes
into pre-put tables by ``batch['id'][0]``). The customer-supplied sample was
~5000 rows x ~617 columns, ~1.45 GB decoded; any parquet with at least one
``id`` column and rows >> batch_size should reproduce.

Usage:
  python repro.py --parquet PATH [--batch-size N] [--n-batches N]
"""
import argparse
import json
import time
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq
import ray


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--parquet", required=True, help="Path to a parquet file with an 'id' column."
    )
    p.add_argument("--batch-size", type=int, default=512)
    p.add_argument("--n-batches", type=int, default=20)
    return p.parse_args()


def snapshot(path):
    """Dump Ray timeline and collect MapWorker thread IDs that exist now.

    Ray master uses cat names like 'task::MapWorker(MapBatches(MinimalActor)).__init__';
    older Ray used '_MapWorker'. Match both.
    """
    ray.timeline(filename=path)
    with open(path) as f:
        tl = json.load(f)
    tids = {
        e["tid"]
        for e in tl
        if isinstance(e, dict)
        and e.get("name") == "__init__"
        and "MapWorker" in e.get("cat", "")
    }
    return tl, tids


def deser_stats_ms(tl, new_tids):
    """p25/p50/p90/max of task:deserialize_arguments (ms) for the new MapWorker actors."""
    durs = sorted(
        e["dur"] / 1000.0
        for e in tl
        if isinstance(e, dict)
        and e.get("tid") in new_tids
        and e.get("name") == "task:deserialize_arguments"
        and e.get("dur", 0) > 0
    )
    if not durs:
        return None
    n = len(durs)
    return {
        "n": n,
        "p25": durs[n // 4],
        "p50": durs[n // 2],
        "p90": durs[int(n * 0.9)],
        "max": durs[-1],
    }


def load_tables(args):
    if not Path(args.parquet).exists():
        raise SystemExit(f"Parquet not found: {args.parquet}")
    full = pq.read_table(args.parquet)
    n = min(args.n_batches, full.num_rows // args.batch_size)
    return [full.slice(i * args.batch_size, args.batch_size) for i in range(n)]


def scenario_A_preput_tinyarg(tables):
    table_refs = [ray.put(t) for t in tables]

    class PrePutActor:
        def __call__(self, batch):
            idx = int(batch["id"][0]) % len(table_refs)
            t = ray.get(table_refs[idx])
            return {"n": np.array([t.num_rows])}

    tl0, tids0 = snapshot("/tmp/tl_A_before.json")
    t0 = time.perf_counter()
    ray.data.range(len(tables)).map_batches(
        PrePutActor,
        batch_size=1,
        num_gpus=0,
        concurrency=2,
        batch_format="numpy",
    ).materialize()
    wall = time.perf_counter() - t0
    tl1, tids1 = snapshot("/tmp/tl_A_after.json")
    return wall, deser_stats_ms(tl1, tids1 - tids0)


def scenario_B_bare_rayget(tables):
    ref = ray.put(tables[0])
    times_ms = []
    for _ in range(10):
        t0 = time.perf_counter()
        ray.get(ref)
        times_ms.append((time.perf_counter() - t0) * 1000)
    times_ms.sort()
    n = len(times_ms)
    return None, {
        "n": n,
        "p25": times_ms[n // 4],
        "p50": times_ms[n // 2],
        "p90": times_ms[int(n * 0.9)],
        "max": times_ms[-1],
    }


class MinimalActor:
    def __call__(self, batch):
        return {"n": np.array([batch.num_rows])}


def scenario_C_readparquet_mapbatches(args):
    ds = ray.data.read_parquet(args.parquet).limit(args.n_batches * args.batch_size)
    tl0, tids0 = snapshot("/tmp/tl_C_before.json")
    t0 = time.perf_counter()
    ds.map_batches(
        MinimalActor,
        batch_size=args.batch_size,
        num_gpus=0,
        concurrency=2,
        batch_format="pyarrow",
        zero_copy_batch=True,
    ).materialize()
    wall = time.perf_counter() - t0
    tl1, tids1 = snapshot("/tmp/tl_C_after.json")
    return wall, deser_stats_ms(tl1, tids1 - tids0)


def scenario_D_prematerialized(args):
    ds_cached = (
        ray.data.read_parquet(args.parquet)
        .limit(args.n_batches * args.batch_size)
        .materialize()
    )
    tl0, tids0 = snapshot("/tmp/tl_D_before.json")
    t0 = time.perf_counter()
    ds_cached.map_batches(
        MinimalActor,
        batch_size=args.batch_size,
        num_gpus=0,
        concurrency=1,
        batch_format="pyarrow",
        zero_copy_batch=True,
    ).materialize()
    wall = time.perf_counter() - t0
    tl1, tids1 = snapshot("/tmp/tl_D_after.json")
    return wall, deser_stats_ms(tl1, tids1 - tids0)


def main():
    args = parse_args()
    ray.init(ignore_reinit_error=True)
    print(f"Ray version: {ray.__version__}")

    tables = load_tables(args)
    print(
        f"Loaded {len(tables)} batches x {args.batch_size} rows "
        f"({tables[0].nbytes / 1e6:.1f} MB each)\n"
    )

    results = {}
    print("[A] pre-put + tiny arg + ray.get inside ...")
    results["A_preput_tinyarg"] = scenario_A_preput_tinyarg(tables)
    print("[B] bare ray.get ...")
    results["B_bare_rayget"] = scenario_B_bare_rayget(tables)
    print("[C] read_parquet -> map_batches ...")
    results["C_readparquet"] = scenario_C_readparquet_mapbatches(args)
    print("[D] materialized -> map_batches ...")
    results["D_materialized"] = scenario_D_prematerialized(args)

    print("\n=== Results ===")
    print(
        f"{'scenario':<22}  {'wall (s)':>9}  "
        f"{'n':>4}  {'p25':>7}  {'p50':>7}  {'p90':>7}  {'max':>7}"
    )
    print("-" * 75)
    for name, (wall, stats) in results.items():
        wall_s = f"{wall:.2f}" if wall is not None else "-"
        if stats is None:
            print(
                f"{name:<22}  {wall_s:>9}  {'-':>4}  {'-':>7}  {'-':>7}  {'-':>7}  {'-':>7}"
            )
        else:
            # B is a single bare-ray.get number — fake it into stats shape
            print(
                f"{name:<22}  {wall_s:>9}  {stats['n']:>4}  "
                f"{stats['p25']:>7.1f}  {stats['p50']:>7.1f}  "
                f"{stats['p90']:>7.1f}  {stats['max']:>7.1f}"
            )

    slow = results["C_readparquet"][1]
    fast = results["B_bare_rayget"][1]
    if slow and fast:
        print(
            f"\nmap_batches deser (p50) vs bare ray.get: "
            f"{slow['p50'] / fast['p50']:.1f}x slower"
        )
        print(
            f"map_batches deser (p90) vs bare ray.get: "
            f"{slow['p90'] / fast['p50']:.1f}x slower"
        )


if __name__ == "__main__":
    main()
