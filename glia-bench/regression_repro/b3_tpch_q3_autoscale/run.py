"""B3: V2 op-resource-reservation under multi-op CPU pressure.

Upstream tpch_q3_autoscaling pipeline (3-way join + aggregation +
sort) regresses 33% on glia/m6_fixed vs master. The autoscaling
variant differs from fixed_size by enabling V2 cluster-autoscaler
behavior; the fix-suspect commit is #62592 (autoscaler V2 was sending
get_allocated_resources() as explicit demand, pinning leased
resources).

WARNING: cluster-autoscaler effects don't reproduce on a single-node
devpod — there's no autoscaler to fix. What we *can* engage is the
adjacent regime: op_resource_reservation_enabled with multi-op DAG
under tight CPU budget. Master's master log shows
'[backpressured:tasks(ResourceBudget)]' and op_resource_reservation
is on. If glia's reservation logic redistributes budgets less
efficiently when many ops compete, this bench will surface that.

The TPC-H q3 logical plan is: customer × orders × lineitem with
filters, group-by orderkey/orderdate/shippriority, sort. Six joins
and an aggregate run concurrently (post-filter). We synthesize
TPC-H-shaped tables small enough to fit in 24 CPU + 8 GB obj store
but with enough block count that operator queues compete.

Validity: bench should show non-trivial Δ wall (>15-20%) between
glia and master, OR show that this bucket genuinely doesn't
reproduce on single-node. The latter is also a useful finding —
it tells us the autoscaler V2 fix matters at cluster scale only.
"""

from __future__ import annotations

import os
import sys

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from _common import run_bench, shutdown_ray  # noqa: E402

import ray  # noqa: E402


# Small but non-trivial — the goal is multi-op concurrency, not data scale.
N_CUSTOMER = 300_000      # 1.5M
N_ORDERS = 3_000_000       # 15M
N_LINEITEM = 12_000_000     # 60M
N_FILES_C = 8
N_FILES_O = 16
N_FILES_L = 24
N_PART_JOIN = 16
N_REPS = 3


def _make_tpch(out_dir: str) -> dict:
    paths = {
        "customer": f"{out_dir}/customer",
        "orders": f"{out_dir}/orders",
        "lineitem": f"{out_dir}/lineitem",
    }
    if all(os.path.isdir(p) and any(os.scandir(p)) for p in paths.values()):
        return paths

    rng = np.random.default_rng(0)

    # customer
    os.makedirs(paths["customer"], exist_ok=True)
    rows_per_file = N_CUSTOMER // N_FILES_C
    for i in range(N_FILES_C):
        offset = i * rows_per_file
        c = pa.table({
            "c_custkey": np.arange(offset, offset + rows_per_file, dtype=np.int64),
            "c_mktsegment": rng.choice(
                ["BUILDING", "AUTOMOBILE", "MACHINERY", "HOUSEHOLD", "FURNITURE"],
                rows_per_file,
            ),
        })
        pq.write_table(c, f"{paths['customer']}/part_{i:03d}.parquet")

    # orders
    os.makedirs(paths["orders"], exist_ok=True)
    rows_per_file = N_ORDERS // N_FILES_O
    for i in range(N_FILES_O):
        offset = i * rows_per_file
        o = pa.table({
            "o_orderkey": np.arange(offset, offset + rows_per_file, dtype=np.int64),
            "o_custkey": rng.integers(0, N_CUSTOMER, rows_per_file, dtype=np.int64),
            "o_orderdate": rng.integers(0, 365 * 7, rows_per_file, dtype=np.int32),
            "o_shippriority": rng.integers(0, 5, rows_per_file, dtype=np.int32),
        })
        pq.write_table(o, f"{paths['orders']}/part_{i:03d}.parquet")

    # lineitem
    os.makedirs(paths["lineitem"], exist_ok=True)
    rows_per_file = N_LINEITEM // N_FILES_L
    for i in range(N_FILES_L):
        offset = i * rows_per_file
        ln = pa.table({
            "l_orderkey": rng.integers(0, N_ORDERS, rows_per_file, dtype=np.int64),
            "l_extendedprice": rng.random(rows_per_file).astype(np.float64) * 100_000,
            "l_discount": (rng.random(rows_per_file) * 0.1).astype(np.float64),
            "l_shipdate": rng.integers(0, 365 * 7, rows_per_file, dtype=np.int32),
        })
        pq.write_table(ln, f"{paths['lineitem']}/part_{i:03d}.parquet")

    return paths


def main() -> None:
    paths = _make_tpch("/tmp/regression_repro_b3_tpch")

    def workload():
        # Filters drop most rows quickly so subsequent joins are sized
        # like q3 ratios.
        c = ray.data.read_parquet(paths["customer"])
        c = c.filter(lambda r: r["c_mktsegment"] == "BUILDING")

        o = ray.data.read_parquet(paths["orders"])
        o = o.filter(lambda r: r["o_orderdate"] < 365 * 3)

        ln = ray.data.read_parquet(paths["lineitem"])
        ln = ln.filter(lambda r: r["l_shipdate"] > 365 * 3)

        # 3-way join: customer ⋈ orders ⋈ lineitem
        co = c.join(o, "inner", N_PART_JOIN, on=("c_custkey",), right_on=("o_custkey",))
        col = co.join(
            ln, "inner", N_PART_JOIN,
            on=("o_orderkey",), right_on=("l_orderkey",),
        )

        # Aggregate: revenue = SUM(extendedprice * (1 - discount))
        col = col.add_column(
            "revenue",
            lambda b: b["l_extendedprice"] * (1.0 - b["l_discount"]),
        )

        agg = col.groupby(["o_orderkey", "o_orderdate", "o_shippriority"]).sum("revenue")
        for _ in agg.iter_internal_ref_bundles():
            pass
        return agg

    init_kwargs = dict(
        num_cpus=24,
        log_to_driver=False,
        logging_level="WARNING",
        include_dashboard=False,
        object_store_memory=4 * 1024**3,
    )
    run_bench(
        name="b3_tpch_q3_autoscale",
        init_kwargs=init_kwargs,
        workload=workload,
        n_reps=N_REPS,
        warmup=True,
    )
    shutdown_ray()


if __name__ == "__main__":
    main()
