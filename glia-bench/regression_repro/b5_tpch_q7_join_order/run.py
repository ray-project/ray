"""B5: TPC-H q7 plan reordering across logical refactor.

Upstream tpch_q7_autoscaling: glia 91513=147s, master 91797=129s (-12%).
Same op count (24) but DIFFERENT op order: master moves a ReadParquet
later in the plan vs glia. Suspect: logical-operator dataclass refactor
stack (#62321 / #62400 / #62568) plus input_dependencies refactor
(#63090 / #63107) changed how the optimizer iterates the DAG.

Q7 logical plan: 6-way join on customer/orders/lineitem/nation/region/
supplier with Filter('n_name != n_name_cust'), then HashAggregate by
(n_name_supp, n_name_cust, l_year), then Sort.

Bench: synthesize the 6 inputs and run a q7-shaped pipeline. Same
shape across both trees so any plan-reordering effect is the only
variable. Compare wall + per-op times.

Validity: bench should show wall delta in the same direction as
upstream (master faster). Op-graph order may differ between trees.
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


N_NATION = 25
N_REGION = 5
N_CUSTOMER = 300_000
N_SUPPLIER = 50_000
N_ORDERS = 3_000_000
N_LINEITEM = 12_000_000
N_FILES_C = 4
N_FILES_S = 2
N_FILES_O = 8
N_FILES_L = 24
N_PART_JOIN = 16
N_REPS = 3


def _make_q7_inputs(out_dir: str) -> dict:
    paths = {
        "nation": f"{out_dir}/nation",
        "region": f"{out_dir}/region",
        "customer": f"{out_dir}/customer",
        "supplier": f"{out_dir}/supplier",
        "orders": f"{out_dir}/orders",
        "lineitem": f"{out_dir}/lineitem",
    }
    if all(os.path.isdir(p) and any(os.scandir(p)) for p in paths.values()):
        return paths

    rng = np.random.default_rng(1)

    nation_names = [f"NATION_{i:02d}" for i in range(N_NATION)]
    region_names = [f"REGION_{i}" for i in range(N_REGION)]

    # nation (no n_regionkey — we don't use it; avoids dup-column on self-join)
    os.makedirs(paths["nation"], exist_ok=True)
    n = pa.table({
        "n_nationkey": np.arange(N_NATION, dtype=np.int64),
        "n_name": nation_names,
    })
    pq.write_table(n, f"{paths['nation']}/part_000.parquet")

    # region
    os.makedirs(paths["region"], exist_ok=True)
    r = pa.table({
        "r_regionkey": np.arange(N_REGION, dtype=np.int64),
        "r_name": region_names,
    })
    pq.write_table(r, f"{paths['region']}/part_000.parquet")

    # customer
    os.makedirs(paths["customer"], exist_ok=True)
    rows = N_CUSTOMER // N_FILES_C
    for i in range(N_FILES_C):
        offset = i * rows
        c = pa.table({
            "c_custkey": np.arange(offset, offset + rows, dtype=np.int64),
            "c_nationkey": rng.integers(0, N_NATION, rows, dtype=np.int64),
        })
        pq.write_table(c, f"{paths['customer']}/part_{i:03d}.parquet")

    # supplier
    os.makedirs(paths["supplier"], exist_ok=True)
    rows = N_SUPPLIER // N_FILES_S
    for i in range(N_FILES_S):
        offset = i * rows
        s = pa.table({
            "s_suppkey": np.arange(offset, offset + rows, dtype=np.int64),
            "s_nationkey": rng.integers(0, N_NATION, rows, dtype=np.int64),
        })
        pq.write_table(s, f"{paths['supplier']}/part_{i:03d}.parquet")

    # orders
    os.makedirs(paths["orders"], exist_ok=True)
    rows = N_ORDERS // N_FILES_O
    for i in range(N_FILES_O):
        offset = i * rows
        o = pa.table({
            "o_orderkey": np.arange(offset, offset + rows, dtype=np.int64),
            "o_custkey": rng.integers(0, N_CUSTOMER, rows, dtype=np.int64),
        })
        pq.write_table(o, f"{paths['orders']}/part_{i:03d}.parquet")

    # lineitem
    os.makedirs(paths["lineitem"], exist_ok=True)
    rows = N_LINEITEM // N_FILES_L
    for i in range(N_FILES_L):
        ln = pa.table({
            "l_orderkey": rng.integers(0, N_ORDERS, rows, dtype=np.int64),
            "l_suppkey": rng.integers(0, N_SUPPLIER, rows, dtype=np.int64),
            "l_extendedprice": (rng.random(rows) * 100_000).astype(np.float64),
            "l_discount": (rng.random(rows) * 0.1).astype(np.float64),
            "l_shipyear": rng.integers(1995, 1997, rows, dtype=np.int32),
        })
        pq.write_table(ln, f"{paths['lineitem']}/part_{i:03d}.parquet")

    return paths


def main() -> None:
    paths = _make_q7_inputs("/tmp/regression_repro_b5_q7")

    def workload():
        nation = ray.data.read_parquet(paths["nation"])
        region = ray.data.read_parquet(paths["region"])  # not used in q7-lite, kept for op-count parity
        customer = ray.data.read_parquet(paths["customer"])
        supplier = ray.data.read_parquet(paths["supplier"])
        orders = ray.data.read_parquet(paths["orders"])
        lineitem = ray.data.read_parquet(paths["lineitem"])

        # Build n_supp (supplier nation), n_cust (customer nation) via two
        # joins of nation against itself (renamed).
        n_supp = nation.rename_columns({"n_nationkey": "n_nationkey_supp", "n_name": "n_name_supp"})
        n_cust = nation.rename_columns({"n_nationkey": "n_nationkey_cust", "n_name": "n_name_cust"})

        sn = supplier.join(
            n_supp, "inner", N_PART_JOIN,
            on=("s_nationkey",), right_on=("n_nationkey_supp",),
        )
        cn = customer.join(
            n_cust, "inner", N_PART_JOIN,
            on=("c_nationkey",), right_on=("n_nationkey_cust",),
        )

        # lineitem ⋈ supplier_nation
        ls = lineitem.join(
            sn, "inner", N_PART_JOIN,
            on=("l_suppkey",), right_on=("s_suppkey",),
        )

        # ⋈ orders
        lso = ls.join(
            orders, "inner", N_PART_JOIN,
            on=("l_orderkey",), right_on=("o_orderkey",),
        )

        # ⋈ customer_nation
        joined = lso.join(
            cn, "inner", N_PART_JOIN,
            on=("o_custkey",), right_on=("c_custkey",),
        )

        # Filter cross-nation pairs and compute revenue
        joined = joined.filter(lambda r: r["n_name_supp"] != r["n_name_cust"])
        joined = joined.add_column(
            "volume",
            lambda b: b["l_extendedprice"] * (1.0 - b["l_discount"]),
        )

        # GroupBy (n_name_supp, n_name_cust, l_shipyear), sum volume
        agg = joined.groupby(["n_name_supp", "n_name_cust", "l_shipyear"]).sum("volume")
        for _ in agg.iter_internal_ref_bundles():
            pass
        return agg

    init_kwargs = dict(
        num_cpus=24,
        log_to_driver=False,
        logging_level="DEBUG",
        include_dashboard=False,
        object_store_memory=4 * 1024**3,
    )
    run_bench(
        name="b5_tpch_q7_join_order",
        init_kwargs=init_kwargs,
        workload=workload,
        n_reps=N_REPS,
        warmup=True,
    )
    shutdown_ray()


if __name__ == "__main__":
    main()
