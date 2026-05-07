import sys, time
sys.path.insert(0, '/workspace/ray/glia-ray-fork/glia-bench/regression_repro/b5_tpch_q7_join_order')
from run import _make_q7_inputs, N_PART_JOIN

import ray
ray.init(num_cpus=24, log_to_driver=False, logging_level="WARNING", include_dashboard=False, object_store_memory=4*1024**3)

paths = _make_q7_inputs('/tmp/regression_repro_b5_q7')

t0 = time.time()
nation = ray.data.read_parquet(paths["nation"])
region = ray.data.read_parquet(paths["region"])
customer = ray.data.read_parquet(paths["customer"])
supplier = ray.data.read_parquet(paths["supplier"])
orders = ray.data.read_parquet(paths["orders"])
lineitem = ray.data.read_parquet(paths["lineitem"])

n_supp = nation.rename_columns({"n_nationkey": "n_nationkey_supp", "n_name": "n_name_supp"})
n_cust = nation.rename_columns({"n_nationkey": "n_nationkey_cust", "n_name": "n_name_cust"})

sn = supplier.join(n_supp, "inner", N_PART_JOIN, on=("s_nationkey",), right_on=("n_nationkey_supp",))
cn = customer.join(n_cust, "inner", N_PART_JOIN, on=("c_nationkey",), right_on=("n_nationkey_cust",))
ls = lineitem.join(sn, "inner", N_PART_JOIN, on=("l_suppkey",), right_on=("s_suppkey",))
lso = ls.join(orders, "inner", N_PART_JOIN, on=("l_orderkey",), right_on=("o_orderkey",))
joined = lso.join(cn, "inner", N_PART_JOIN, on=("o_custkey",), right_on=("c_custkey",))
joined = joined.filter(lambda r: r["n_name_supp"] != r["n_name_cust"])
joined = joined.add_column("volume", lambda b: b["l_extendedprice"] * (1.0 - b["l_discount"]))
agg = joined.groupby(["n_name_supp", "n_name_cust", "l_shipyear"]).sum("volume")

for _ in agg.iter_internal_ref_bundles(): pass
wall = time.time() - t0
print(f"\nWALL: {wall:.2f}s\n")
print(agg.stats())
