import ray
from ray.data.aggregate import Sum
from ray.data.expressions import col
from common import parse_tpch_args, load_table, to_f64, run_tpch_benchmark


def main(args):
    def benchmark_fn():
        from datetime import datetime

        # Load all required tables with early column pruning to reduce
        # intermediate data size (projection pushes down to Parquet reader)
        # TODO: Remove manual projection once we support proper projection derivation
        region = load_table("region", args.sf).select_columns(["r_regionkey", "r_name"])
        nation = load_table("nation", args.sf).select_columns(
            ["n_nationkey", "n_name", "n_regionkey"]
        )
        supplier = load_table("supplier", args.sf).select_columns(
            ["s_suppkey", "s_nationkey"]
        )
        customer = load_table("customer", args.sf).select_columns(
            ["c_custkey", "c_nationkey"]
        )
        orders = load_table("orders", args.sf).select_columns(
            ["o_orderkey", "o_custkey", "o_orderdate"]
        )
        lineitem = load_table("lineitem", args.sf).select_columns(
            ["l_orderkey", "l_suppkey", "l_extendedprice", "l_discount"]
        )

        # Q5 parameters
        date = datetime(1994, 1, 1)

        # Filter region by name
        region_filtered = region.filter(expr=col("r_name") == "ASIA")

        # Join region with nation (Ray join) to restrict to the target region
        nation_region = region_filtered.join(
            nation,
            num_partitions=16,  # Empirical value to balance parallelism and shuffle overhead
            join_type="inner",
            on=("r_regionkey",),
            right_on=("n_regionkey",),
        )

        # Keep only needed nation columns; avoid pre-join rename to prevent
        # projection pushdown issues with Ray join
        asia_for_supplier = nation_region.select_columns(["n_nationkey", "n_name"])
        asia_for_customer = nation_region.select_columns(["n_nationkey", "n_name"])

        # supplier ⋈ asia_for_supplier (Ray join), get supplier nations
        supplier_nation = supplier.join(
            asia_for_supplier,
            num_partitions=16,
            join_type="inner",
            on=("s_nationkey",),
            right_on=("n_nationkey",),
        )
        supplier_nation = (
            supplier_nation.rename_columns({"n_name": "n_name_supp"})
            .select_columns(["s_suppkey", "s_nationkey", "n_name_supp"])
        )

        # customer ⋈ asia_for_customer (Ray join), get customer nations
        customer_nation = customer.join(
            asia_for_customer,
            num_partitions=16,
            join_type="inner",
            on=("c_nationkey",),
            right_on=("n_nationkey",),
        )
        customer_nation = (
            customer_nation.rename_columns({"n_name": "n_name_cust"})
            .select_columns(["c_custkey", "c_nationkey", "n_name_cust"])
        )

        orders_filtered = orders.filter(
            expr=(
                (col("o_orderdate") >= date)
                & (col("o_orderdate") < datetime(date.year + 1, date.month, date.day))
            )
        )
        orders_customer = orders_filtered.join(
            customer_nation,
            num_partitions=16,
            join_type="inner",
            on=("o_custkey",),
            right_on=("c_custkey",),
        )

        lineitem_orders = lineitem.join(
            orders_customer,
            num_partitions=16,
            join_type="inner",
            on=("l_orderkey",),
            right_on=("o_orderkey",),
        )

        # lineitem_orders ⋈ supplier_nation (Ray join)
        ds = lineitem_orders.join(
            supplier_nation,
            num_partitions=16,
            join_type="inner",
            on=("l_suppkey",),
            right_on=("s_suppkey",),
        )

        # Keep only local suppliers: customer and supplier share the same nation
        ds = ds.filter(expr=col("c_nationkey") == col("s_nationkey"))

        # Calculate revenue
        ds = ds.with_column(
            "revenue",
            to_f64(col("l_extendedprice")) * (1 - to_f64(col("l_discount"))),
        )

        # Aggregate by nation name (supplier nation)
        _ = (
            ds.groupby("n_name_supp")
            .aggregate(Sum(on="revenue", alias_name="revenue"))
            .sort(key="revenue", descending=True)
            .materialize()
        )

        # Report arguments for the benchmark.
        return vars(args)

    run_tpch_benchmark("tpch_q5", benchmark_fn)


if __name__ == "__main__":
    ray.init()
    args = parse_tpch_args()
    main(args)
