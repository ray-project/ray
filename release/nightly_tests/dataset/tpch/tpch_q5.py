import ray
from ray.data.aggregate import Sum
from ray.data.expressions import col
from common import parse_tpch_args, load_table, to_f64, run_tpch_benchmark


def main(args):
    def benchmark_fn():
        from datetime import datetime

        # Load all required tables
        region = load_table("region", args.sf)
        nation = load_table("nation", args.sf)
        supplier = load_table("supplier", args.sf)
        customer = load_table("customer", args.sf)
        orders = load_table("orders", args.sf)
        lineitem = load_table("lineitem", args.sf)

        # Q5 parameters
        date = datetime(1994, 1, 1)

        # Filter region by name
        region = region.filter(expr=col("r_name") == "ASIA")

        # Join region with nation
        ds = region.join(
            nation,
            join_type="inner",
            num_partitions=100,
            on=("r_regionkey",),
            right_on=("n_regionkey",),
        )

        # Join with supplier
        ds = ds.join(
            supplier,
            join_type="inner",
            num_partitions=100,
            on=("n_nationkey",),
            right_on=("s_nationkey",),
        )

        # Join with customer
        ds = ds.join(
            customer,
            join_type="inner",
            num_partitions=100,
            on=("s_nationkey",),
            right_on=("c_nationkey",),
        )

        # Join with orders and filter by date
        orders_filtered = orders.filter(
            expr=(
                (col("o_orderdate") >= date)
                & (col("o_orderdate") < datetime(date.year + 1, date.month, date.day))
            )
        )
        ds = ds.join(
            orders_filtered,
            join_type="inner",
            num_partitions=100,
            on=("c_custkey",),
            right_on=("o_custkey",),
        )

        # Join with lineitem on order key and supplier key
        ds = ds.join(
            lineitem,
            join_type="inner",
            num_partitions=100,
            on=["o_orderkey", "s_suppkey"],
            right_on=["l_orderkey", "l_suppkey"],
        )

        # Calculate revenue
        ds = ds.with_column(
            "revenue",
            to_f64(col("l_extendedprice")) * (1 - to_f64(col("l_discount"))),
        )

        # Aggregate by nation name
        _ = (
            ds.groupby("n_name")
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
