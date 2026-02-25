import ray
from ray.data.aggregate import Sum
from ray.data.expressions import col
from common import parse_tpch_args, load_table, to_f64, run_tpch_benchmark


def main(args):
    def benchmark_fn():
        from datetime import datetime

        # Load all required tables
        customer = load_table("customer", args.sf)
        orders = load_table("orders", args.sf)
        lineitem = load_table("lineitem", args.sf)

        # Q3 parameters
        date = datetime(1995, 3, 15)
        segment = "BUILDING"

        # Filter customer by segment
        customer_filtered = customer.filter(expr=col("c_mktsegment") == segment)

        # Filter orders by date
        orders_filtered = orders.filter(expr=col("o_orderdate") < date)

        # Join orders with customer
        orders_customer = orders_filtered.join(
            customer_filtered,
            join_type="inner",
            num_partitions=16,
            on=("o_custkey",),
            right_on=("c_custkey",),
        )

        # Join with lineitem and filter by ship date
        lineitem_filtered = lineitem.filter(expr=col("l_shipdate") > date)
        ds = lineitem_filtered.join(
            orders_customer,
            join_type="inner",
            num_partitions=16,
            on=("l_orderkey",),
            right_on=("o_orderkey",),
        )

        # Calculate revenue
        ds = ds.with_column(
            "revenue",
            to_f64(col("l_extendedprice")) * (1 - to_f64(col("l_discount"))),
        )

        # Aggregate by order key, order date, and ship priority
        _ = (
            ds.groupby(["l_orderkey", "o_orderdate", "o_shippriority"])
            .aggregate(Sum(on="revenue", alias_name="revenue"))
            .sort(key=["revenue", "o_orderdate"], descending=[True, False])
            .materialize()
        )

        # Report arguments for the benchmark.
        return vars(args)

    run_tpch_benchmark("tpch_q3", benchmark_fn)


if __name__ == "__main__":
    ray.init()
    args = parse_tpch_args()
    main(args)
