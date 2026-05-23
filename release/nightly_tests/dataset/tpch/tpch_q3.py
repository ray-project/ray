import ray
from ray.data.aggregate import Sum
from ray.data.expressions import col
from common import parse_tpch_args, load_table, to_f64, run_tpch_benchmark


def main(args):
    def benchmark_fn():
        from datetime import datetime

        # Q3: Shipping Priority Query
        # Revenue for orders from a market segment before a date, shipped after that date.
        #
        # Equivalent SQL:
        #   SELECT l_orderkey,
        #          SUM(l_extendedprice * (1 - l_discount)) AS revenue,
        #          o_orderdate,
        #          o_shippriority
        #   FROM customer, orders, lineitem
        #   WHERE c_mktsegment = 'BUILDING'
        #     AND c_custkey = o_custkey
        #     AND l_orderkey = o_orderkey
        #     AND o_orderdate < DATE '1995-03-15'
        #     AND l_shipdate > DATE '1995-03-15'
        #   GROUP BY l_orderkey, o_orderdate, o_shippriority
        #   ORDER BY revenue DESC, o_orderdate;
        #
        # Note:
        # This implementation keeps a linear join path:
        # customer -> orders -> lineitem.

        # Load all required tables with early projection.
        customer = load_table("customer", args.sf).select_columns(
            ["c_custkey", "c_mktsegment"]
        )
        orders = load_table("orders", args.sf).select_columns(
            ["o_orderkey", "o_custkey", "o_orderdate", "o_shippriority"]
        )
        lineitem = load_table("lineitem", args.sf).select_columns(
            ["l_orderkey", "l_shipdate", "l_extendedprice", "l_discount"]
        )

        # Q3 parameters
        date = datetime(1995, 3, 15)
        segment = "BUILDING"

        # Filter customer by segment.
        customer_filtered = customer.filter(expr=col("c_mktsegment") == segment)
        customer_filtered = customer_filtered.select_columns(["c_custkey"])

        # Filter orders by date
        orders_filtered = orders.filter(expr=col("o_orderdate") < date)

        # Join customer with orders in a linear chain.
        orders_customer = customer_filtered.join(
            orders_filtered,
            join_type="inner",
            num_partitions=16,
            on=("c_custkey",),
            right_on=("o_custkey",),
        ).select_columns(["o_orderkey", "o_orderdate", "o_shippriority"])

        # Join with lineitem and filter by ship date
        lineitem_filtered = lineitem.filter(expr=col("l_shipdate") > date)
        ds = orders_customer.join(
            lineitem_filtered,
            join_type="inner",
            num_partitions=16,
            on=("o_orderkey",),
            right_on=("l_orderkey",),
        )

        # Calculate revenue
        ds = ds.with_column(
            "revenue",
            to_f64(col("l_extendedprice")) * (1 - to_f64(col("l_discount"))),
        )

        # Aggregate by order key, order date, and ship priority
        _ = (
            ds.groupby(["o_orderkey", "o_orderdate", "o_shippriority"])
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
