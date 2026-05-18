import ray
from ray.data.aggregate import Sum
from ray.data.expressions import col
from common import parse_tpch_args, load_table, run_tpch_benchmark


def main(args):
    def benchmark_fn():
        # Q18: Large Volume Customer Query
        # Orders whose total lineitem quantity exceeds a threshold, with customer info.
        #
        # Equivalent SQL:
        #   SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice,
        #          SUM(l_quantity) AS sum_quantity
        #   FROM customer, orders, lineitem
        #   WHERE o_orderkey IN (
        #     SELECT l_orderkey
        #     FROM lineitem
        #     GROUP BY l_orderkey
        #     HAVING SUM(l_quantity) > 312
        #   )
        #     AND c_custkey = o_custkey
        #     AND o_orderkey = l_orderkey
        #   GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
        #   ORDER BY o_totalprice DESC, o_orderdate
        #   LIMIT 100;
        #
        # Note:
        # TPC-H parameter is in [312, 315]; this benchmark uses the fixed value 312.
        # It also materializes full sorted output (no LIMIT pushdown).

        # Load all required tables with early projection.
        customer = load_table("customer", args.sf).select_columns(
            ["c_custkey", "c_name"]
        )
        orders = load_table("orders", args.sf).select_columns(
            ["o_orderkey", "o_custkey", "o_orderdate", "o_totalprice"]
        )
        lineitem = load_table("lineitem", args.sf).select_columns(
            ["l_orderkey", "l_quantity"]
        )

        # Q18 parameters (spec: [312..315])
        quantity = 312

        # Calculate total quantity per order
        lineitem_quantity = lineitem.groupby("l_orderkey").aggregate(
            Sum(on="l_quantity", alias_name="total_quantity")
        )

        # Filter orders with total quantity > threshold
        large_orders = lineitem_quantity.filter(expr=col("total_quantity") > quantity)

        orders_customer = orders.join(
            customer.select_columns(["c_custkey", "c_name"]),
            join_type="inner",
            num_partitions=16,
            on=("o_custkey",),
            right_on=("c_custkey",),
        )
        orders_customer = orders_customer.select_columns(
            ["o_orderkey", "o_custkey", "o_orderdate", "o_totalprice", "c_name"]
        )

        # Join lineitem with large orders
        lineitem_large = lineitem.join(
            large_orders,
            join_type="inner",
            num_partitions=16,
            on=("l_orderkey",),
        )
        lineitem_large = lineitem_large.select_columns(
            ["l_orderkey", "l_quantity", "total_quantity"]
        )

        # Join lineitem_large with orders_customer
        ds = lineitem_large.join(
            orders_customer,
            join_type="inner",
            num_partitions=16,
            on=("l_orderkey",),
            right_on=("o_orderkey",),
        )

        # Aggregate by customer name, customer key, order key, and order date
        _ = (
            ds.groupby(
                ["c_name", "o_custkey", "l_orderkey", "o_orderdate", "o_totalprice"]
            )
            .aggregate(Sum(on="l_quantity", alias_name="sum_quantity"))
            .sort(key=["o_totalprice", "o_orderdate"], descending=[True, False])
            .materialize()
        )

        # Report arguments for the benchmark.
        return vars(args)

    run_tpch_benchmark("tpch_q18", benchmark_fn)


if __name__ == "__main__":
    ray.init()
    args = parse_tpch_args()
    main(args)
