import ray
from ray.data.aggregate import Count
from ray.data.expressions import col
from common import load_table, parse_tpch_args, run_tpch_benchmark


def main(args):
    def benchmark_fn():
        from datetime import datetime

        # Q4: Order Priority Checking Query
        # Count orders in a quarter where at least one lineitem was received
        # after its committed date, grouped by order priority.
        #
        # Equivalent SQL:
        #   SELECT o_orderpriority,
        #          COUNT(*) AS order_count
        #   FROM orders
        #   WHERE o_orderdate >= DATE '1993-07-01'
        #     AND o_orderdate < DATE '1993-07-01' + INTERVAL '3' MONTH
        #     AND EXISTS (
        #         SELECT *
        #         FROM lineitem
        #         WHERE l_orderkey = o_orderkey
        #           AND l_commitdate < l_receiptdate
        #     )
        #   GROUP BY o_orderpriority
        #   ORDER BY o_orderpriority;
        #
        # Note:
        # The EXISTS subquery is implemented as a left_semi join, which
        # returns orders that have at least one matching late lineitem.

        # Load tables with early projection.
        orders = load_table("orders", args.sf).select_columns(
            ["o_orderkey", "o_orderpriority", "o_orderdate"]
        )
        lineitem = load_table("lineitem", args.sf).select_columns(
            ["l_orderkey", "l_commitdate", "l_receiptdate"]
        )

        # Q4 parameters
        date_start = datetime(1993, 7, 1)
        date_end = datetime(1993, 10, 1)

        # Filter orders by date range, then drop o_orderdate (no longer needed).
        orders_filtered = orders.filter(
            expr=(col("o_orderdate") >= date_start) & (col("o_orderdate") < date_end)
        ).select_columns(["o_orderkey", "o_orderpriority"])

        # Filter lineitem: commitdate < receiptdate (late deliveries).
        lineitem_late = lineitem.filter(
            expr=col("l_commitdate") < col("l_receiptdate")
        ).select_columns(["l_orderkey"])

        # Semi-join: keep only orders that have at least one late lineitem.
        ds = orders_filtered.join(
            lineitem_late,
            join_type="left_semi",
            num_partitions=16,
            on=("o_orderkey",),
            right_on=("l_orderkey",),
        )

        # Group by order priority and count.
        _ = (
            ds.groupby("o_orderpriority")
            .aggregate(Count(alias_name="order_count"))
            .sort(key="o_orderpriority")
            .materialize()
        )

        # Report arguments for the benchmark.
        return vars(args)

    run_tpch_benchmark("tpch_q4", benchmark_fn)


if __name__ == "__main__":
    ray.init()
    args = parse_tpch_args()
    main(args)
