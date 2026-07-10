import ray
from ray.data.aggregate import Sum
from ray.data.datatype import DataType
from ray.data.expressions import col
from common import parse_tpch_args, load_table, run_tpch_benchmark


def main(args):
    def benchmark_fn():
        from datetime import datetime

        # Q12: Shipping Modes and Order Priority Query
        # Counts of high- vs low-priority orders per ship mode for late
        # lineitems in a 1-year window.
        #
        # Equivalent SQL:
        #   SELECT l_shipmode,
        #          SUM(CASE WHEN o_orderpriority IN ('1-URGENT','2-HIGH')
        #                   THEN 1 ELSE 0 END) AS high_line_count,
        #          SUM(CASE WHEN o_orderpriority NOT IN ('1-URGENT','2-HIGH')
        #                   THEN 1 ELSE 0 END) AS low_line_count
        #   FROM orders, lineitem
        #   WHERE o_orderkey = l_orderkey
        #     AND l_shipmode IN ('MAIL','SHIP')
        #     AND l_commitdate < l_receiptdate
        #     AND l_shipdate < l_commitdate
        #     AND l_receiptdate >= DATE '1994-01-01'
        #     AND l_receiptdate <  DATE '1995-01-01'
        #   GROUP BY l_shipmode
        #   ORDER BY l_shipmode;
        #
        # Note:
        # CASE WHEN is expressed by casting the boolean predicate to int64
        # and summing, which is equivalent to the SUM(CASE ... THEN 1 ELSE 0)
        # shape.

        # Q12 parameters
        ship_modes = ["MAIL", "SHIP"]
        start_date = datetime(1994, 1, 1)
        end_date = datetime(1995, 1, 1)
        high_priorities = ["1-URGENT", "2-HIGH"]

        orders = load_table("orders", args.sf).select_columns(
            ["o_orderkey", "o_orderpriority"]
        )
        lineitem = load_table("lineitem", args.sf).select_columns(
            [
                "l_orderkey",
                "l_shipmode",
                "l_commitdate",
                "l_shipdate",
                "l_receiptdate",
            ]
        )

        # Filter lineitem: restricted shipmodes, late (receipt > commit),
        # on-time shipment (ship < commit), and receipt date in window.
        lineitem = lineitem.filter(
            expr=(
                col("l_shipmode").is_in(ship_modes)
                & (col("l_commitdate") < col("l_receiptdate"))
                & (col("l_shipdate") < col("l_commitdate"))
                & (col("l_receiptdate") >= start_date)
                & (col("l_receiptdate") < end_date)
            )
        ).select_columns(["l_orderkey", "l_shipmode"])

        joined = lineitem.join(
            orders,
            join_type="inner",
            num_partitions=16,
            on=("l_orderkey",),
            right_on=("o_orderkey",),
        )

        joined = joined.with_column(
            "high_line_count",
            col("o_orderpriority").is_in(high_priorities).cast(DataType.int64()),
        ).with_column(
            "low_line_count",
            col("o_orderpriority").not_in(high_priorities).cast(DataType.int64()),
        )

        _ = (
            joined.groupby("l_shipmode")
            .aggregate(
                Sum(on="high_line_count", alias_name="high_line_count"),
                Sum(on="low_line_count", alias_name="low_line_count"),
            )
            .sort(key="l_shipmode")
            .materialize()
        )

        return vars(args)

    run_tpch_benchmark("tpch_q12", benchmark_fn)


if __name__ == "__main__":
    ray.init()
    args = parse_tpch_args()
    main(args)
