import ray
from ray.data.aggregate import Max, Sum
from ray.data.expressions import col
from common import parse_tpch_args, load_table, to_f64, run_tpch_benchmark


def main(args):
    def benchmark_fn():
        from datetime import datetime

        # Q15: Top Supplier Query
        # Supplier(s) with the maximum total revenue in a 3-month shipping
        # window.
        #
        # Equivalent SQL:
        #   CREATE VIEW revenue0 (supplier_no, total_revenue) AS
        #     SELECT l_suppkey,
        #            SUM(l_extendedprice * (1 - l_discount))
        #     FROM lineitem
        #     WHERE l_shipdate >= DATE '1996-01-01'
        #       AND l_shipdate <  DATE '1996-04-01'
        #     GROUP BY l_suppkey;
        #
        #   SELECT s_suppkey, s_name, s_address, s_phone, total_revenue
        #   FROM supplier, revenue0
        #   WHERE s_suppkey = supplier_no
        #     AND total_revenue = (SELECT MAX(total_revenue) FROM revenue0)
        #   ORDER BY s_suppkey;
        #
        # Note:
        # Materialize the revenue view and derive the scalar max from it,
        # mirroring the Q2 min-cost decorrelation. Float equality is safe:
        # max_revenue comes from the same Sum output column, so comparing
        # the groupwise sums to it is bit-exact.

        # Q15 parameters
        start_date = datetime(1996, 1, 1)
        end_date = datetime(1996, 4, 1)

        supplier = load_table("supplier", args.sf).select_columns(
            ["s_suppkey", "s_name", "s_address", "s_phone"]
        )
        lineitem = load_table("lineitem", args.sf).select_columns(
            ["l_suppkey", "l_extendedprice", "l_discount", "l_shipdate"]
        )

        lineitem = lineitem.filter(
            expr=((col("l_shipdate") >= start_date) & (col("l_shipdate") < end_date))
        ).with_column(
            "rev", to_f64(col("l_extendedprice")) * (1 - to_f64(col("l_discount")))
        )

        revenue = (
            lineitem.groupby("l_suppkey")
            .aggregate(Sum(on="rev", alias_name="total_revenue"))
            .materialize()
        )

        max_revenue = revenue.aggregate(Max(on="total_revenue", alias_name="max_rev"))[
            "max_rev"
        ]

        top = revenue.filter(expr=col("total_revenue") == max_revenue)

        _ = (
            top.join(
                supplier,
                join_type="inner",
                num_partitions=16,
                on=("l_suppkey",),
                right_on=("s_suppkey",),
            )
            .select_columns(
                ["s_suppkey", "s_name", "s_address", "s_phone", "total_revenue"]
            )
            .sort(key="s_suppkey")
            .materialize()
        )

        return vars(args)

    run_tpch_benchmark("tpch_q15", benchmark_fn)


if __name__ == "__main__":
    ray.init()
    args = parse_tpch_args()
    main(args)
