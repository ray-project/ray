import ray
from ray.data.aggregate import Sum
from ray.data.datatype import DataType
from ray.data.expressions import col
from common import parse_tpch_args, load_table, to_f64, run_tpch_benchmark


def main(args):
    def benchmark_fn():
        from datetime import datetime

        # Q14: Promotion Effect Query
        # Share of revenue coming from promotional parts in a 1-month window.
        #
        # Equivalent SQL:
        #   SELECT 100.00 *
        #          SUM(CASE WHEN p_type LIKE 'PROMO%'
        #                   THEN l_extendedprice * (1 - l_discount)
        #                   ELSE 0 END)
        #          / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
        #   FROM lineitem, part
        #   WHERE l_partkey = p_partkey
        #     AND l_shipdate >= DATE '1995-09-01'
        #     AND l_shipdate <  DATE '1995-10-01';
        #
        # Note:
        # CASE WHEN is expressed by multiplying revenue by a bool cast to
        # float64 (1.0 / 0.0). The /SUM(revenue) and 100* scaling match the
        # Q17 pattern: aggregate() returns a scalar dict, so the final
        # division is a trivial post-hoc step that does not affect timing.

        # Q14 parameters
        start_date = datetime(1995, 9, 1)
        end_date = datetime(1995, 10, 1)

        lineitem = load_table("lineitem", args.sf).select_columns(
            ["l_partkey", "l_extendedprice", "l_discount", "l_shipdate"]
        )
        part = load_table("part", args.sf).select_columns(["p_partkey", "p_type"])

        lineitem = lineitem.filter(
            expr=((col("l_shipdate") >= start_date) & (col("l_shipdate") < end_date))
        )

        joined = lineitem.join(
            part,
            join_type="inner",
            num_partitions=16,
            on=("l_partkey",),
            right_on=("p_partkey",),
        )

        joined = joined.with_column(
            "revenue",
            to_f64(col("l_extendedprice")) * (1 - to_f64(col("l_discount"))),
        ).with_column(
            "promo_revenue",
            col("revenue")
            * col("p_type").str.starts_with("PROMO").cast(DataType.float64()),
        )

        _ = joined.aggregate(
            Sum(on="promo_revenue", alias_name="sum_promo_revenue"),
            Sum(on="revenue", alias_name="sum_revenue"),
        )

        return vars(args)

    run_tpch_benchmark("tpch_q14", benchmark_fn)


if __name__ == "__main__":
    ray.init()
    args = parse_tpch_args()
    main(args)
