import ray
from ray.data.aggregate import Mean, Sum
from ray.data.expressions import col
from common import load_table, parse_tpch_args, run_tpch_benchmark, to_f64


def main(args):
    def benchmark_fn():
        # Q17: Small-Quantity-Order Revenue Query
        # Determine how much average yearly revenue would be lost if orders
        # were no longer filled for small quantities of certain parts.
        #
        # Equivalent SQL:
        #   SELECT SUM(l_extendedprice) / 7.0 AS avg_yearly
        #   FROM lineitem, part
        #   WHERE p_partkey = l_partkey
        #     AND p_brand = 'Brand#23'
        #     AND p_container = 'MED BOX'
        #     AND l_quantity < (
        #         SELECT 0.2 * AVG(l_quantity)
        #         FROM lineitem
        #         WHERE l_partkey = p_partkey
        #     )
        #
        # Note:
        # The correlated subquery is decorrelated by joining lineitem with
        # the filtered parts first, materializing the small result, then
        # computing AVG(l_quantity) per partkey from that intermediate.
        # This avoids a double S3 read of lineitem and scopes the groupby
        # to only the matching rows.

        # Load tables with early projection.
        part = load_table("part", args.sf).select_columns(
            ["p_partkey", "p_brand", "p_container"]
        )
        lineitem = load_table("lineitem", args.sf).select_columns(
            ["l_partkey", "l_quantity", "l_extendedprice"]
        )

        # Q17 parameters
        brand = "Brand#23"
        container = "MED BOX"

        # Filter part by brand and container.
        part_filtered = part.filter(
            expr=(col("p_brand") == brand) & (col("p_container") == container)
        ).select_columns(["p_partkey"])

        # Join lineitem with filtered parts first, then materialize the small
        # result for dual consumption (avg_qty groupby + filter pipeline).
        # This avoids a double S3 read of lineitem and reduces the groupby
        # from the full lineitem table to only matching rows.
        joined = (
            part_filtered.join(
                lineitem,
                join_type="inner",
                num_partitions=16,
                on=("p_partkey",),
                right_on=("l_partkey",),
            )
            .select_columns(["p_partkey", "l_quantity", "l_extendedprice"])
            .materialize()
        )

        # Decorrelate: compute average quantity per part (only matching parts).
        avg_qty = (
            joined.select_columns(["p_partkey", "l_quantity"])
            .groupby("p_partkey")
            .aggregate(Mean(on="l_quantity", alias_name="avg_quantity"))
        )

        # Join with average quantity per part.
        ds = joined.join(
            avg_qty,
            join_type="inner",
            num_partitions=16,
            on=("p_partkey",),
        ).select_columns(["l_quantity", "l_extendedprice", "avg_quantity"])

        # Filter: keep lineitems with quantity < 0.2 * avg_quantity.
        ds = ds.filter(
            expr=to_f64(col("l_quantity")) < 0.2 * to_f64(col("avg_quantity"))
        )

        # Aggregate: SUM(l_extendedprice) / 7.0
        # The / 7.0 is omitted since aggregate() returns a scalar dict and
        # the result is not consumed; this matches the Q6 benchmark pattern.
        ds = ds.with_column("l_extendedprice_f", to_f64(col("l_extendedprice")))
        _ = ds.aggregate(Sum(on="l_extendedprice_f", alias_name="avg_yearly"))

        # Report arguments for the benchmark.
        return vars(args)

    run_tpch_benchmark("tpch_q17", benchmark_fn)


if __name__ == "__main__":
    ray.init()
    args = parse_tpch_args()
    main(args)
