import ray
from ray.data.aggregate import Count, Mean, Sum
from ray.data.expressions import col
from common import parse_tpch_args, load_table, to_f64, run_tpch_benchmark


def main(args):
    def benchmark_fn():
        # The TPC-H queries are a widely used set of benchmarks to measure the
        # performance of data processing systems. See
        # https://www.tpc.org/tpch/
        from datetime import datetime

        ds = load_table("lineitem", args.sf)

        ds = ds.filter(expr=col("l_shipdate") <= datetime(1998, 9, 2))

        # Build float views + derived columns
        ds = (
            ds.with_column("l_quantity_f", to_f64(col("l_quantity")))
            .with_column("l_extendedprice_f", to_f64(col("l_extendedprice")))
            .with_column("l_discount_f", to_f64(col("l_discount")))
            .with_column("l_tax_f", to_f64(col("l_tax")))
            .with_column(
                "disc_price",
                col("l_extendedprice_f") * (1 - col("l_discount_f")),
            )
            .with_column("charge", col("disc_price") * (1 + col("l_tax_f")))
        )

        # Drop original DECIMALs
        ds = ds.select_columns(
            [
                "l_returnflag",
                "l_linestatus",
                "l_quantity_f",
                "l_extendedprice_f",
                "l_discount_f",
                "disc_price",
                "charge",
            ]
        )

        _ = (
            ds.groupby(["l_returnflag", "l_linestatus"])
            .aggregate(
                Sum(on="l_quantity_f", alias_name="sum_qty"),
                Sum(on="l_extendedprice_f", alias_name="sum_base_price"),
                Sum(on="disc_price", alias_name="sum_disc_price"),
                Sum(on="charge", alias_name="sum_charge"),
                Mean(on="l_quantity_f", alias_name="avg_qty"),
                Mean(on="l_extendedprice_f", alias_name="avg_price"),
                Mean(on="l_discount_f", alias_name="avg_disc"),
                Count(alias_name="count_order"),
            )
            .sort(key=["l_returnflag", "l_linestatus"])
            .select_columns(
                [
                    "l_returnflag",
                    "l_linestatus",
                    "sum_qty",
                    "sum_base_price",
                    "sum_disc_price",
                    "sum_charge",
                    "avg_qty",
                    "avg_price",
                    "avg_disc",
                    "count_order",
                ]
            )
            .materialize()
        )

        # Report arguments for the benchmark.
        return vars(args)

    run_tpch_benchmark("tpch_q1", benchmark_fn)


if __name__ == "__main__":
    ray.init()
    args = parse_tpch_args()
    main(args)
