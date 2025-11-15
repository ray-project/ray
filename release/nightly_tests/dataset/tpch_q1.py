import argparse

from benchmark import Benchmark

import ray

# TODO: We should make these public again.
from ray.data.aggregate import Count, Mean, Sum
from ray.data.expressions import col, udf
from ray.data.datatype import DataType
import pyarrow as pa
import pyarrow.compute as pc


@udf(return_dtype=DataType.float64())
def to_f64(arr: pa.Array) -> pa.Array:
    """Cast any numeric type to float64."""
    return pc.cast(arr, pa.float64())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TPCH Q1")
    parser.add_argument("--sf", choices=[1, 10, 100, 1000, 10000], type=int, default=1)
    return parser.parse_args()


def main(args):
    path = f"s3://ray-benchmark-data/tpch/parquet/sf{args.sf}/lineitem"
    benchmark = Benchmark()

    def benchmark_fn():
        # The TPC-H queries are a widely used set of benchmarks to measure the
        # performance of data processing systems. See
        # https://examples.citusdata.com/tpch_queries.html.
        from datetime import datetime

        ds = (
            ray.data.read_parquet(path)
            .rename_columns(
                {
                    "column00": "l_orderkey",
                    "column02": "l_suppkey",
                    "column03": "l_linenumber",
                    "column04": "l_quantity",
                    "column05": "l_extendedprice",
                    "column06": "l_discount",
                    "column07": "l_tax",
                    "column08": "l_returnflag",
                    "column09": "l_linestatus",
                    "column10": "l_shipdate",
                    "column11": "l_commitdate",
                    "column12": "l_receiptdate",
                    "column13": "l_shipinstruct",
                    "column14": "l_shipmode",
                    "column15": "l_comment",
                }
            )
            .filter(expr=col("l_shipdate") <= datetime(1998, 9, 2))
        )

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

    benchmark.run_fn("main", benchmark_fn)
    benchmark.write_result()


if __name__ == "__main__":
    ray.init()
    args = parse_args()
    main(args)
