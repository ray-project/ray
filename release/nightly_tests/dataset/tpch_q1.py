import argparse

import numpy as np
from benchmark import Benchmark

import ray

# TODO: We should make these public again.
from ray.data.aggregate import Count, Mean, Sum
from ray.data import col
from ray.data.expressions import udf
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

        cutoff = np.datetime64("1998-09-02")  # date '1998-12-01' - 90 days
        ds = ray.data.read_parquet(path).filter(expr=col("column10") <= cutoff)

        # Build float views + derived columns
        ds = (
            ds.with_column("l_quantity_f", to_f64(col("column04")))
            .with_column("l_extendedprice_f", to_f64(col("column05")))
            .with_column("l_discount_f", to_f64(col("column06")))
            .with_column("l_tax_f", to_f64(col("column07")))
            .with_column(
                "disc_price",
                to_f64(col("column05")) * (1 - to_f64(col("column06"))),
            )
            .with_column("charge", col("disc_price") * (1 + to_f64(col("column07"))))
        )

        # Drop original DECIMALs
        ds = ds.select_columns(
            [
                "column08",  # l_returnflag
                "column09",  # l_linestatus
                "l_quantity_f",
                "l_extendedprice_f",
                "l_discount_f",
                "disc_price",
                "charge",
            ]
        )

        _ = (
            ds.groupby(["column08", "column09"])  # l_returnflag, l_linestatus
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
            .sort(key=["column08", "column09"])  # l_returnflag, l_linestatus
            .select_columns(
                [
                    "column08",  # l_returnflag
                    "column09",  # l_linestatus
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
