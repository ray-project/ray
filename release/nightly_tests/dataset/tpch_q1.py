import argparse
from datetime import datetime, timedelta
from typing import Dict

import numpy as np
import ray

# TODO: We should make these public again.
from ray.data._internal.aggregate import Count, Sum, Mean

from benchmark import Benchmark


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TPCH Q1")
    parser.add_argument("--sf", choices=[1, 10, 100, 1000, 10000], type=int, default=1)
    return parser.parse_args()


def main(args):
    path = f"s3://ray-benchmark-data/tpch/parquet/sf{args.sf}/lineitem"
    benchmark = Benchmark("tpch-q1")

    def benchmark_fn():
        # The TPC-H queries are a widely used set of benchmarks to measure the
        # performance of data processing systems. See
        # https://examples.citusdata.com/tpch_queries.html.
        (
            ray.data.read_parquet(path)
            .filter(
                lambda row: row["column10"]  # l_shipdate
                <= datetime.strptime("1998-12-01", "%Y-%m-%d").date()
                - timedelta(days=90)
            )
            .map_batches(compute_disc_price)
            .map_batches(compute_charge)
            .groupby(["column08", "column09"])  # l_returnflag, l_linestatus
            .aggregate(
                Sum(on="column04", alias_name="sum_qty"),  # l_quantity
                Sum(on="column05", alias_name="sum_base_price"),  # l_extendedprice
                Sum(on="disc_price", alias_name="sum_disc_price"),
                Sum(on="charge", alias_name="sum_charge"),
                Mean(on="column04", alias_name="avg_qty"),  # l_quantity
                Mean(on="column05", alias_name="avg_price"),  # l_extendedprice
                Mean(on="column06", alias_name="avg_disc"),  # l_discount
                Count(),  # FIXME: No way to specify column name
            )
            .sort(["column08", "column09"])  # l_returnflag, l_linestatus
            .materialize()
        )

    benchmark.run_fn(str(vars(args)), benchmark_fn)
    benchmark.write_result()


def compute_disc_price(batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
    # l_extendedprice (column05) * (1 - l_discount (column06))
    batch["disc_price"] = batch["column05"] * (1 - batch["column06"])
    return batch


def compute_charge(batch):
    # l_extendedprice (column05) * (1 - l_discount (column06)) * (1 + l_tax (column07))
    batch["charge"] = (
        batch["column05"] * (1 - batch["column06"]) * (1 + batch["column07"])
    )
    return batch


if __name__ == "__main__":
    ray.init()
    args = parse_args()
    main(args)
