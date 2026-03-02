import argparse

import pyarrow as pa
import pyarrow.compute as pc
import ray
from ray.data.datatype import DataType
from ray.data.expressions import udf
from benchmark import Benchmark

# Define schemas for TPC-H tables
TABLE_COLUMNS = {
    "lineitem": {
        "column00": "l_orderkey",
        "column01": "l_partkey",
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
    },
    # customer, orders, nation use column0, column1, ... (no zero-padding)
    "customer": {
        "column0": "c_custkey",
        "column1": "c_name",
        "column2": "c_address",
        "column3": "c_nationkey",
        "column4": "c_phone",
        "column5": "c_acctbal",
        "column6": "c_mktsegment",
        "column7": "c_comment",
    },
    "orders": {
        "column0": "o_orderkey",
        "column1": "o_custkey",
        "column2": "o_orderstatus",
        "column3": "o_totalprice",
        "column4": "o_orderdate",
        "column5": "o_orderpriority",
        "column6": "o_clerk",
        "column7": "o_shippriority",
        "column8": "o_comment",
    },
    "nation": {
        "column0": "n_nationkey",
        "column1": "n_name",
        "column2": "n_regionkey",
        "column3": "n_comment",
    },
}


@udf(return_dtype=DataType.float64())
def to_f64(arr: pa.Array) -> pa.Array:
    """Cast any numeric type to float64."""
    return pc.cast(arr, pa.float64())


def parse_tpch_args(description: str = "TPC-H Benchmark") -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "--sf",
        choices=[1, 10, 100, 1000, 10000],
        type=int,
        default=1,
        help="Scale factor",
    )
    return parser.parse_args()


def load_table(
    table_name: str,
    scale_factor: int,
    base_uri: str = "s3://ray-benchmark-data/tpch/parquet",
) -> ray.data.Dataset:
    path = f"{base_uri}/sf{scale_factor}/{table_name}"
    ds = ray.data.read_parquet(path)

    if table_name in TABLE_COLUMNS:
        ds = ds.rename_columns(TABLE_COLUMNS[table_name])

    return ds


def run_tpch_benchmark(name: str, benchmark_fn):
    benchmark = Benchmark()
    benchmark.run_fn(name, benchmark_fn)
    benchmark.write_result()
