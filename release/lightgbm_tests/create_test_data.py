import argparse
import numpy as np
import os

from xgboost_ray.tests.utils import create_parquet

if __name__ == "__main__":
    if "OMP_NUM_THREADS" in os.environ:
        del os.environ["OMP_NUM_THREADS"]

    parser = argparse.ArgumentParser(description="Create fake data.")
    parser.add_argument(
        "filename", type=str, default="/data/parted.parquet/", help="ray/dask"
    )
    parser.add_argument(
        "-r", "--num-rows", required=False, type=int, default=1e8, help="num rows"
    )
    parser.add_argument(
        "-p",
        "--num-partitions",
        required=False,
        type=int,
        default=100,
        help="num partitions",
    )
    parser.add_argument(
        "-c",
        "--num-cols",
        required=False,
        type=int,
        default=4,
        help="num columns (features)",
    )
    parser.add_argument(
        "-C", "--num-classes", required=False, type=int, default=2, help="num classes"
    )
    parser.add_argument(
        "-s", "--seed", required=False, type=int, default=1234, help="random seed"
    )

    args = parser.parse_args()

    np.random.seed(args.seed)
    create_parquet(
        args.filename,
        num_rows=int(args.num_rows),
        num_partitions=int(args.num_partitions),
        num_features=int(args.num_cols),
        num_classes=int(args.num_classes),
    )
