import argparse
from typing import Callable

import ray

from benchmark import Benchmark


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "api",
        choices=["iter_batches", "to_tf", "iter_torch_batches"],
    )
    parser.add_argument(
        "--batch-format",
        choices=["numpy", "pandas", "pyarrow"],
    )
    return parser.parse_args()


def main(args):
    benchmark = Benchmark("iter")

    ds = ray.data.read_parquet("s3://anyscale-imagenet/parquet/")
    iterable = get_iterable(args, ds)

    benchmark.run_iterate_ds(str(vars(args)), iterable)
    benchmark.write_result()


def get_iterable(
    args: argparse.Namespace, ds: ray.data.Dataset
) -> Callable[[ray.data.Dataset], None]:
    if args.api == "iter_batches":
        return ds.iter_batches(batch_format=args.batch_format)

    elif args.api == "to_tf":
        return ds.to_tf()

    elif args.api == "iter_torch_batches":
        return ds.iter_torch_batches()

    else:
        assert False, f"Invalid API argument: {args}"


if __name__ == "__main__":
    args = parse_args()
    main(args)
