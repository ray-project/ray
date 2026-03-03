import argparse
import functools
import uuid
from typing import Callable

from benchmark import Benchmark

import ray

# Add a random prefix to avoid conflicts between different runs.
WRITE_PATH = f"s3://ray-data-write-benchmark/{uuid.uuid4().hex}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("path", type=str)
    parser.add_argument(
        "--format",
        choices=["image", "parquet", "tfrecords"],
        required=True,
    )

    consume_group = parser.add_mutually_exclusive_group()
    consume_group.add_argument("--count", action="store_true")
    consume_group.add_argument("--iter-bundles", action="store_true")
    consume_group.add_argument("--iter-batches", choices=["numpy", "pandas", "pyarrow"])
    consume_group.add_argument("--iter-torch-batches", action="store_true")
    consume_group.add_argument(
        "--to-tf",
        nargs=2,
        metavar=("feature", "label"),
    )
    consume_group.add_argument("--write", action="store_true")

    return parser.parse_args()


def main(args):
    benchmark = Benchmark()

    def benchmark_fn():
        read_fn = get_read_fn(args)
        consume_fn = get_consume_fn(args)

        ds = read_fn(args.path)
        consume_fn(ds)

        # Report arguments for the benchmark.
        return vars(args)

    benchmark.run_fn("main", benchmark_fn)
    benchmark.write_result()


def get_read_fn(args: argparse.Namespace) -> Callable[[str], ray.data.Dataset]:
    if args.format == "image":
        # FIXME: We specify the mode as a workaround for
        # https://github.com/ray-project/ray/issues/49883.
        read_fn = functools.partial(ray.data.read_images, mode="RGB")
    elif args.format == "parquet":
        read_fn = ray.data.read_parquet
    elif args.format == "tfrecords":
        read_fn = ray.data.read_tfrecords
    else:
        assert False, f"Invalid data format argument: {args}"

    return read_fn


def get_consume_fn(args: argparse.Namespace) -> Callable[[ray.data.Dataset], None]:
    if args.count:

        def consume_fn(ds):
            ds.count()

    elif args.iter_bundles:

        def consume_fn(ds):
            for _ in ds.iter_internal_ref_bundles():
                pass

    elif args.iter_batches:

        def consume_fn(ds):
            for _ in ds.iter_batches(batch_format=args.iter_batches):
                pass

    elif args.iter_torch_batches:
        # In addition to consuming the data, we also want to test the performance of
        # moving data to GPU.
        def consume_fn(ds):
            for _ in ds.iter_torch_batches(device="cuda"):
                pass

    elif args.to_tf:

        def consume_fn(ds):
            feature, label = args.to_tf
            for _ in ds.to_tf(feature_columns=feature, label_columns=label):
                pass

    elif args.write:

        def consume_fn(ds):
            ds.write_parquet(WRITE_PATH)

    else:
        assert False, f"Invalid consume arguments: {args}"

    return consume_fn


if __name__ == "__main__":
    ray.init()
    args = parse_args()
    main(args)
