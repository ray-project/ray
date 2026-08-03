import argparse
import functools
import uuid
from typing import Callable

from benchmark import Benchmark, collect_operator_metrics

import ray
from ray.data import SaveMode

# Add a random prefix to avoid conflicts between different runs.
WRITE_PATH = f"s3://ray-data-write-benchmark/{uuid.uuid4().hex}"
# Region of the ray-data-write-benchmark bucket. write_parquet resolves this
# automatically, but deltalake's Rust S3 client doesn't follow a region
# redirect -- it defaults to us-east-1 and fails outright ("Received redirect
# without LOCATION") against a bucket hosted elsewhere unless told explicitly.
WRITE_DELTA_STORAGE_OPTIONS = {"AWS_REGION": "us-west-2"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("path", type=str)
    parser.add_argument(
        "--format",
        choices=["image", "parquet", "tfrecords"],
        required=True,
    )
    parser.add_argument(
        "--memory",
        type=int,
        default=None,
        help="Logical memory in bytes to pass to the read.",
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
    consume_group.add_argument("--write-delta", action="store_true")

    # Modifiers for --write-delta (not alternative consume actions, so they
    # live outside the mutually-exclusive group above).
    parser.add_argument(
        "--write-delta-mode",
        choices=["append", "overwrite"],
        default="append",
        help="SaveMode to use with --write-delta.",
    )
    parser.add_argument(
        "--write-delta-partition-by",
        type=str,
        default=None,
        help="Column to Hive-partition the Delta table by, when using --write-delta.",
    )

    args = parser.parse_args()
    if not args.write_delta and (
        args.write_delta_mode != "append" or args.write_delta_partition_by
    ):
        parser.error(
            "--write-delta-mode/--write-delta-partition-by require --write-delta."
        )
    return args


def main(args):
    benchmark = Benchmark()

    def benchmark_fn():
        read_fn = get_read_fn(args)
        consume_fn = get_consume_fn(args)

        ds = read_fn(args.path)
        consume_fn(ds)

        # Report arguments for the benchmark, plus per-operator time / output bytes /
        # decode-USS (isolates the read from downstream consume; surfaces the decode
        # memory the object-store peak can't see). ``ds`` is still in scope here.
        return {**vars(args), **collect_operator_metrics(ds)}

    if args.write_delta and args.write_delta_mode == "overwrite":
        # Populate the table once first (same source/scale as the timed run
        # below) so "main" genuinely overwrites existing data instead of
        # creating an empty table -- an OVERWRITE against a not-yet-existing
        # table is just a create, which isn't the interesting case to time.
        benchmark.run_fn("setup_populate", benchmark_fn)

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

    return functools.partial(read_fn, memory=args.memory)


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

    elif args.write_delta:

        def consume_fn(ds):
            mode = (
                SaveMode.OVERWRITE
                if args.write_delta_mode == "overwrite"
                else SaveMode.APPEND
            )
            partition_by = (
                [args.write_delta_partition_by]
                if args.write_delta_partition_by
                else None
            )
            ds.write_delta(
                WRITE_PATH,
                mode=mode,
                partition_by=partition_by,
                storage_options=WRITE_DELTA_STORAGE_OPTIONS,
            )

    else:
        assert False, f"Invalid consume arguments: {args}"

    return consume_fn


if __name__ == "__main__":
    ray.init()
    args = parse_args()
    main(args)
