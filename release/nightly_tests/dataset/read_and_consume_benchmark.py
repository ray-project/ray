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

    parser.add_argument("--batch-size", type=int, default=None)
    parser.add_argument(
        "--batch-target-bytes",
        type=int,
        default=None,
        help=(
            "Target in-memory batch size in bytes for --iter-batches. When set, "
            "batch_size is computed at runtime from a sampled batch's bytes/row in "
            "the active representation, so the same target fills differently with "
            "and without --types-mapper. Overrides --batch-size."
        ),
    )
    parser.add_argument(
        "--types-mapper",
        choices=["on", "off"],
        default="off",
        help=(
            "Whether ArrowBlockAccessor.to_pandas uses the pd.ArrowDtype types_mapper "
            "(PR #63017). 'off' (default) matches master; 'on' injects the mapper to "
            "measure its iter_batches(pandas) cost."
        ),
    )

    return parser.parse_args()


def set_types_mapper(mode: str) -> None:
    """Release-test-only: deterministically set ArrowBlockAccessor.to_pandas to the
    mapper path ('on' = PR #63017's pd.ArrowDtype types_mapper) or the no-mapper path
    ('off' = pre-#63017), OVERRIDING whatever the checked-out code does. This lets one
    branch A/B the mapper regardless of whether the base has it (the fix has a
    revert/reapply history), with no product code change.

    Patches the driver-side class method; iter_batches formats batches in the driver
    threadpool, so this covers the consumed batches. The 'on' body mirrors #63017.
    """
    import pandas as pd
    import pyarrow

    from ray.data._internal import arrow_block
    from ray.data.context import DataContext
    from ray.data.util.data_batch_conversion import _cast_tensor_columns_to_ndarrays

    def to_pandas(self):
        ctx = DataContext.get_current()
        kwargs = {"ignore_metadata": ctx.pandas_block_ignore_metadata}
        if mode == "on":

            def _types_mapper(t):
                if isinstance(t, pyarrow.BaseExtensionType) or pyarrow.types.is_dictionary(t):
                    return None
                return pd.ArrowDtype(t)

            kwargs["types_mapper"] = _types_mapper

        df = self._table.to_pandas(**kwargs)
        if ctx.enable_tensor_extension_casting:
            df = _cast_tensor_columns_to_ndarrays(df, arrow_schema=self._table.schema)
        return df

    arrow_block.ArrowBlockAccessor.to_pandas = to_pandas


def compute_batch_size_for_target_bytes(args) -> int:
    """Translate --batch-target-bytes into a row batch_size by sampling one batch
    (untimed) and measuring its bytes/row in the active representation. Must run
    after set_types_mapper so the sample reflects mapper on vs off."""
    sample = get_read_fn(args)(args.path).take_batch(
        4096, batch_format=args.iter_batches
    )
    bytes_per_row = sample.memory_usage(deep=True).sum() / len(sample)
    batch_size = max(1, int(args.batch_target_bytes / bytes_per_row))
    print(
        f"[batch-target] {args.batch_target_bytes} bytes / "
        f"{bytes_per_row:.1f} bytes-per-row -> batch_size={batch_size} "
        f"(types_mapper={args.types_mapper})"
    )
    return batch_size


def main(args):
    benchmark = Benchmark()

    # Force the mapper on/off deterministically before any read so sampling and the
    # timed run use the same representation (independent of the base's to_pandas).
    set_types_mapper(args.types_mapper)

    # Translate a byte target into a row batch_size using the actual data (untimed).
    if args.batch_target_bytes and args.iter_batches:
        args.batch_size = compute_batch_size_for_target_bytes(args)

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
            kwargs = {} if args.batch_size is None else {"batch_size": args.batch_size}
            for _ in ds.iter_batches(batch_format=args.iter_batches, **kwargs):
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
