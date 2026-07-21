"""Read benchmark for ``ray.data.read_zarr``.

Reads a Zarr v2 store off cloud storage and fully consumes it so the shared
:class:`benchmark.Benchmark` harness records wall time and object-store
usage/spill for the release-test dashboard. The scaling dimension (fixed-size vs
autoscaling cluster) comes from the release-test matrix, not this script.

By default each row is one chunk (long-form). ``--align-axis-0`` switches to the
wide-form (``align_axis_0``) schema -- one row per axis-0 chunk, one column per
array; name the row-aligned arrays with ``--arrays``. ``--chunk-shapes`` re-tiles
the leading axes at read time and ``--overlap`` adds wide-form sliding-window
overlap.

Example::

    python read_zarr_benchmark.py \
        s3://anonymous@ray-example-data/zarr/umi-cup.zarr \
        --align-axis-0 --arrays data/camera0_rgb data/robot0_eef_pos \
        --chunk-shapes 16 --iter-bundles
"""

import argparse
import functools
from typing import Callable

from benchmark import Benchmark

import ray


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("path", type=str)
    parser.add_argument(
        "--align-axis-0",
        action="store_true",
        help="Align axis-0 chunks and name the row-aligned arrays with --arrays.",
    )
    parser.add_argument(
        "--arrays",
        type=str,
        nargs="+",
        default=None,
        help="array_paths subset to read. Omit to read every array in the store.",
    )
    parser.add_argument(
        "--chunk-shapes",
        type=int,
        nargs="+",
        default=None,
        help="Leading-axis chunk-shape prefix applied to every selected array.",
    )
    parser.add_argument(
        "--overlap",
        type=int,
        default=0,
        help="Wide-form sliding-window overlap (--align-axis-0 only).",
    )
    parser.add_argument(
        "--allow-full-metadata-scan",
        action="store_true",
        help="Recursively scan the store for arrays when it isn't consolidated.",
    )
    parser.add_argument(
        "--register-imagecodecs",
        action="store_true",
        help=(
            "Register the imagecodecs numcodecs on the driver and (via a "
            "worker_process_setup_hook) every Ray worker -- needed for stores "
            "whose chunks use imagecodecs codecs (e.g. UMI camera is JPEG-XL)."
        ),
    )
    parser.add_argument(
        "--memory",
        type=int,
        default=None,
        help="Logical memory in bytes to pass to the read.",
    )

    consume_group = parser.add_mutually_exclusive_group(required=True)
    consume_group.add_argument("--count", action="store_true")
    consume_group.add_argument("--iter-bundles", action="store_true")
    consume_group.add_argument("--iter-batches", choices=["numpy", "pandas", "pyarrow"])

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
    return functools.partial(
        ray.data.read_zarr,
        array_paths=args.arrays,
        chunk_shapes=args.chunk_shapes,
        align_axis_0=args.align_axis_0,
        overlap=args.overlap,
        allow_full_metadata_scan=args.allow_full_metadata_scan,
        memory=args.memory,
    )


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

    else:
        raise ValueError(f"Invalid consume arguments: {args}")

    return consume_fn


if __name__ == "__main__":
    args = parse_args()
    runtime_env = None
    if args.register_imagecodecs:
        # imagecodecs codecs (e.g. UMI camera arrays are JPEG-XL) must be
        # registered with numcodecs on BOTH the driver -- read_zarr opens array
        # metadata while planning -- and every read worker, which decodes chunks.
        import imagecodecs.numcodecs

        imagecodecs.numcodecs.register_codecs()  # driver
        # Workers register at process startup via a job-level hook: Ray
        # deserializes a read task's arguments (which carry the store's
        # compressor) before any per-task runtime_env setup runs.
        runtime_env = {
            "worker_process_setup_hook": "imagecodecs.numcodecs.register_codecs"
        }
    ray.init(runtime_env=runtime_env)
    main(args)
