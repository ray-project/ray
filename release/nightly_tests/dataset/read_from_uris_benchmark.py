import argparse
import io

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
from PIL import Image

import ray
from ray.data.expressions import col, download
from benchmark import Benchmark, BenchmarkMetric

BUCKET = "anyscale-imagenet"
# This Parquet file contains the keys of images in the 'anyscale-imagenet' bucket.
METADATA_PATH = "s3://anyscale-imagenet/metadata.parquet"

NUM_DROPPED_ROWS_METRIC = "num_dropped_rows"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--sf",
        type=int,
        default=1,
        help="Scale factor. Reads the image URIs this many times.",
    )
    return parser.parse_args()


def main(args: argparse.Namespace):
    # A transient S3 error that outlives the download's own retries raises a
    # DownloadError. Retry the task a couple of times before giving up, so a
    # short S3 slowdown does not fail a long benchmark run.
    ctx = ray.data.DataContext.get_current()
    ctx.retried_map_errors = ["DownloadError"]
    ctx.max_map_retries = 2

    # Count the input rows outside the timed window. The count is served from
    # the Parquet footers, but it is still a separate dataset execution with
    # its own scheduling; keeping it out of ``run_fn`` keeps the ``time``
    # metric and the per-dataset scheduling stats comparable with history.
    num_input_rows = read_metadata(args.sf).count()

    benchmark = Benchmark()
    benchmark.run_fn("main", lambda: benchmark_fn(args.sf, num_input_rows))
    benchmark.write_result()

    # Every key in the metadata exists in the bucket, so a row without bytes
    # means a download failed and was dropped. Fail after the result is
    # written so the metrics are still reported.
    metrics = benchmark.result["main"]
    num_dropped_rows = metrics[NUM_DROPPED_ROWS_METRIC]
    if num_dropped_rows > 0:
        num_rows = metrics[BenchmarkMetric.NUM_ROWS.value]
        raise RuntimeError(
            f"{num_dropped_rows} of {num_rows + num_dropped_rows} rows had no "
            "bytes after download; see the download warnings in the job log."
        )


def read_metadata(sf: int) -> ray.data.Dataset:
    """The image keys, repeated ``sf`` times. Only Parquet metadata is read."""
    return ray.data.read_parquet([METADATA_PATH] * sf)


def benchmark_fn(sf: int, num_input_rows: int):
    # A fresh dataset, so no execution state from the count in ``main`` is
    # carried into the timed pipeline.
    metadata = read_metadata(sf)

    def decode_images(batch):
        images = []
        for b in batch["image_bytes"]:
            image = Image.open(io.BytesIO(b)).convert("RGB")
            images.append(np.array(image))
        del batch["image_bytes"]
        batch["image"] = np.array(images, dtype=object)
        return batch

    def convert_key(table):
        col = table["key"]
        t = col.type
        new_col = pc.binary_join_element_wise(
            pa.scalar("s3://" + BUCKET, type=t), col, pa.scalar("/", type=t)
        )
        return table.set_column(table.schema.get_field_index("key"), "key", new_col)

    ds = metadata.map_batches(convert_key, batch_format="pyarrow")
    ds = ds.with_column("image_bytes", download("key"))
    # A failed download yields null bytes. Drop those rows instead of letting
    # PIL fail on an empty buffer; the dropped count is reported and checked.
    ds = ds.filter(expr=col("image_bytes").is_not_null())
    ds = ds.map_batches(decode_images)

    num_output_rows = 0
    for bundle in ds.iter_internal_ref_bundles():
        num_output_rows += bundle.num_rows() or 0

    return {
        BenchmarkMetric.NUM_ROWS: num_output_rows,
        NUM_DROPPED_ROWS_METRIC: num_input_rows - num_output_rows,
    }


if __name__ == "__main__":
    main(parse_args())
