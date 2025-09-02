import io

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
from PIL import Image

import ray
from ray.data.expressions import download
from benchmark import Benchmark

BUCKET = "anyscale-imagenet"
# This Parquet file contains the keys of images in the 'anyscale-imagenet' bucket.
METADATA_PATH = "s3://anyscale-imagenet/metadata.parquet"


def main():
    benchmark = Benchmark()
    benchmark.run_fn("main", benchmark_fn)
    benchmark.write_result()


def benchmark_fn():
    metadata = ray.data.read_parquet(METADATA_PATH)

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
    ds = ds.map_batches(decode_images)
    for _ in ds.iter_internal_ref_bundles():
        pass


if __name__ == "__main__":
    main()
