import io

import numpy as np
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
        batch["image"] = np.stack(images)
        return batch

    ds = metadata.with_column("image_bytes", download("key"))
    ds = ds.map_batches(decode_images)
    for _ in ds.iter_internal_ref_bundles():
        pass


if __name__ == "__main__":
    main()
