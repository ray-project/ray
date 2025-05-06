import io

import boto3
import numpy as np
from PIL import Image

import ray
from ray.data import ActorPoolStrategy
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
    # Assuming there are 80 CPUs and 4 in-flight tasks per actor, we need at least 320
    # partitions to utilize all CPUs.
    # TODO: This is a temporary workaround. We need to improve the default partitioning.
    metadata = metadata.repartition(320)

    class LoadImage:
        def __init__(self):
            self._client = boto3.client("s3")

        def __call__(self, row):
            data = io.BytesIO()
            self._client.download_fileobj(BUCKET, row["key"], data)
            image = Image.open(data).convert("RGB")
            return {"image": np.array(image)}

    ds = metadata.map(LoadImage, compute=ActorPoolStrategy(min_size=1))
    for _ in ds.iter_internal_ref_bundles():
        pass


if __name__ == "__main__":
    main()
