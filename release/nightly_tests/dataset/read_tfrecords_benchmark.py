import random
import shutil
import tempfile
from typing import List, Tuple

import ray
from ray.data.dataset import Dataset

from benchmark import Benchmark
from read_images_benchmark import generate_images
import pyarrow as pa
import numpy as np


def read_tfrecords(path: str) -> Dataset:
    return ray.data.read_tfrecords(paths=path)


def generate_tfrecords_from_images(
    num_images: int, sizes: List[Tuple[int, int]], modes: List[str], formats: List[str]
) -> str:
    images_dir = generate_images(num_images, sizes, modes, formats)
    try:
        ds = ray.data.read_images(images_dir)

        # Convert images from NumPy to bytes
        def images_to_bytes(batch):
            images_as_bytes = [image.tobytes() for image in batch.values()]
            return pa.table({"image": images_as_bytes})

        ds = ds.map_batches(images_to_bytes, batch_format="numpy")
        tfrecords_dir = tempfile.mkdtemp()
        ds.write_tfrecords(tfrecords_dir)
    finally:
        shutil.rmtree(images_dir)
    return tfrecords_dir


def generate_random_tfrecords(
    num_rows: int,
    num_int: int = 0,
    num_float: int = 0,
    num_bytes: int = 0,
    bytes_size: int = 0,
) -> str:
    def generate_features(batch):
        features = {"int_features": [], "float_features": [], "bytes_features": []}
        lower_bound = -(2**32)
        upper_bound = 2**32
        for _ in batch:
            if num_int > 0:
                int_features = [
                    random.randint(lower_bound, upper_bound) for _ in range(num_int)
                ]
                features["int_features"].append(int_features)
            if num_float > 0:
                float_features = [
                    random.uniform(lower_bound, upper_bound) for _ in range(num_float)
                ]
                features["float_features"].append(float_features)
            if num_bytes > 0:
                bytes_features = [np.random.bytes(bytes_size) for _ in range(num_bytes)]
                features["bytes_features"].append(bytes_features)
        features = {k: v for (k, v) in features.items() if len(v) > 0}
        return pa.table(features)

    ds = ray.data.range(num_rows).map_batches(generate_features)
    tfrecords_dir = tempfile.mkdtemp()
    ds.write_tfrecords(tfrecords_dir)
    return tfrecords_dir


def run_tfrecords_benchmark(benchmark: Benchmark):
    # Set global random seed.
    random.seed(42)

    test_input = [
        generate_tfrecords_from_images(100, [(256, 256)], ["RGB"], ["jpg"]),
        generate_tfrecords_from_images(100, [(2048, 2048)], ["RGB"], ["jpg"]),
        generate_tfrecords_from_images(
            1000, [(64, 64), (256, 256)], ["RGB"], ["jpg", "jpeg", "png"]
        ),
        generate_random_tfrecords(1024 * 1024 * 10, num_int=100),
        generate_random_tfrecords(1024 * 1024 * 10, num_float=100),
        generate_random_tfrecords(1024 * 1024, num_bytes=10, bytes_size=100),
    ]

    try:
        benchmark.run("tfrecords-images-100-256", read_tfrecords, path=test_input[0])
        benchmark.run("tfrecords-images-100-2048", read_tfrecords, path=test_input[1])
        benchmark.run("tfrecords-images-1000-mix", read_tfrecords, path=test_input[2])
        benchmark.run("tfrecords-random-int-1g", read_tfrecords, path=test_input[3])
        benchmark.run("tfrecords-random-float-1g", read_tfrecords, path=test_input[4])
        benchmark.run("tfrecords-random-bytes-1g", read_tfrecords, path=test_input[5])

    finally:
        for root in test_input:
            shutil.rmtree(root)


if __name__ == "__main__":
    ray.init()

    benchmark = Benchmark("read-tfrecords")

    run_tfrecords_benchmark(benchmark)

    benchmark.write_result()
