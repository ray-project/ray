import os
import random
import shutil
import tempfile
from typing import List, Optional, Tuple

from PIL import Image

import ray
from ray.data.dataset import Dataset

from benchmark import Benchmark
import pandas as pd


def read_images(
    root: str,
    size: Optional[Tuple[int, int]] = None,
    mode: Optional[str] = None,
    write_to_object_store: bool = True,
) -> Dataset:

    ds = ray.data.read_images(paths=root, size=size, mode=mode)
    if not write_to_object_store:
        # Apply UDF to generate dummy 1-row output from images.
        # This is to avoid writing images data to object store.
        ds = ds.map_batches(lambda _: pd.DataFrame({"one": [1]}), batch_size=None)
    return ds


def generate_images(
    num_images: int, sizes: List[Tuple[int, int]], modes: List[str], formats: List[str]
) -> str:

    dimensions = []
    for mode in modes:
        if mode in ["1", "L", "P"]:
            dimension = 1
        elif mode in ["RGB", "YCbCr", "LAB", "HSV"]:
            dimension = 3
        elif mode in ["RGBA", "CMYK", "I", "F"]:
            dimension = 4
        else:
            raise ValueError(f"Found unknown image mode: {mode}.")
        dimensions.append(dimension)

    images_dir = tempfile.mkdtemp()

    for image_idx in range(num_images):
        size = random.choice(sizes)
        file_format = random.choice(formats)
        mode_idx = random.randrange(len(modes))
        mode = modes[mode_idx]
        dimension = dimensions[mode_idx]

        width, height = size
        file_name = f"{images_dir}/{image_idx}.{file_format}"
        pixels_per_dimension = []
        for _ in range(dimension):
            pixels = os.urandom(width * height)
            pixels_per_dimension.append(pixels)

        image = Image.new(mode, size)
        if len(pixels_per_dimension) == 1:
            image.putdata(pixels_per_dimension[0])
        else:
            image.putdata(list(zip(*pixels_per_dimension)))
        image.save(file_name)

    return images_dir


def run_images_benchmark(benchmark: Benchmark):
    # Set global random seed.
    random.seed(42)

    test_input = [
        generate_images(100, [(256, 256)], ["RGB"], ["jpg"]),
        generate_images(100, [(2048, 2048)], ["RGB"], ["jpg"]),
        generate_images(
            1000, [(64, 64), (256, 256)], ["RGB", "L"], ["jpg", "jpeg", "png"]
        ),
    ]

    benchmark.run("images-100-256-rbg-jpg", read_images, root=test_input[0])
    benchmark.run("images-100-2048-rbg-jpg", read_images, root=test_input[1])
    benchmark.run(
        "images-100-2048-to-256-rbg-jpg",
        read_images,
        root=test_input[1],
        size=(256, 256),
    )
    benchmark.run(
        "images-1000-mix", read_images, root=test_input[2], size=(256, 256), mode="RGB"
    )

    for root in test_input:
        shutil.rmtree(root)

    # Run benchmark on 1G, 20G and 100G imagenet data.
    benchmark.run(
        "images-imagenet-1g",
        read_images,
        root="s3://air-example-data-2/1G-image-data-synthetic-raw",
    )

    benchmark.run(
        "images-imagenet-20g",
        read_images,
        root="s3://air-example-data-2/20G-image-data-synthetic-raw",
        # Not write to object store as the images size are too large.
        write_to_object_store=False,
    )

    benchmark.run(
        "images-imagenet-100g",
        read_images,
        root="s3://air-example-data-2/100G-image-data-synthetic-raw",
        # Not write to object store as the images size are too large.
        write_to_object_store=False,
    )


if __name__ == "__main__":
    ray.init()

    benchmark = Benchmark("read")

    run_images_benchmark(benchmark)

    benchmark.write_result()
