import argparse
import os
import random
import shutil
import tempfile
from typing import List, Tuple

from PIL import Image

import ray

from benchmark import Benchmark


def parse_args():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--single-node",
        action="store_true",
        help="Run single-node read_images benchmark.",
    )
    group.add_argument(
        "--multi-node",
        action="store_true",
        help="Run multi-node read_images benchmark.",
    )
    return parser.parse_args()


def main(args):
    ray.init()

    benchmark = Benchmark("read-images")
    if args.single_node:
        run_images_benchmark_single_node(benchmark)
    elif args.multi_node:
        run_images_benchmark_multi_node(benchmark)

    benchmark.write_result()


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


def run_images_benchmark_single_node(benchmark: Benchmark):
    # Set global random seed.
    random.seed(42)

    test_input = [
        generate_images(100, [(256, 256)], ["RGB"], ["jpg"]),
        generate_images(100, [(2048, 2048)], ["RGB"], ["jpg"]),
        generate_images(
            1000, [(64, 64), (256, 256)], ["RGB", "L"], ["jpg", "jpeg", "png"]
        ),
    ]

    benchmark.run_materialize_ds(
        "images-100-256-rbg-jpg", ray.data.read_images, test_input[0]
    )
    benchmark.run_materialize_ds(
        "images-100-2048-rbg-jpg", ray.data.read_images, test_input[1]
    )
    benchmark.run_materialize_ds(
        "images-100-2048-to-256-rbg-jpg",
        ray.data.read_images,
        test_input[1],
        size=(256, 256),
    )
    benchmark.run_materialize_ds(
        "images-1000-mix",
        ray.data.read_images,
        test_input[2],
        size=(256, 256),
        mode="RGB",
    )

    for root in test_input:
        shutil.rmtree(root)

    # TODO(chengsu): run benchmark on 20G and 100G imagenet data in multi-nodes
    # cluster.
    benchmark.run_materialize_ds(
        "images-imagenet-1g",
        ray.data.read_images,
        "s3://air-example-data-2/1G-image-data-synthetic-raw",
    )


def run_images_benchmark_multi_node(benchmark: Benchmark):
    hundred_thousand_image_paths = [
        f"s3://air-example-data-2/100k-images-data-synthetic-raw/dog_{i}/dog_0.jpg"
        for i in range(100_000)
    ]
    hundred_million_image_paths = []
    for _ in range(100_000_000 // 100_000):
        hundred_million_image_paths.extend(hundred_thousand_image_paths)

    def fn():
        ds = ray.data.read_images(hundred_million_image_paths)
        for _ in ds.iter_batches(batch_size=None, batch_format="pyarrow"):
            pass

    benchmark.run_fn("images-100M", fn)


if __name__ == "__main__":
    args = parse_args()
    main(args)
