import time
import argparse
import logging
from typing import Any, Dict, Optional, Tuple
import torch
import numpy as np
import ray

logger = logging.getLogger("BenchmarkLogger")
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO
)


def collate_fn(batch: Dict[str, np.ndarray]) -> Any:
    """Collates a batch of image and non-image data into tensors."""
    # Separate the images from the other columns
    image_batch = [torch.as_tensor(batch["image"][i]) for i in range(len(batch))]
    other_columns_batch = {
        key: [torch.as_tensor(value[i]) for i in range(len(value))]
        for key, value in batch.items()
        if key != "image"
    }

    # Stack image batch
    # Shape: [batch_size, channels, height, width]
    images = torch.stack(image_batch, axis=0)

    # Stack the non-image columns
    other_columns = {
        key: torch.stack(value, axis=0) for key, value in other_columns_batch.items()
    }

    # Return a dictionary of images and other columns
    return {"images": images, **other_columns}


def generate_dataset(
    num_rows: int,
    image_size: Tuple[int, int] = (224, 224),
    num_channels: int = 3,
    num_columns: int = 2,
) -> ray.data.Dataset:
    """Generates a dataset with `num_rows` samples of random image data with a
    specified number of channels.
    """

    def generate_image_data(idx: int) -> Dict[str, np.ndarray]:
        # Create a random image with the specified number of channels
        # Shape (channels, height, width)
        image = np.random.rand(num_channels, *image_size).astype(np.float32)
        # Generate additional random columns
        data = {"image": image}
        # Subtract 1 for the 'image' column
        for col_idx in range(num_columns - 1):
            data[f"col_{col_idx+1}"] = np.random.rand(
                image_size[0], image_size[1]
            ).astype(np.float32)
        return data

    return ray.data.from_items([generate_image_data(i) for i in range(num_rows)])


def benchmark(
    dataset_size: int,
    batch_size: int,
    iterations: int,
    prefetch_batches: int,
    local_shuffle_buffer_size: Optional[int],
    local_shuffle_seed: Optional[int],
    image_size: Tuple[int, int],
    num_channels: int,
    num_columns: int,
    verbose: bool,
):
    """Runs a micro-benchmark on iter_torch_batches with the specified parameters."""
    dataset = generate_dataset(dataset_size, image_size, num_channels, num_columns)

    if verbose:
        logger.info(
            f"Benchmarking with dataset_size={dataset_size}, "
            f"batch_size={batch_size}, iterations={iterations}"
        )
        logger.info(
            f"Image Size: {image_size}, Num Channels: {num_channels}, "
            f"Num Columns: {num_columns}"
        )
        logger.info(
            f"Prefetch Batches: {prefetch_batches}, Shuffle Buffer Size: "
            f"{local_shuffle_buffer_size}, Shuffle Seed: {local_shuffle_seed}"
        )
        logger.info("=" * 80)

    times = []
    for i in range(iterations):
        start_time = time.perf_counter()
        if verbose:
            logger.info(f"[Iteration {i+1}] Start")

        for _ in dataset.iter_torch_batches(
            batch_size=batch_size,
            collate_fn=collate_fn,
            prefetch_batches=prefetch_batches,
            local_shuffle_buffer_size=local_shuffle_buffer_size,
            local_shuffle_seed=local_shuffle_seed,
        ):
            pass

        end_time = time.perf_counter()
        iteration_time = end_time - start_time
        times.append(iteration_time)

        if verbose:
            logger.info(
                f"[Iteration {i+1}] Complete | Duration: {iteration_time:.6f} seconds"
            )

    logger.info("=" * 80)
    logger.info("[SUMMARY] Benchmark Results")
    logger.info(
        f"Dataset size: {dataset_size}, Batch size: {batch_size}, "
        f"Iterations: {iterations}"
    )
    logger.info(
        f"Image Size: {image_size}, Num Channels: {num_channels}, "
        f"Num Columns: {num_columns}"
    )
    logger.info(
        f"Prefetch Batches: {prefetch_batches}, Shuffle Buffer Size: "
        f"{local_shuffle_buffer_size}, Shuffle Seed: {local_shuffle_seed}"
    )
    logger.info(f"Average time per iteration: {np.mean(times):.6f} seconds")
    logger.info(f"Standard deviation: {np.std(times):.6f} seconds")
    logger.info("=" * 80)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Micro-benchmark for Ray dataset batching."
    )
    parser.add_argument(
        "--dataset_size", type=int, default=10000, help="Number of rows in the dataset."
    )
    parser.add_argument(
        "--batch_size", type=int, default=256, help="Batch size for iteration."
    )
    parser.add_argument(
        "--iterations", type=int, default=10, help="Number of iterations to benchmark."
    )
    parser.add_argument(
        "--prefetch_batches", type=int, default=1, help="Number of batches to prefetch."
    )
    parser.add_argument(
        "--local_shuffle_buffer_size",
        type=int,
        default=None,
        help="Buffer size for local shuffling.",
    )
    parser.add_argument(
        "--local_shuffle_seed", type=int, default=None, help="Seed for local shuffling."
    )
    parser.add_argument(
        "--image_size",
        type=str,
        default="224,224",
        help="Image size as comma-separated width,height.",
    )
    parser.add_argument(
        "--num_channels",
        type=int,
        default=3,
        help="Number of channels in the image (e.g., 1 for grayscale, 3 for RGB).",
    )
    parser.add_argument(
        "--num_columns",
        type=int,
        default=2,
        help="Number of columns in the dataset (including 'image').",
    )
    parser.add_argument(
        "--verbose", action="store_true", help="Enable verbose logging."
    )

    args = parser.parse_args()

    # Parse the image size
    image_size = tuple(map(int, args.image_size.split(",")))

    ray.init(ignore_reinit_error=True)

    benchmark(
        dataset_size=args.dataset_size,
        batch_size=args.batch_size,
        iterations=args.iterations,
        prefetch_batches=args.prefetch_batches,
        local_shuffle_buffer_size=args.local_shuffle_buffer_size,
        local_shuffle_seed=args.local_shuffle_seed,
        image_size=image_size,
        num_channels=args.num_channels,
        num_columns=args.num_columns,
        verbose=args.verbose,
    )

    ray.shutdown()
