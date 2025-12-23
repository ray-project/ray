"""Benchmark script for iterating through Ray Data parquet datasets with different hyperparameters.

This script:
- Loads parquet files
- Applies different map operations (various crop/scale transforms)
- Iterates through batches with different batch sizes and prefetch values
- Cycles through all hyperparameter combinations
"""

import io
import itertools
import time
from typing import Callable, Dict, List, Union
import argparse
import numpy as np
import torchvision.transforms as transforms
from PIL import Image
from tabulate import tabulate
from benchmark import Benchmark
import ray
import ray.data

# Standalone constants
IMAGENET_PARQUET_SPLIT_S3_ROOT = (
    "s3://ray-benchmark-data-internal-us-west-2/imagenet/parquet_split"
)
IMAGENET_PARQUET_SPLIT_S3_DIRS = {
    "train": f"{IMAGENET_PARQUET_SPLIT_S3_ROOT}/train",
    "val": f"{IMAGENET_PARQUET_SPLIT_S3_ROOT}/val",
    "test": f"{IMAGENET_PARQUET_SPLIT_S3_ROOT}/test",
}

# ============================================================================
# HYPERPARAMETERS - Modify these to change benchmark configuration
# ============================================================================

# Transform types to test
TRANSFORM_TYPES = [
    "random_crop",
    "large_crop",
    "small_crop",
    "center_crop",
    "scale_up",
    "scale_down",
]

# Batch sizes
BATCH_SIZES = [16, 32, 64, 128]

# Prefetch batches
PREFETCH_BATCHES_LIST = [1, 2, 4, 8]

# Number of image columns per row
NUM_IMAGE_COLUMNS_LIST = [64]

# Number of batches to process
DEFAULT_NUM_BATCHES = 16

# Simulated training time per batch in seconds
SIMULATED_TRAINING_TIME_PER_BATCH = 0.5

# Data directory to use
DATA_DIR = IMAGENET_PARQUET_SPLIT_S3_DIRS["train"]

# Dynamic label mapping - builds mapping as labels are encountered
_label_to_id_map: Dict[str, int] = {}
_label_counter = 0


def get_label_id(label: str) -> int:
    """Get integer ID for a label (WNID), creating mapping dynamically.

    Args:
        label: String label (WNID format)

    Returns:
        Integer ID for the label
    """
    global _label_to_id_map, _label_counter

    if label not in _label_to_id_map:
        _label_to_id_map[label] = _label_counter
        _label_counter += 1

    return _label_to_id_map[label]


def get_map_fn_with_transform(
    transform_type: str, decode_image: bool = True, num_image_columns: int = 1
) -> Callable[[Dict[str, Union[bytes, str]]], Dict[str, Union[np.ndarray, int]]]:
    """Get a map function with a specific transform type.

    Args:
        transform_type: Type of transform to apply. Options:
            - "random_crop": RandomResizedCrop with default scale/ratio
            - "large_crop": RandomResizedCrop with larger scale range
            - "small_crop": RandomResizedCrop with smaller scale range
            - "center_crop": CenterCrop with resize
            - "scale_up": Resize to larger size then crop
            - "scale_down": Resize to smaller size then crop
        decode_image: Whether to decode the image bytes into a tensor
        num_image_columns: Number of image columns per row (simulated by duplicating the image)

    Returns:
        A map function that processes a row dict
    """
    # Create transform based on type
    if transform_type == "random_crop":
        transform = transforms.Compose(
            [
                transforms.RandomResizedCrop(
                    antialias=True,
                    size=224,
                    scale=(0.05, 1.0),
                    ratio=(0.75, 1.33),
                ),
                transforms.RandomHorizontalFlip(),
            ]
        )
    elif transform_type == "large_crop":
        transform = transforms.Compose(
            [
                transforms.RandomResizedCrop(
                    antialias=True,
                    size=224,
                    scale=(0.2, 1.0),  # Larger minimum scale
                    ratio=(0.5, 2.0),  # Wider ratio range
                ),
                transforms.RandomHorizontalFlip(),
            ]
        )
    elif transform_type == "small_crop":
        transform = transforms.Compose(
            [
                transforms.RandomResizedCrop(
                    antialias=True,
                    size=224,
                    scale=(0.05, 0.5),  # Smaller maximum scale
                    ratio=(0.9, 1.1),  # Narrower ratio range
                ),
                transforms.RandomHorizontalFlip(),
            ]
        )
    elif transform_type == "center_crop":
        transform = transforms.Compose(
            [
                transforms.Resize(256),
                transforms.CenterCrop(224),
            ]
        )
    elif transform_type == "scale_up":
        transform = transforms.Compose(
            [
                transforms.Resize(320),  # Scale up first
                transforms.RandomCrop(224),
                transforms.RandomHorizontalFlip(),
            ]
        )
    elif transform_type == "scale_down":
        transform = transforms.Compose(
            [
                transforms.Resize(180),  # Scale down first
                transforms.RandomCrop(180),
                transforms.Resize(224),
                transforms.RandomHorizontalFlip(),
            ]
        )
    else:
        raise ValueError(f"Unknown transform_type: {transform_type}")

    # Add ToTensor and normalization
    transform = transforms.Compose(
        [
            transform,
            transforms.ToTensor(),
            transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
        ]
    )

    def map_fn(row: Dict[str, Union[bytes, str]]) -> Dict[str, Union[np.ndarray, int]]:
        """Process a single row into the expected format."""
        assert "image" in row and "label" in row, row.keys()

        # Process the original image
        if decode_image:
            # Decode image from bytes to PIL Image
            image_pil = Image.open(io.BytesIO(row["image"]))
        else:
            # If already decoded, convert tensor/array to PIL
            if isinstance(row["image"], np.ndarray):
                image_pil = Image.fromarray(row["image"])
            else:
                # Assume it's a tensor, convert to numpy then PIL
                if hasattr(row["image"], "numpy"):
                    image_pil = Image.fromarray(row["image"].numpy().transpose(1, 2, 0))
                else:
                    image_pil = Image.fromarray(row["image"])

        # Apply transform (expects PIL Image, returns tensor)
        image_tensor = transform(image_pil)

        # Convert tensor to numpy array (CHW format)
        if hasattr(image_tensor, "numpy"):
            processed_image = image_tensor.numpy()
        elif hasattr(image_tensor, "detach"):
            # PyTorch tensor
            processed_image = image_tensor.detach().cpu().numpy()
        else:
            processed_image = np.array(image_tensor)

        # Create multiple image columns by duplicating the processed image
        # Store each image as a separate column (image_0, image_1, etc.)
        for i in range(num_image_columns):
            if i == 0:
                # Keep the original "image" column for backward compatibility
                row["image"] = processed_image
            else:
                # Create additional columns for each image
                row[f"image_{i}"] = processed_image

        # Convert label to integer ID
        row["label"] = get_label_id(row["label"])

        return row

    return map_fn


def benchmark_iteration(
    dataset: ray.data.Dataset,
    batch_size: int,
    prefetch_batches: int,
    num_batches: int = 100,
) -> Dict[str, float]:
    """Benchmark iterating through batches.

    Args:
        dataset: Ray dataset to iterate through
        batch_size: Batch size for iter_torch_batches
        prefetch_batches: Number of batches to prefetch
        num_batches: Number of batches to iterate through for timing

    Returns:
        Dictionary with timing metrics
    """
    start_time = time.time()

    # Create iterator
    iterator = dataset.iter_torch_batches(
        batch_size=batch_size,
        prefetch_batches=prefetch_batches,
        drop_last=True,
    )

    # Iterate through batches
    batch_count = 0
    total_rows = 0

    for batch in iterator:
        batch_count += 1
        if "image" in batch:
            total_rows += len(batch["image"])

        # Simulate training time
        time.sleep(SIMULATED_TRAINING_TIME_PER_BATCH)

        if batch_count >= num_batches:
            break

    elapsed_time = time.time() - start_time

    return {
        "elapsed_time": elapsed_time,
        "batches_processed": batch_count,
        "rows_processed": total_rows,
        "rows_per_second": total_rows / elapsed_time if elapsed_time > 0 else 0,
        "batches_per_second": batch_count / elapsed_time if elapsed_time > 0 else 0,
    }


def run_benchmark(
    data_dir: str,
    transform_types: List[str],
    batch_sizes: List[int],
    prefetch_batches_list: List[int],
    num_image_columns_list: List[int],
    num_batches: int,
) -> List[Dict]:
    """Run benchmarks with all hyperparameter combinations.

    Args:
        data_dir: Path to parquet data directory
        transform_types: List of transform types to test
        batch_sizes: List of batch sizes to test
        prefetch_batches_list: List of prefetch_batches values to test
        num_image_columns_list: List of number of image columns per row to test
        num_batches: Number of batches to process per benchmark

    Returns:
        List of benchmark results
    """
    results = []

    # Generate all combinations
    combinations = list(
        itertools.product(
            transform_types, batch_sizes, prefetch_batches_list, num_image_columns_list
        )
    )

    print(f"Running {len(combinations)} benchmark combinations...")
    print(f"Transform types: {transform_types}")
    print(f"Batch sizes: {batch_sizes}")
    print(f"Prefetch batches: {prefetch_batches_list}")
    print(f"Number of image columns: {num_image_columns_list}")
    print(f"Number of batches: {num_batches}")

    for transform_type, batch_size, prefetch_batches, num_image_columns in combinations:
        print(
            f"Benchmarking: transform={transform_type}, "
            f"batch_size={batch_size}, prefetch_batches={prefetch_batches}, "
            f"num_image_columns={num_image_columns}"
        )

        # Calculate limit based on batch_size and num_batches
        # Need enough rows for num_batches of batch_size, plus a small buffer for drop_last
        limit = batch_size * num_batches + batch_size

        # Load dataset
        ds = ray.data.read_parquet(
            data_dir,
            columns=["image", "label"],
        )

        # Apply limit based on batch size and num batches
        ds = ds.limit(limit)

        # Apply map with transform
        map_fn = get_map_fn_with_transform(
            transform_type, decode_image=True, num_image_columns=num_image_columns
        )
        ds = ds.map(map_fn)

        # Run benchmark
        metrics = benchmark_iteration(
            dataset=ds,
            batch_size=batch_size,
            prefetch_batches=prefetch_batches,
            num_batches=num_batches,
        )

        # Store results
        result = {
            "transform_type": transform_type,
            "batch_size": batch_size,
            "prefetch_batches": prefetch_batches,
            "num_image_columns": num_image_columns,
            **metrics,
        }
        results.append(result)

        print(
            f"  Results: {metrics['rows_per_second']:.2f} rows/sec, "
            f"{metrics['batches_per_second']:.2f} batches/sec"
        )
        print()

    return results


def print_summary(results: List[Dict]):
    """Print summary of benchmark results using tabulate."""
    if not results:
        print("No results to display.")
        return

    # Sort results by batch_size, prefetch_batches, and num_image_columns
    sorted_results = sorted(
        results,
        key=lambda x: (x["batch_size"], x["prefetch_batches"], x["num_image_columns"]),
    )

    # Prepare table data
    headers = [
        "Transform",
        "Batch Size",
        "Prefetch",
        "Image Cols",
        "Rows/sec",
        "Batches/sec",
        "Rows",
        "Batches",
        "Time (s)",
    ]

    table_data = []
    for result in sorted_results:
        table_data.append(
            [
                result["transform_type"],
                result["batch_size"],
                result["prefetch_batches"],
                result["num_image_columns"],
                f"{result['rows_per_second']:.2f}",
                f"{result['batches_per_second']:.2f}",
                result["rows_processed"],
                result["batches_processed"],
                f"{result['elapsed_time']:.2f}",
            ]
        )

    # Print table to stdout
    print(tabulate(table_data, headers=headers, tablefmt="grid"))


def main():
    """Main entry point for the benchmark."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-batches", type=int, default=DEFAULT_NUM_BATCHES)
    args = parser.parse_args()

    # Run benchmarks using hyperparameters
    results = run_benchmark(
        data_dir=DATA_DIR,
        transform_types=TRANSFORM_TYPES,
        batch_sizes=BATCH_SIZES,
        prefetch_batches_list=PREFETCH_BATCHES_LIST,
        num_image_columns_list=NUM_IMAGE_COLUMNS_LIST,
        num_batches=args.num_batches,
    )

    # Print summary table
    print_summary(results)

    if results:
        return {
            "results": results,
            "transform_types": TRANSFORM_TYPES,
            "batch_sizes": BATCH_SIZES,
            "prefetch_batches_list": PREFETCH_BATCHES_LIST,
            "num_image_columns_list": NUM_IMAGE_COLUMNS_LIST,
            "num_batches": args.num_batches,
        }


if __name__ == "__main__":
    benchmark = Benchmark()
    benchmark.run_fn("training-ingest-micro-benchmark", main)
    benchmark.write_result()
