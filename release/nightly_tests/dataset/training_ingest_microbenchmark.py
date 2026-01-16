"""Benchmark script for training data ingest with Ray Data.

This script benchmarks different approaches for loading and preprocessing images:
- Loads images from S3 (parquet or JPEG format)
- Applies image transforms (crop, scale, flip)
- Iterates through batches with configurable batch sizes and prefetch settings
- Tests all hyperparameter combinations

Supported data formats:
- images_with_read_parquet: Uses ray.data.read_parquet() with embedded image bytes
- images_with_map_batches: Lists JPEG files via torchdata, downloads with map_batches
- images_with_read_images: Uses ray.data.read_images() with Partitioning
"""

import io
import itertools
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, List, Optional
import argparse
import numpy as np
import torchvision.transforms as transforms
from PIL import Image
from tabulate import tabulate
from benchmark import Benchmark
from dataset_benchmark_util import IMAGENET_WNID_TO_ID
import ray
import ray.data

logger = logging.getLogger(__name__)


@dataclass
class BenchmarkConfig:
    """Configuration for the training ingest benchmark."""

    # Data format options
    data_format: str = "images_with_read_parquet"

    # Transform types to benchmark
    transform_types: List[str] = field(
        default_factory=lambda: [
            "random_crop",
            "large_crop",
            "small_crop",
            "center_crop",
            "scale_up",
            "scale_down",
        ]
    )

    # Batch sizes to test
    batch_sizes: List[int] = field(default_factory=lambda: [64])

    # Prefetch batch counts to test
    prefetch_batches_list: List[int] = field(default_factory=lambda: [3])

    # Number of image columns per row to test
    num_image_columns_list: List[int] = field(default_factory=lambda: [64])

    # Number of batches to process per benchmark run
    num_batches: int = 16

    # Optional simulated training time (seconds) per batch
    simulated_training_time: Optional[float] = None

    # Data split to use
    split: str = "train"

    @property
    def supported_formats(self) -> List[str]:
        """Return list of supported data formats."""
        return [
            "images_with_read_parquet",
            "images_with_map_batches",
            "images_with_read_images",
        ]

    def validate(self):
        """Validate configuration values."""
        if self.data_format not in self.supported_formats:
            raise ValueError(
                f"Unknown data format: {self.data_format}. "
                f"Supported: {self.supported_formats}"
            )

    def log_config(self):
        """Log the current configuration."""
        logger.info("=" * 80)
        logger.info("BENCHMARK CONFIGURATION")
        logger.info("=" * 80)
        logger.info(f"Data format: {self.data_format}")
        logger.info(f"Split: {self.split}")
        logger.info(f"Transform types: {self.transform_types}")
        logger.info(f"Batch sizes: {self.batch_sizes}")
        logger.info(f"Prefetch batches: {self.prefetch_batches_list}")
        logger.info(f"Number of image columns: {self.num_image_columns_list}")
        logger.info(f"Number of batches: {self.num_batches}")
        logger.info(f"Simulated training time: {self.simulated_training_time}")
        logger.info("=" * 80)


class BaseDataLoader(ABC):
    """Abstract base class for benchmark data loaders.

    Provides shared functionality for loading and transforming image datasets.
    Subclasses implement format-specific data loading logic.
    """

    # Transform configurations: {name: (base_transforms, use_horizontal_flip)}
    TRANSFORM_CONFIGS = {
        "random_crop": (
            lambda: transforms.RandomResizedCrop(
                antialias=True, size=224, scale=(0.05, 1.0), ratio=(0.75, 1.33)
            ),
            True,
        ),
        "large_crop": (
            lambda: transforms.RandomResizedCrop(
                antialias=True, size=224, scale=(0.2, 1.0), ratio=(0.5, 2.0)
            ),
            True,
        ),
        "small_crop": (
            lambda: transforms.RandomResizedCrop(
                antialias=True, size=224, scale=(0.05, 0.5), ratio=(0.9, 1.1)
            ),
            True,
        ),
        "center_crop": (
            lambda: transforms.Compose(
                [transforms.Resize(256), transforms.CenterCrop(224)]
            ),
            False,
        ),
        "scale_up": (
            lambda: transforms.Compose(
                [transforms.Resize(320), transforms.RandomCrop(224)]
            ),
            True,
        ),
        "scale_down": (
            lambda: transforms.Compose(
                [
                    transforms.Resize(180),
                    transforms.RandomCrop(180),
                    transforms.Resize(224),
                ]
            ),
            True,
        ),
    }

    def __init__(self, data_dir: str, label_to_id_map: Dict[str, int] = None):
        """Initialize the data loader.

        Args:
            data_dir: Path to data directory
            label_to_id_map: Mapping from label strings to integer IDs
        """
        self.data_dir = data_dir
        self.label_to_id_map = label_to_id_map or IMAGENET_WNID_TO_ID

    @classmethod
    def get_transform(cls, transform_type: str) -> transforms.Compose:
        """Get an image transform pipeline for the specified transform type."""
        if transform_type not in cls.TRANSFORM_CONFIGS:
            raise ValueError(f"Unknown transform_type: {transform_type}")

        base_fn, use_flip = cls.TRANSFORM_CONFIGS[transform_type]
        transform_list = [base_fn()]
        if use_flip:
            transform_list.append(transforms.RandomHorizontalFlip())

        return transforms.Compose(
            [
                transforms.Compose(transform_list),
                transforms.ToTensor(),
                transforms.Normalize(
                    mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]
                ),
            ]
        )

    @staticmethod
    def tensor_to_numpy(tensor) -> np.ndarray:
        """Convert a tensor to numpy array."""
        if hasattr(tensor, "detach"):
            return tensor.detach().cpu().numpy()
        elif hasattr(tensor, "numpy"):
            return tensor.numpy()
        return np.array(tensor)

    @staticmethod
    def add_image_columns(result: Dict, processed_image: np.ndarray, num_columns: int):
        """Add multiple image columns to result dict."""
        result["image"] = processed_image
        for i in range(1, num_columns):
            result[f"image_{i}"] = processed_image.copy()

    def convert_label(self, label: str) -> int:
        """Convert a string label to integer ID."""
        return self.label_to_id_map.get(label, -1)

    @abstractmethod
    def create_dataset(
        self,
        transform_type: str,
        batch_size: int,
        num_batches: int,
        num_image_columns: int,
    ) -> ray.data.Dataset:
        """Create a Ray dataset with the specified configuration.

        Args:
            transform_type: Type of image transform to apply
            batch_size: Batch size for processing
            num_batches: Number of batches to prepare (for limiting data)
            num_image_columns: Number of image columns per row

        Returns:
            Configured Ray dataset ready for iteration
        """
        raise NotImplementedError

    @staticmethod
    def parse_s3_url(s3_url: str) -> tuple:
        """Parse an S3 URL into bucket and key components."""
        if not s3_url.startswith("s3://"):
            raise ValueError(f"Invalid S3 URL: {s3_url}")
        s3_parts = s3_url.replace("s3://", "").split("/", 1)
        return s3_parts[0], s3_parts[1] if len(s3_parts) > 1 else ""


class ParquetS3Loader(BaseDataLoader):
    """Data loader for parquet format with embedded image bytes.

    Caches the base dataset (before map) to avoid repeated file listings.
    """

    # S3 configuration
    S3_ROOT = "s3://ray-benchmark-data-internal-us-west-2/imagenet/parquet_split"
    SPLIT_DIRS = {
        "train": f"{S3_ROOT}/train",
        "val": f"{S3_ROOT}/val",
        "test": f"{S3_ROOT}/test",
    }

    def __init__(self, data_dir: str, label_to_id_map: Dict[str, int] = None):
        """Initialize the data loader with base dataset cache."""
        super().__init__(data_dir, label_to_id_map)
        self._base_dataset_cache: Optional[ray.data.Dataset] = None
        self._cached_limit: int = 0

    @classmethod
    def get_data_dir(cls, split: str = "train") -> str:
        """Get the data directory for the specified split."""
        if split not in cls.SPLIT_DIRS:
            raise ValueError(f"Unknown split: {split}")
        return cls.SPLIT_DIRS[split]

    def get_base_dataset(self, limit: int) -> ray.data.Dataset:
        """Get the base dataset (read_parquet + limit), creating and caching if needed."""
        if self._base_dataset_cache is not None and limit <= self._cached_limit:
            logger.info(f"Using cached base dataset (limit={self._cached_limit})")
            return self._base_dataset_cache

        logger.info(f"Reading parquet from {self.data_dir}...")
        ds = ray.data.read_parquet(self.data_dir, columns=["image", "label"])
        ds = ds.limit(limit)

        # Cache the base dataset
        self._base_dataset_cache = ds
        self._cached_limit = limit
        logger.info(f"Created and cached base dataset (limit={limit})")

        return ds

    def create_dataset(
        self,
        transform_type: str,
        batch_size: int,
        num_batches: int,
        num_image_columns: int,
    ) -> ray.data.Dataset:
        """Create dataset by applying map to the cached base dataset."""
        limit = batch_size * num_batches + batch_size
        transform = self.get_transform(transform_type)

        # Capture instance variables for closure
        label_to_id_map = self.label_to_id_map

        def process_row(row: Dict) -> Dict:
            image_pil = Image.open(io.BytesIO(row["image"])).convert("RGB")
            processed = BaseDataLoader.tensor_to_numpy(transform(image_pil))
            BaseDataLoader.add_image_columns(row, processed, num_image_columns)
            row["label"] = label_to_id_map.get(row["label"], -1)
            return row

        # Get base dataset (cached after first call)
        base_ds = self.get_base_dataset(limit)
        return base_ds.map(process_row)


class MapBatchesS3Loader(BaseDataLoader):
    """Data loader for JPEG files stored in S3.

    Uses torchdata for S3 file listing (same approach as multi_node_train_benchmark.py).
    Caches the file listing and base dataset to avoid repeated slow listings.
    """

    # S3 configuration
    AWS_REGION = "us-west-2"
    S3_ROOT = "s3://anyscale-imagenet/ILSVRC/Data/CLS-LOC"
    SPLIT_DIRS = {
        "train": f"{S3_ROOT}/train",
        "val": f"{S3_ROOT}/val",
        "test": f"{S3_ROOT}/test",
    }

    def __init__(self, data_dir: str, label_to_id_map: Dict[str, int] = None):
        """Initialize the data loader with file listing cache."""
        super().__init__(data_dir, label_to_id_map)
        self._file_records_cache: Optional[List[Dict[str, str]]] = None
        self._base_dataset_cache: Optional[ray.data.Dataset] = None
        self._cached_limit: int = 0

    @classmethod
    def get_data_dir(cls, split: str = "train") -> str:
        """Get the data directory for the specified split."""
        if split not in cls.SPLIT_DIRS:
            raise ValueError(f"Unknown split: {split}")
        return cls.SPLIT_DIRS[split]

    def _list_files(self, limit: int = None) -> List[Dict[str, str]]:
        """List JPEG files from S3 with class labels extracted from path.

        Uses torchdata's S3 listing. Results are cached.
        """
        # Return cached results if available and sufficient
        if self._file_records_cache is not None:
            if limit is None or len(self._file_records_cache) >= limit:
                logger.info(
                    f"Using cached file list ({len(self._file_records_cache)} files)"
                )
                return (
                    self._file_records_cache[:limit]
                    if limit
                    else self._file_records_cache
                )

        import os
        from torchdata.datapipes.iter import IterableWrapper

        # Required for torchdata S3 access
        os.environ.setdefault("S3_VERIFY_SSL", "0")
        os.environ.setdefault("AWS_REGION", self.AWS_REGION)

        logger.info(f"Listing JPEG files from {self.data_dir}...")

        # List all files using torchdata
        file_url_dp = IterableWrapper([self.data_dir]).list_files_by_s3()

        # Cache more than needed to avoid re-listing for larger limits
        cache_limit = max(10000, (limit or 1000) * 10)

        # Extract class labels from path structure: .../class_name/image.jpg
        file_records = []
        for file_path in file_url_dp:
            if not file_path.lower().endswith((".jpg", ".jpeg")):
                continue

            # Extract class from path: s3://bucket/prefix/class/image.jpg
            parts = file_path.rstrip("/").split("/")
            if len(parts) >= 2:
                class_name = parts[-2]  # Parent directory is the class
                file_records.append({"path": file_path, "class": class_name})

            if len(file_records) >= cache_limit:
                break

        logger.info(f"Listed and cached {len(file_records)} JPEG files")
        self._file_records_cache = file_records

        return file_records[:limit] if limit else file_records

    def get_base_dataset(self, limit: int) -> ray.data.Dataset:
        """Get the base dataset (from_items with file records), creating and caching if needed."""
        if self._base_dataset_cache is not None and limit <= self._cached_limit:
            logger.info(f"Using cached base dataset (limit={self._cached_limit})")
            return self._base_dataset_cache

        file_records = self._list_files(limit=limit)
        ds = ray.data.from_items(file_records)

        # Cache the base dataset
        self._base_dataset_cache = ds
        self._cached_limit = limit
        logger.info(f"Created and cached base dataset (limit={limit})")

        return ds

    def create_dataset(
        self,
        transform_type: str,
        batch_size: int,
        num_batches: int,
        num_image_columns: int,
    ) -> ray.data.Dataset:
        """Create dataset by applying map_batches to the cached base dataset."""
        limit = batch_size * num_batches + batch_size

        # Get base dataset (cached after first call)
        base_ds = self.get_base_dataset(limit)

        transform = self.get_transform(transform_type)
        label_to_id_map = self.label_to_id_map

        def download_and_process_batch(
            batch: Dict[str, np.ndarray]
        ) -> Dict[str, np.ndarray]:
            from torchdata.datapipes.iter import IterableWrapper, S3FileLoader

            processed_images = []
            labels = []

            # Use torchdata's S3FileLoader for downloading
            paths = list(batch["path"])
            classes = list(batch["class"])
            file_dp = S3FileLoader(IterableWrapper(paths))

            for (url, fd), wnid in zip(file_dp, classes):
                data = fd.file_obj.read()
                image_pil = Image.open(io.BytesIO(data)).convert("RGB")
                processed_images.append(
                    BaseDataLoader.tensor_to_numpy(transform(image_pil))
                )
                labels.append(label_to_id_map.get(wnid, -1))

            result = {"label": np.array(labels)}
            BaseDataLoader.add_image_columns(
                result, np.stack(processed_images), num_image_columns
            )
            return result

        return base_ds.map_batches(download_and_process_batch, batch_size=batch_size)


class ReadImagesS3Loader(BaseDataLoader):
    """Data loader using ray.data.read_images() with Partitioning.

    Uses the same approach as multi_node_train_benchmark.py for reading images.
    Caches the base dataset (before map) to avoid repeated file listings.
    """

    # S3 configuration
    AWS_REGION = "us-west-2"
    S3_ROOT = "s3://anyscale-imagenet/ILSVRC/Data/CLS-LOC"
    SPLIT_DIRS = {
        "train": f"{S3_ROOT}/train",
        "val": f"{S3_ROOT}/val",
        "test": f"{S3_ROOT}/test",
    }

    def __init__(self, data_dir: str, label_to_id_map: Dict[str, int] = None):
        """Initialize the data loader with base dataset cache."""
        super().__init__(data_dir, label_to_id_map)
        self._base_dataset_cache: Optional[ray.data.Dataset] = None
        self._cached_limit: int = 0

    @classmethod
    def get_data_dir(cls, split: str = "train") -> str:
        """Get the data directory for the specified split."""
        if split not in cls.SPLIT_DIRS:
            raise ValueError(f"Unknown split: {split}")
        return cls.SPLIT_DIRS[split]

    @staticmethod
    def _get_s3fs_with_boto_creds():
        """Get S3 filesystem with boto credentials.

        Same as multi_node_train_benchmark.py to avoid ACCESS_DENIED errors.
        """
        import boto3
        from pyarrow import fs

        credentials = boto3.Session().get_credentials()
        s3fs = fs.S3FileSystem(
            access_key=credentials.access_key,
            secret_key=credentials.secret_key,
            session_token=credentials.token,
            region=ReadImagesS3Loader.AWS_REGION,
        )
        return s3fs

    def get_base_dataset(self, limit: int) -> ray.data.Dataset:
        """Get the base dataset (read_images + limit), creating and caching if needed.

        The base dataset is cached to avoid repeated slow file listings.
        If a larger limit is requested, the cache is invalidated and rebuilt.
        """
        if self._base_dataset_cache is not None and limit <= self._cached_limit:
            logger.info(f"Using cached base dataset (limit={self._cached_limit})")
            return self._base_dataset_cache

        from ray.data.datasource.partitioning import Partitioning

        # Use partitioning to extract class from directory structure
        partitioning = Partitioning(
            "dir",
            field_names=["class"],
            base_dir=self.data_dir,
        )

        # Use S3 filesystem with boto credentials
        fs = self._get_s3fs_with_boto_creds()

        logger.info(f"Reading images from {self.data_dir} using read_images()...")
        ds = ray.data.read_images(
            self.data_dir,
            filesystem=fs,
            mode="RGB",
            partitioning=partitioning,
        )
        ds = ds.limit(limit)

        # Cache the base dataset
        self._base_dataset_cache = ds
        self._cached_limit = limit
        logger.info(f"Created and cached base dataset (limit={limit})")

        return ds

    def create_dataset(
        self,
        transform_type: str,
        batch_size: int,
        num_batches: int,
        num_image_columns: int,
    ) -> ray.data.Dataset:
        """Create dataset by applying map to the cached base dataset."""
        limit = batch_size * num_batches + batch_size

        # Get base dataset (cached after first call)
        base_ds = self.get_base_dataset(limit)

        transform = self.get_transform(transform_type)
        label_to_id_map = self.label_to_id_map

        def process_row(row: Dict) -> Dict:
            # Image is already loaded as numpy array by read_images
            image_pil = Image.fromarray(row["image"])
            processed = BaseDataLoader.tensor_to_numpy(transform(image_pil))
            BaseDataLoader.add_image_columns(row, processed, num_image_columns)
            row["label"] = label_to_id_map.get(row["class"], -1)
            del row["class"]
            return row

        return base_ds.map(process_row)


def get_data_loader(data_format: str, split: str = "train") -> BaseDataLoader:
    """Factory function to create the appropriate data loader.

    Args:
        data_format: One of "images_with_read_parquet", "images_with_map_batches",
            or "images_with_read_images"
        split: Data split to use ("train", "val", or "test")

    Returns:
        Configured data loader instance
    """
    if data_format == "images_with_read_parquet":
        data_dir = ParquetS3Loader.get_data_dir(split)
        return ParquetS3Loader(data_dir)
    elif data_format == "images_with_map_batches":
        data_dir = MapBatchesS3Loader.get_data_dir(split)
        return MapBatchesS3Loader(data_dir)
    elif data_format == "images_with_read_images":
        data_dir = ReadImagesS3Loader.get_data_dir(split)
        return ReadImagesS3Loader(data_dir)
    else:
        raise ValueError(f"Unknown data format: {data_format}")


def benchmark_iteration(
    dataset: ray.data.Dataset,
    batch_size: int,
    prefetch_batches: int,
    num_batches: int = 100,
    simulated_training_time: float = None,
) -> Dict[str, float]:
    """Benchmark iterating through batches.

    Args:
        dataset: Ray dataset to iterate through
        batch_size: Batch size for iter_torch_batches
        prefetch_batches: Number of batches to prefetch
        num_batches: Number of batches to iterate through for timing
        simulated_training_time: Time in seconds to sleep per batch to simulate training.
            If None, no sleep is performed.

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

        # Simulate training time if configured
        if simulated_training_time is not None:
            time.sleep(simulated_training_time)

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


def run_benchmark(config: BenchmarkConfig) -> List[Dict]:
    """Run benchmarks with all hyperparameter combinations.

    Args:
        config: Benchmark configuration

    Returns:
        List of benchmark results
    """
    config.validate()
    results = []

    # Create data loader for the specified format
    data_loader = get_data_loader(config.data_format, config.split)
    logger.info(
        f"Using {data_loader.__class__.__name__} with "
        f"{len(data_loader.label_to_id_map)} classes"
    )
    logger.info(f"Data directory: {data_loader.data_dir}")

    # Generate all combinations
    combinations = list(
        itertools.product(
            config.transform_types,
            config.batch_sizes,
            config.prefetch_batches_list,
            config.num_image_columns_list,
        )
    )

    logger.info(f"Running {len(combinations)} benchmark combinations...")

    for transform_type, batch_size, prefetch_batches, num_image_columns in combinations:
        logger.info(
            f"Benchmarking: transform={transform_type}, "
            f"batch_size={batch_size}, prefetch_batches={prefetch_batches}, "
            f"num_image_columns={num_image_columns}"
        )

        # Create dataset using the data loader
        ds = data_loader.create_dataset(
            transform_type=transform_type,
            batch_size=batch_size,
            num_batches=config.num_batches,
            num_image_columns=num_image_columns,
        )

        # Run benchmark
        metrics = benchmark_iteration(
            dataset=ds,
            batch_size=batch_size,
            prefetch_batches=prefetch_batches,
            num_batches=config.num_batches,
            simulated_training_time=config.simulated_training_time,
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

        logger.info(
            f"  Results: {metrics['rows_per_second']:.2f} rows/sec, "
            f"{metrics['batches_per_second']:.2f} batches/sec"
        )

    return results


def print_summary(results: List[Dict]):
    """Print summary of benchmark results using tabulate."""
    if not results:
        logger.warning("No results to display.")
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
    logger.info("\n" + tabulate(table_data, headers=headers, tablefmt="grid"))


def main():
    """Main entry point for the benchmark."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Create default config to get supported formats
    default_config = BenchmarkConfig()

    parser = argparse.ArgumentParser(
        description="Benchmark Ray Data image loading with parquet or JPEG formats."
    )
    parser.add_argument(
        "--num-batches",
        type=int,
        default=default_config.num_batches,
        help=f"Number of batches to process. Default: {default_config.num_batches}",
    )
    parser.add_argument(
        "--simulated-training-time",
        type=float,
        default=default_config.simulated_training_time,
        help="Time in seconds to sleep per batch to simulate training.",
    )
    parser.add_argument(
        "--data-format",
        type=str,
        choices=default_config.supported_formats,
        default=default_config.data_format,
        help=f"Data format. Default: {default_config.data_format}",
    )
    parser.add_argument(
        "--split",
        type=str,
        choices=["train", "val", "test"],
        default=default_config.split,
        help=f"Data split to use. Default: {default_config.split}",
    )
    args = parser.parse_args()

    # Build configuration from CLI args
    config = BenchmarkConfig(
        data_format=args.data_format,
        num_batches=args.num_batches,
        simulated_training_time=args.simulated_training_time,
        split=args.split,
    )

    # Log benchmark configuration
    config.log_config()

    # Run benchmarks
    results = run_benchmark(config)

    # Print summary table
    print_summary(results)

    if results:
        return {
            "results": results,
            "data_format": config.data_format,
            "transform_types": config.transform_types,
            "batch_sizes": config.batch_sizes,
            "prefetch_batches_list": config.prefetch_batches_list,
            "num_image_columns_list": config.num_image_columns_list,
            "num_batches": config.num_batches,
        }


if __name__ == "__main__":
    benchmark = Benchmark()
    benchmark.run_fn("training-ingest-micro-benchmark", main)
    benchmark.write_result()
