from abc import ABC, abstractmethod
from typing import Any, Dict, Iterator, Tuple, List, Optional
import time
import multiprocessing
import logging
import io

import torch
import boto3
import pandas as pd
from botocore.exceptions import NoCredentialsError
from torch.utils.data import IterableDataset

import ray.train
from ray.data import Dataset

from config import BenchmarkConfig, DataLoaderConfig, RayDataConfig
from image_classification.imagenet import get_preprocess_map_fn

logger = logging.getLogger(__name__)

# Set multiprocessing start method to 'spawn' for CUDA compatibility
if torch.cuda.is_available():
    try:
        multiprocessing.set_start_method("spawn", force=True)
        logger.info(
            "[DataLoader] Set multiprocessing start method to 'spawn' for CUDA compatibility"
        )
    except RuntimeError:
        logger.info("[DataLoader] Multiprocessing start method already set")

# AWS configuration
AWS_REGION = "us-west-2"


class S3Reader:
    """Base class for reading files from S3."""

    class S3Error(Exception):
        """Base exception for S3-related errors."""

        pass

    class S3CredentialsError(S3Error):
        """Raised when AWS credentials are not found or invalid."""

        pass

    class S3FileError(S3Error):
        """Raised when there's an error reading a file from S3."""

        pass

    def __init__(self):
        """Initialize the S3Reader."""
        self._s3_client = None

    @property
    def s3_client(self):
        """Lazy initialization of S3 client to avoid serialization issues."""
        if self._s3_client is None:
            self._s3_client = boto3.client("s3", region_name=AWS_REGION)
        return self._s3_client

    def _parse_s3_url(self, s3_url: str) -> Tuple[str, str]:
        """Parse an S3 URL into bucket and key.

        Args:
            s3_url: The S3 URL to parse

        Returns:
            Tuple[str, str]: The bucket and key

        Raises:
            ValueError: If the S3 URL is invalid
        """
        if s3_url.startswith("s3://"):
            s3_parts = s3_url.replace("s3://", "").split("/", 1)
            return s3_parts[0], s3_parts[1]
        else:
            raise ValueError(f"Invalid S3 URL format: {s3_url}")

    def read_file(self, s3_url: str) -> bytes:
        """Download a file from S3 and return its contents as bytes.

        Args:
            s3_url: The S3 URL of the file

        Returns:
            bytes: The file contents

        Raises:
            S3CredentialsError: If AWS credentials are not found
            S3FileError: If there's an error reading the file
            ValueError: If the S3 URL is invalid
        """
        try:
            bucket, key = self._parse_s3_url(s3_url)
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            return response["Body"].read()
        except NoCredentialsError:
            raise self.S3CredentialsError(
                "AWS credentials not found. Ensure you have configured them."
            )
        except Exception as e:
            raise self.S3FileError(f"Error reading file from {s3_url}: {str(e)}")

    def _get_file_urls(self, url: str) -> List[str]:
        """Get file URLs from S3 and distribute them among Ray workers.

        Args:
            url: The S3 URL to list files from

        Returns:
            List of S3 URLs assigned to the current Ray worker
        """
        try:
            # Get Ray worker info
            worker_rank = ray.train.get_context().get_world_rank()
            logger.info(
                f"[DataLoader] Ray worker {worker_rank}/{self.num_ray_workers} listing files"
            )

            # List files from S3
            bucket, prefix = self._parse_s3_url(url)
            file_urls = []
            response = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

            if "Contents" in response:
                for file in response["Contents"]:
                    file_urls.append(f"s3://{bucket}/{file['Key']}")

            # Sort URLs for consistent distribution
            file_urls.sort()

            # If no workers, return all files
            if self.num_torch_workers == 0:
                logger.info(
                    f"[DataLoader] No torch workers, returning all {len(file_urls)} files"
                )
                return file_urls

            # Round-robin allocation: each worker gets every Nth file
            worker_urls = file_urls[worker_rank :: self.num_ray_workers]

            logger.info(
                f"[DataLoader] Ray worker {worker_rank} assigned {len(worker_urls)}/{len(file_urls)} files"
            )
            return worker_urls

        except NoCredentialsError:
            raise self.S3CredentialsError(
                "AWS credentials not found. Ensure you have configured them."
            )
        except Exception as e:
            raise self.S3FileError(f"Error listing files from {url}: {str(e)}")


class S3ParquetImageIterableDataset(S3Reader, IterableDataset):
    """An iterable dataset that loads images from S3-stored Parquet files.

    This dataset:
    1. Reads Parquet files from S3 one row group at a time
    2. Processes images with optional random transforms
    3. Yields (image, label) tensors
    4. Supports row limits per worker for controlled data processing
    """

    LOG_FREQUENCY = 1000  # Log progress every 1000 rows

    def __init__(
        self,
        file_urls: List[str],
        random_transforms: bool = True,
        limit_rows_per_worker: Optional[int] = None,
    ):
        """Initialize the dataset.

        Args:
            file_urls: List of S3 URLs to load
            random_transforms: Whether to use random transforms for training
            limit_rows_per_worker: Maximum number of rows to process per worker (None for all rows)
        """
        S3Reader.__init__(self)
        self.file_urls = file_urls
        self.limit_rows_per_worker = limit_rows_per_worker
        self.random_transforms = random_transforms

    def _read_parquet_file(self, file_url: str) -> Iterator[pd.DataFrame]:
        """Read a Parquet file from S3 one row group at a time.

        Args:
            file_url: S3 URL of the Parquet file

        Yields:
            DataFrame containing one row group at a time
        """
        try:
            start_time = time.time()
            logger.info(f"[Dataset] Reading Parquet file: {file_url}")

            # Get parquet file metadata
            import pyarrow.parquet as pq

            bucket, key = self._parse_s3_url(file_url)
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            parquet_file = pq.ParquetFile(io.BytesIO(response["Body"].read()))
            num_row_groups = parquet_file.num_row_groups

            logger.info(f"[Dataset] Found {num_row_groups} row groups in {file_url}")

            for row_group in range(num_row_groups):
                # Read row group and convert to pandas
                table = parquet_file.read_row_group(row_group)
                df = table.to_pandas()
                yield df

            total_time = time.time() - start_time
            logger.info(f"[Dataset] Completed reading {file_url} in {total_time:.2f}s")

        except Exception as e:
            logger.error(f"[Dataset] Error reading file {file_url}: {str(e)}")
            raise

    def __iter__(self) -> Iterator[Tuple[torch.Tensor, torch.Tensor]]:
        """Main iteration method that processes files and yields (image, label) tensors.

        This method:
        1. Distributes files among workers
        2. Processes rows with image transforms
        3. Converts to tensors
        4. Respects row limits per worker
        """
        try:
            # Get worker info for file distribution
            worker_info = torch.utils.data.get_worker_info()
            worker_id = worker_info.id if worker_info else 0
            num_workers = worker_info.num_workers if worker_info else 1

            logger.info(f"[Dataset] Worker {worker_id}/{num_workers} starting")

            # Initialize preprocessing function
            preprocess_fn = get_preprocess_map_fn(
                decode_image=True, random_transforms=self.random_transforms
            )

            # Distribute files among workers
            if worker_info is None:
                files_to_read = self.file_urls
            else:
                per_worker = max(1, len(self.file_urls) // num_workers)
                start_idx = worker_id * per_worker
                end_idx = (
                    start_idx + per_worker
                    if worker_id < num_workers - 1
                    else len(self.file_urls)
                )
                files_to_read = self.file_urls[start_idx:end_idx]

            logger.info(
                f"[Dataset] Worker {worker_id} processing {len(files_to_read)} files"
            )

            # Process files and rows
            rows_processed = 0
            last_log_time = time.time()
            total_start_time = time.time()

            for file_url in files_to_read:
                # Skip file if we've reached the limit
                if (
                    self.limit_rows_per_worker is not None
                    and rows_processed >= self.limit_rows_per_worker
                ):
                    logger.info(
                        f"[Dataset] Worker {worker_id} reached row limit: {rows_processed}"
                    )
                    break

                for df in self._read_parquet_file(file_url):
                    # Skip DataFrame if we've reached the limit
                    if (
                        self.limit_rows_per_worker is not None
                        and rows_processed >= self.limit_rows_per_worker
                    ):
                        break

                    for _, row in df.iterrows():
                        # Skip row if we've reached the limit
                        if (
                            self.limit_rows_per_worker is not None
                            and rows_processed >= self.limit_rows_per_worker
                        ):
                            break

                        try:
                            # Process row and convert to tensors
                            processed = preprocess_fn(row)
                            image = torch.as_tensor(
                                processed["image"], dtype=torch.float32
                            )
                            label = torch.as_tensor(
                                processed["label"], dtype=torch.int64
                            )

                            rows_processed += 1

                            # Log progress periodically
                            if rows_processed % self.LOG_FREQUENCY == 0:
                                current_time = time.time()
                                elapsed_time = current_time - last_log_time
                                rows_per_second = (
                                    self.LOG_FREQUENCY / elapsed_time
                                    if elapsed_time > 0
                                    else 0
                                )
                                logger.info(
                                    f"[Dataset] Worker {worker_id}: "
                                    f"Processed {rows_processed} rows ({rows_per_second:.2f} rows/sec)"
                                )
                                last_log_time = current_time

                            yield image, label

                        except Exception as e:
                            logger.error(
                                f"[Dataset] Worker {worker_id} error processing row: {str(e)}"
                            )
                            continue

            # Log final statistics
            total_time = time.time() - total_start_time
            logger.info(
                f"[Dataset] Worker {worker_id} finished: "
                f"{rows_processed} rows in {total_time:.2f}s "
                f"({rows_processed/total_time:.2f} rows/sec)"
            )

        except Exception as e:
            logger.error(
                f"[Dataset] Worker {worker_id} fatal error: {str(e)}", exc_info=True
            )
            raise


class BaseDataLoaderFactory(ABC):
    """Base class for creating and managing dataloaders."""

    def __init__(self, benchmark_config: BenchmarkConfig):
        self.benchmark_config = benchmark_config

    def get_dataloader_config(self) -> DataLoaderConfig:
        return self.benchmark_config.dataloader_config

    @abstractmethod
    def get_train_dataloader(self) -> Iterator[Tuple[torch.Tensor, torch.Tensor]]:
        pass

    @abstractmethod
    def get_val_dataloader(self) -> Iterator[Tuple[torch.Tensor, torch.Tensor]]:
        pass

    def get_metrics(self) -> Dict[str, Any]:
        """Return metrics about dataloader performance."""
        return {}

    def get_ray_datasets(self) -> Dict[str, Dataset]:
        """Get Ray datasets if this loader type uses Ray Data."""
        return {}


class TorchDataLoaderFactory(BaseDataLoaderFactory, S3Reader):
    """Factory for creating PyTorch DataLoaders that read from S3 parquet files.

    This factory:
    1. Creates DataLoaders that read Parquet files from S3
    2. Distributes files among Ray workers using round-robin allocation
    3. Handles device transfer and error handling for batches
    4. Supports row limits per worker for controlled data processing
    """

    @staticmethod
    def worker_init_fn(worker_id: int):
        """Initialize each worker with proper CUDA settings and seed.

        Args:
            worker_id: The ID of the worker being initialized
        """
        # Set worker-specific seed for reproducibility
        worker_seed = torch.initial_seed() % 2**32
        torch.manual_seed(worker_seed)
        if torch.cuda.is_available():
            torch.cuda.manual_seed(worker_seed)
            torch.cuda.manual_seed_all(worker_seed)

        logger.info(
            f"[DataLoader] Initialized worker {worker_id} with seed {worker_seed}"
        )

    def __init__(
        self,
        benchmark_config: BenchmarkConfig,
        train_url: str,
        val_url: str,
        limit_total_rows: Optional[int] = None,
    ):
        """Initialize the factory.

        Args:
            benchmark_config: Configuration for the benchmark
            train_url: S3 URL for training data
            val_url: S3 URL for validation data
            limit_total_rows: Optional limit on total rows to process
        """
        BaseDataLoaderFactory.__init__(self, benchmark_config)
        S3Reader.__init__(self)
        self.train_url = train_url
        self.val_url = val_url

        # Get worker configuration
        num_gpus = torch.cuda.device_count() if torch.cuda.is_available() else 1
        self.num_torch_workers = benchmark_config.num_torch_workers
        self.num_ray_workers = benchmark_config.num_workers

        # Calculate total workers and row limits
        total_workers = self.num_ray_workers * self.num_torch_workers
        if limit_total_rows is not None:
            if total_workers > 0:
                self.limit_rows_per_worker = max(1, limit_total_rows // total_workers)
            else:
                # When no workers, apply limit directly to the dataset
                self.limit_rows_per_worker = limit_total_rows
        else:
            self.limit_rows_per_worker = None

        # Log configuration
        logger.info(
            f"[DataLoader] Configuration: {total_workers} total workers "
            f"({self.num_ray_workers} Ray × {self.num_torch_workers} Torch) "
            f"across {num_gpus} GPUs"
        )
        if limit_total_rows is not None:
            if total_workers > 0 and limit_total_rows < total_workers:
                logger.warning(
                    f"[DataLoader] Warning: limit_total_rows ({limit_total_rows}) is less than "
                    f"total_workers ({total_workers}). Each worker will process at least 1 row."
                )
            logger.info(f"[DataLoader] Rows per worker: {self.limit_rows_per_worker}")

    def _get_device(self) -> torch.device:
        """Get the device for the current worker using Ray Train's device management."""
        device = ray.train.torch.get_device()
        logger.info(f"[DataLoader] Using Ray Train device: {device}")
        return device

    def _create_batch_iterator(
        self, dataloader: torch.utils.data.DataLoader, device: torch.device
    ) -> Iterator[Tuple[torch.Tensor, torch.Tensor]]:
        """Create a safe iterator that handles device transfer and error handling.

        Args:
            dataloader: The PyTorch DataLoader to iterate over
            device: The device to move tensors to

        Returns:
            An iterator that yields batches moved to the specified device
        """
        worker_rank = ray.train.get_context().get_world_rank()
        logger.info(f"[DataLoader] Worker {worker_rank} starting batch iteration")

        try:
            last_batch_time = time.time()
            for batch_idx, batch in enumerate(dataloader):
                try:
                    # Check for delays between batches
                    current_time = time.time()
                    time_since_last_batch = current_time - last_batch_time
                    if time_since_last_batch > 10:
                        logger.warning(
                            f"[DataLoader] Worker {worker_rank}: "
                            f"Long delay ({time_since_last_batch:.2f}s) between batches {batch_idx-1} and {batch_idx}"
                        )

                    # Move batch to device
                    images, labels = batch
                    logger.info(
                        f"[DataLoader] Worker {worker_rank} processing batch {batch_idx} "
                        f"(shape: {images.shape}, time since last: {time_since_last_batch:.2f}s)"
                    )

                    transfer_start = time.time()
                    images = images.to(device, non_blocking=True)
                    labels = labels.to(device, non_blocking=True)
                    transfer_time = time.time() - transfer_start

                    if transfer_time > 5:
                        logger.warning(
                            f"[DataLoader] Worker {worker_rank}: "
                            f"Slow device transfer ({transfer_time:.2f}s) for batch {batch_idx}"
                        )

                    logger.info(
                        f"[DataLoader] Worker {worker_rank} completed device transfer "
                        f"for batch {batch_idx} in {transfer_time:.2f}s"
                    )

                    last_batch_time = time.time()
                    yield images, labels

                except Exception as e:
                    logger.error(
                        f"[DataLoader] Worker {worker_rank} error processing batch {batch_idx}: {str(e)}",
                        exc_info=True,
                    )
                    raise

        except Exception as e:
            logger.error(
                f"[DataLoader] Worker {worker_rank} error in batch iterator: {str(e)}",
                exc_info=True,
            )
            raise

    def get_train_dataloader(self) -> Iterator[Tuple[torch.Tensor, torch.Tensor]]:
        """Create a DataLoader for training data.

        Returns:
            An iterator that yields (image, label) tensors for training
        """
        worker_rank = ray.train.get_context().get_world_rank()
        logger.info(f"[DataLoader] Worker {worker_rank} creating train dataloader")

        dataloader_config = self.get_dataloader_config()
        device = self._get_device()

        # Create dataset and dataloader
        train_ds = S3ParquetImageIterableDataset(
            file_urls=self._get_file_urls(self.train_url),
            random_transforms=True,
        )

        # Adjust worker settings for 0 workers case
        num_workers = max(0, self.num_torch_workers)
        persistent_workers = num_workers > 0
        pin_memory = torch.cuda.is_available()  # Always pin memory if CUDA is available

        dataloader = torch.utils.data.DataLoader(
            dataset=train_ds,
            batch_size=dataloader_config.train_batch_size,
            num_workers=num_workers,
            pin_memory=pin_memory,
            persistent_workers=persistent_workers,
            prefetch_factor=dataloader_config.prefetch_batches
            if num_workers > 0
            else 2,
            timeout=self.benchmark_config.torch_dataloader_timeout_seconds,
            drop_last=True,
            worker_init_fn=self.worker_init_fn if num_workers > 0 else None,
        )

        return self._create_batch_iterator(dataloader, device)

    def get_val_dataloader(self) -> Iterator[Tuple[torch.Tensor, torch.Tensor]]:
        """Create a DataLoader for validation data.

        Returns:
            An iterator that yields (image, label) tensors for validation
        """
        worker_rank = ray.train.get_context().get_world_rank()
        logger.info(f"[DataLoader] Worker {worker_rank} creating validation dataloader")

        dataloader_config = self.get_dataloader_config()
        device = self._get_device()

        # Create dataset and dataloader with row limits
        val_ds = S3ParquetImageIterableDataset(
            file_urls=self._get_file_urls(self.val_url),
            random_transforms=False,
            limit_rows_per_worker=self.limit_rows_per_worker,
        )

        # Adjust worker settings for 0 workers case
        num_workers = max(0, self.num_torch_workers)
        persistent_workers = num_workers > 0
        pin_memory = torch.cuda.is_available()  # Always pin memory if CUDA is available

        dataloader = torch.utils.data.DataLoader(
            dataset=val_ds,
            batch_size=dataloader_config.validation_batch_size,
            num_workers=num_workers,
            pin_memory=pin_memory,
            persistent_workers=persistent_workers,
            prefetch_factor=dataloader_config.prefetch_batches
            if num_workers > 0
            else 2,
            timeout=self.benchmark_config.torch_dataloader_timeout_seconds,
            drop_last=False,
            worker_init_fn=self.worker_init_fn if num_workers > 0 else None,
        )

        return self._create_batch_iterator(dataloader, device)


class RayDataLoaderFactory(BaseDataLoaderFactory):
    def __init__(self, benchmark_config: BenchmarkConfig) -> None:
        super().__init__(benchmark_config)
        self._ray_ds_iterators = {}

        assert isinstance(self.get_dataloader_config(), RayDataConfig), type(
            self.get_dataloader_config()
        )

        # Configure Ray Data settings.
        data_context = ray.data.DataContext.get_current()
        data_context.enable_operator_progress_bars = False

    @abstractmethod
    def get_ray_datasets(self) -> Dict[str, Dataset]:
        """Get the Ray datasets for training and validation.

        Returns:
            Dict with "train" and "val" Dataset objects
        """
        pass

    @abstractmethod
    def collate_fn(self, batch: Dict[str, Any]) -> Tuple[torch.Tensor, torch.Tensor]:
        """Get the collate function for the dataloader.

        Returns:
            A function that takes a batch and returns a tuple of tensors.
        """
        pass

    def get_train_dataloader(self):
        ds_iterator = self._ray_ds_iterators["train"] = ray.train.get_dataset_shard(
            "train"
        )
        dataloader_config = self.get_dataloader_config()
        return iter(
            ds_iterator.iter_torch_batches(
                batch_size=dataloader_config.train_batch_size,
                local_shuffle_buffer_size=(
                    dataloader_config.local_buffer_shuffle_size
                    if dataloader_config.local_buffer_shuffle_size > 0
                    else None
                ),
                collate_fn=self.collate_fn,
                prefetch_batches=dataloader_config.prefetch_batches,
            )
        )

    def get_val_dataloader(self):
        ds_iterator = self._ray_ds_iterators["val"] = ray.train.get_dataset_shard("val")
        dataloader_config = self.get_dataloader_config()
        return iter(
            ds_iterator.iter_torch_batches(
                batch_size=dataloader_config.validation_batch_size,
                collate_fn=self.collate_fn,
                prefetch_batches=dataloader_config.prefetch_batches,
            )
        )

    def get_metrics(self) -> Dict[str, Any]:
        metrics = {}
        for ds_key, ds_iterator in self._ray_ds_iterators.items():
            stats = ray.get(ds_iterator._coord_actor.stats.remote())
            summary = stats.to_summary()
            summary.iter_stats = ds_iterator._iter_stats.to_summary().iter_stats
            summary.iter_stats.streaming_split_coord_time.add(
                stats.streaming_split_coordinator_s.get()
            )

            if not summary.parents:
                continue

            # The split() operator has no metrics, so pull the stats
            # from the final dataset stage.
            ds_output_summary = summary.parents[0]
            ds_throughput = (
                ds_output_summary.operators_stats[-1].output_num_rows["sum"]
                / ds_output_summary.get_total_wall_time()
            )

            iter_stats = summary.iter_stats

            metrics[f"dataloader/{ds_key}"] = {
                "producer_throughput": ds_throughput,
                "iter_stats": {
                    "prefetch_block-avg": iter_stats.wait_time.avg(),
                    "prefetch_block-min": iter_stats.wait_time.min(),
                    "prefetch_block-max": iter_stats.wait_time.max(),
                    "prefetch_block-total": iter_stats.wait_time.get(),
                    "fetch_block-avg": iter_stats.get_time.avg(),
                    "fetch_block-min": iter_stats.get_time.min(),
                    "fetch_block-max": iter_stats.get_time.max(),
                    "fetch_block-total": iter_stats.get_time.get(),
                    "block_to_batch-avg": iter_stats.next_time.avg(),
                    "block_to_batch-min": iter_stats.next_time.min(),
                    "block_to_batch-max": iter_stats.next_time.max(),
                    "block_to_batch-total": iter_stats.next_time.get(),
                    "format_batch-avg": iter_stats.format_time.avg(),
                    "format_batch-min": iter_stats.format_time.min(),
                    "format_batch-max": iter_stats.format_time.max(),
                    "format_batch-total": iter_stats.format_time.get(),
                    "collate-avg": iter_stats.collate_time.avg(),
                    "collate-min": iter_stats.collate_time.min(),
                    "collate-max": iter_stats.collate_time.max(),
                    "collate-total": iter_stats.collate_time.get(),
                    "finalize-avg": iter_stats.finalize_batch_time.avg(),
                    "finalize-min": iter_stats.finalize_batch_time.min(),
                    "finalize-max": iter_stats.finalize_batch_time.max(),
                    "finalize-total": iter_stats.finalize_batch_time.get(),
                    "time_spent_blocked-avg": iter_stats.block_time.avg(),
                    "time_spent_blocked-min": iter_stats.block_time.min(),
                    "time_spent_blocked-max": iter_stats.block_time.max(),
                    "time_spent_blocked-total": iter_stats.block_time.get(),
                    "time_spent_training-avg": iter_stats.user_time.avg(),
                    "time_spent_training-min": iter_stats.user_time.min(),
                    "time_spent_training-max": iter_stats.user_time.max(),
                    "time_spent_training-total": iter_stats.user_time.get(),
                },
            }
        return metrics
