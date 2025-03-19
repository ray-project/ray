from abc import ABC, abstractmethod
from typing import Any, Dict, Iterator, Tuple, List, Optional
import time
import multiprocessing
import logging

import torch
import boto3
import pandas as pd
from botocore.exceptions import NoCredentialsError
import io
import os
from torch.utils.data import IterableDataset

import ray.data
import ray.train
from ray.train import torch as ray_train_torch
from ray.data import Dataset

from config import BenchmarkConfig, DataLoaderConfig, RayDataConfig
from image_classification.imagenet import (
    get_preprocess_map_fn,
)

logger = logging.getLogger(__name__)

# Set multiprocessing start method to 'spawn' for CUDA compatibility
if torch.cuda.is_available():
    try:
        multiprocessing.set_start_method('spawn', force=True)
        logger.info("[DataLoader] Set multiprocessing start method to 'spawn' for CUDA compatibility")
    except RuntimeError:
        logger.info("[DataLoader] Multiprocessing start method already set")

# Configure logging for worker processes
def configure_worker_logging():
    """Configure logging for DataLoader worker processes."""
    # Get the current worker info
    worker_info = torch.utils.data.get_worker_info()
    if worker_info is not None:
        # Configure logging for this worker
        worker_id = worker_info.id
        num_workers = worker_info.num_workers
        
        # Create a new logger for this worker
        worker_logger = logging.getLogger(f"DataLoader.Worker{worker_id}")
        worker_logger.setLevel(logging.INFO)
        
        # Create a handler that includes worker ID in the log message
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            f'%(asctime)s [DataLoader.Worker{worker_id}/{num_workers}] %(levelname)s: %(message)s'
        )
        handler.setFormatter(formatter)
        worker_logger.addHandler(handler)
        
        # Return the worker-specific logger
        return worker_logger
    return logger

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
            self._s3_client = boto3.client(
                "s3",
                region_name=AWS_REGION
            )
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

    def list_files(self, s3_url: str) -> List[str]:
        """List all files in the S3 bucket with a specific prefix.

        Args:
            s3_url: The S3 URL to list files from

        Returns:
            List[str]: List of S3 URLs for all files found

        Raises:
            S3CredentialsError: If AWS credentials are not found
            S3FileError: If there's an error listing files
            ValueError: If the S3 URL is invalid
        """
        try:
            bucket, prefix = self._parse_s3_url(s3_url)
            file_urls = []
            response = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

            if "Contents" in response:
                for file in response["Contents"]:
                    file_urls.append(f"s3://{bucket}/{file['Key']}")

            return file_urls
        except NoCredentialsError:
            raise self.S3CredentialsError("AWS credentials not found. Ensure you have configured them.")
        except Exception as e:
            raise self.S3FileError(f"Error listing files from {s3_url}: {str(e)}")


class S3ParquetReader(S3Reader):
    """A class to handle reading Parquet files from S3."""

    class S3ParquetError(S3Reader.S3FileError):
        """Raised when there's an error reading a Parquet file from S3."""
        pass

    def read_parquet(self, s3_url: str, row_group: Optional[int] = None) -> pd.DataFrame:
        """Download Parquet file from S3 and return as Pandas DataFrame.

        Args:
            s3_url: The S3 URL of the Parquet file
            row_group: Specific row group to read (None reads all groups)

        Returns:
            pd.DataFrame: The loaded Parquet file as a DataFrame

        Raises:
            S3CredentialsError: If AWS credentials are not found
            S3ParquetError: If there's an error reading the Parquet file
            ValueError: If the S3 URL is invalid
        """
        try:
            import pyarrow.parquet as pq
            parquet_data = self.read_file(s3_url)
            parquet_file = pq.ParquetFile(io.BytesIO(parquet_data))
            
            if row_group is not None:
                # Read specific row group using PyArrow
                table = parquet_file.read_row_group(row_group)
            else:
                # Read all row groups
                table = parquet_file.read()
                
            # Convert PyArrow table to pandas DataFrame
            return table.to_pandas()
            
        except self.S3FileError as e:
            raise self.S3ParquetError(str(e))
        except Exception as e:
            raise self.S3ParquetError(
                f"Error loading Parquet file from {s3_url}: {str(e)}"
            )

    def get_num_row_groups(self, s3_url: str) -> int:
        """Get the number of row groups in a Parquet file.

        Args:
            s3_url: The S3 URL of the Parquet file

        Returns:
            int: Number of row groups in the file
        """
        try:
            import pyarrow.parquet as pq
            parquet_data = self.read_file(s3_url)
            parquet_file = pq.ParquetFile(io.BytesIO(parquet_data))
            return parquet_file.num_row_groups
        except Exception as e:
            raise self.S3ParquetError(
                f"Error getting row groups from {s3_url}: {str(e)}"
            )


class S3ParquetImageIterableDataset(S3Reader, IterableDataset):
    """An iterable dataset that loads images from S3-stored Parquet files."""

    LOG_FREQUENCY = 1000  # Log every 1000 rows

    def __init__(
        self,
        file_urls: List[str],
        random_transforms: bool = True,
        limit_rows_per_worker: Optional[int] = None,
        device: Optional[torch.device] = None,
    ):
        """Initialize the dataset.

        Args:
            file_urls: List of S3 URLs to load
            random_transforms: Whether to use random transforms for training
            limit_rows_per_worker: Maximum number of rows to process per worker (None for all rows)
                                 The caller should divide the total desired limit by num_workers
            device: Device to place tensors on (default: None, stays on CPU)
        """
        S3Reader.__init__(self)  # Initialize the S3Reader parent class
        self.file_urls = file_urls
        self.limit_rows_per_worker = limit_rows_per_worker
        self.random_transforms = random_transforms
        self.device = device
        self.logger = configure_worker_logging()  # Get worker-specific logger

    def _read_parquet_file(self, file_url: str) -> Iterator[pd.DataFrame]:
        """Read a Parquet file from S3 one row group at a time."""
        try:
            start_time = time.time()
            self.logger.info(f"[S3ParquetImageIterableDataset] Getting row groups for {file_url}")
            
            # Create S3 client inline
            import boto3
            s3_client = boto3.client("s3", region_name=AWS_REGION)
            
            # Get parquet file metadata
            import pyarrow.parquet as pq
            bucket, key = self._parse_s3_url(file_url)
            response = s3_client.get_object(Bucket=bucket, Key=key)
            parquet_data = response["Body"].read()
            parquet_file = pq.ParquetFile(io.BytesIO(parquet_data))
            num_row_groups = parquet_file.num_row_groups
            
            self.logger.info(f"[S3ParquetImageIterableDataset] Found {num_row_groups} row groups in {file_url}")
            
            for row_group in range(num_row_groups):
                try:
                    group_start_time = time.time()
                    # Get row group metadata
                    row_group_metadata = parquet_file.metadata.row_group(row_group)
                    num_rows = row_group_metadata.num_rows
                    
                    self.logger.info(f"[S3ParquetImageIterableDataset] Reading row group {row_group}/{num_row_groups} ({num_rows} rows) from {file_url}")
                    
                    # Read row group and convert to pandas
                    table = parquet_file.read_row_group(row_group)
                    df = table.to_pandas()
                    
                    group_time = time.time() - group_start_time
                    self.logger.info(f"[S3ParquetImageIterableDataset] Row group {row_group} read in {group_time:.2f}s ({num_rows/group_time:.2f} rows/sec)")
                    yield df
                    
                except Exception as e:
                    self.logger.error(f"[S3ParquetImageIterableDataset] Error processing row group {row_group} from {file_url}: {str(e)}")
                    continue
                    
            total_time = time.time() - start_time
            self.logger.info(f"[S3ParquetImageIterableDataset] Completed reading {file_url} in {total_time:.2f}s")
                    
        except Exception as e:
            self.logger.error(f"[S3ParquetImageIterableDataset] Error reading file {file_url}: {str(e)}")
            raise

    def __iter__(self) -> Iterator[Tuple[torch.Tensor, torch.Tensor]]:
        try:
            worker_info = torch.utils.data.get_worker_info()
            worker_id = worker_info.id if worker_info else 0
            num_workers = worker_info.num_workers if worker_info else 1
            
            self.logger.info(
                f"Starting iteration"
            )
            self.logger.info(
                f"Configuration: random_transforms={self.random_transforms}, "
                f"limit_rows_per_worker={self.limit_rows_per_worker}, "
                f"device={self.device}"
            )
            rows_processed = 0  # Local to this worker
            last_log_time = time.time()
            total_start_time = time.time()
            last_row_time = time.time()

            try:
                preprocess_start = time.time()
                preprocess_fn = get_preprocess_map_fn(
                    decode_image=True,
                    random_transforms=self.random_transforms
                )
                preprocess_time = time.time() - preprocess_start
                self.logger.info(f"Created preprocess_fn in {preprocess_time:.2f}s")
            except Exception as e:
                self.logger.error(f"Error creating preprocess_fn", exc_info=True)
                raise

            if worker_info is None:
                files_to_read = self.file_urls
            else:
                per_worker = max(1, len(self.file_urls) // num_workers)
                start_idx = worker_id * per_worker
                end_idx = start_idx + per_worker if worker_id < num_workers - 1 else len(self.file_urls)
                files_to_read = self.file_urls[start_idx:end_idx]

            self.logger.info(
                f"Processing {len(files_to_read)} files: {files_to_read}"
            )

            for file_url in files_to_read:
                if self.limit_rows_per_worker is not None and rows_processed >= self.limit_rows_per_worker:
                    self.logger.info(
                        f"Reached row limit {self.limit_rows_per_worker}, "
                        f"processed {rows_processed} rows"
                    )
                    return

                file_start_time = time.time()
                self.logger.info(
                    f"Starting to read {file_url}"
                )
                for df in self._read_parquet_file(file_url):
                    if self.limit_rows_per_worker is not None and rows_processed >= self.limit_rows_per_worker:
                        self.logger.info(
                            f"Reached row limit {self.limit_rows_per_worker}, "
                            f"processed {rows_processed} rows"
                        )
                        return

                    for _, row in df.iterrows():
                        if self.limit_rows_per_worker is not None and rows_processed >= self.limit_rows_per_worker:
                            self.logger.info(
                                f"Reached row limit {self.limit_rows_per_worker}, "
                                f"processed {rows_processed} rows"
                            )
                            return

                        try:
                            # Check if we're taking too long between rows
                            current_time = time.time()
                            time_since_last_row = current_time - last_row_time
                            if time_since_last_row > 5:  # Log warning if more than 5 seconds between rows
                                self.logger.warning(
                                    f"Long delay ({time_since_last_row:.2f}s) between rows {rows_processed-1} and {rows_processed}"
                                )
                            
                            row_start_time = time.time()
                            processed = preprocess_fn(row)
                            preprocess_time = time.time() - row_start_time
                            
                            if preprocess_time > 2:  # Log warning if preprocessing takes too long
                                self.logger.warning(
                                    f"Slow preprocessing ({preprocess_time:.2f}s) for row {rows_processed}"
                                )
                            
                            # Convert to tensors and optionally move to device
                            tensor_start_time = time.time()
                            image = torch.as_tensor(processed["image"], dtype=torch.float32)
                            label = torch.as_tensor(processed["label"], dtype=torch.int64)
                            if self.device is not None:
                                image = image.to(self.device, non_blocking=True)
                                label = label.to(self.device, non_blocking=True)
                            tensor_time = time.time() - tensor_start_time
                            
                            if tensor_time > 2:  # Log warning if tensor conversion takes too long
                                self.logger.warning(
                                    f"Slow tensor conversion ({tensor_time:.2f}s) for row {rows_processed}"
                                )
                            
                            rows_processed += 1
                            last_row_time = time.time()

                            # Log progress periodically
                            if rows_processed % self.LOG_FREQUENCY == 0:
                                current_time = time.time()
                                elapsed_time = current_time - last_log_time
                                rows_per_second = self.LOG_FREQUENCY / elapsed_time if elapsed_time > 0 else 0
                                self.logger.info(
                                    f"Processed {rows_processed} rows ({rows_per_second:.2f} rows/sec)"
                                )
                                last_log_time = current_time

                            yield image, label

                        except Exception as e:
                            self.logger.error(f"Error processing row", exc_info=True)
                            continue

                file_time = time.time() - file_start_time
                self.logger.info(
                    f"Completed file {file_url} in {file_time:.2f}s"
                )

            total_time = time.time() - total_start_time
            self.logger.info(
                f"Finished processing {rows_processed} rows in {total_time:.2f}s "
                f"({rows_processed/total_time:.2f} rows/sec)"
            )
        except Exception as e:
            self.logger.error(f"Fatal error", exc_info=True)
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


class TorchDataLoaderFactory(BaseDataLoaderFactory):
    """Factory for creating PyTorch DataLoaders that read from S3 parquet files."""

    def __init__(
        self, 
        benchmark_config: BenchmarkConfig, 
        train_url: str, 
        val_url: str,
        limit_total_rows: Optional[int] = None
    ):
        super().__init__(benchmark_config)
        self.train_url = train_url
        self.val_url = val_url
        self._iterator = None
        
        # Calculate number of torch workers based on CPUs per GPU
        num_gpus = torch.cuda.device_count() if torch.cuda.is_available() else 1
        num_cpus = os.cpu_count() or 1
        self.num_torch_workers = max(1, num_cpus // num_gpus)  # At least 1 worker
        logger.info(
            f"[DataLoader] Workers: {self.num_torch_workers} per GPU "
            f"(CPUs: {num_cpus}, GPUs: {num_gpus})"
        )
        
        # Calculate per-worker row limit based on benchmark config workers
        self.num_ray_workers = benchmark_config.num_workers
        total_workers = self.num_ray_workers * self.num_torch_workers
        self.limit_rows_per_worker = (
            max(1, limit_total_rows // total_workers) if limit_total_rows is not None else None
        )
        logger.info(
            f"[DataLoader] Total workers: {total_workers} "
            f"({self.num_ray_workers} Ray × {self.num_torch_workers} Torch)"
        )
        if limit_total_rows is not None:
            if limit_total_rows < total_workers:
                logger.warning(
                    f"[DataLoader] Warning: limit_total_rows ({limit_total_rows}) is less than "
                    f"total_workers ({total_workers}). This means each worker will process at least "
                    f"1 row instead of 0."
                )
            logger.info(f"[DataLoader] Rows per worker: {self.limit_rows_per_worker}")

    def _get_device(self) -> torch.device:
        """Get the device for the current worker using Ray Train's device management."""
        device = ray.train.torch.get_device()
        logger.info(f"[DataLoader] Using Ray Train device: {device}")
        return device

    def _parse_s3_url(self, s3_url: str) -> Tuple[str, str]:
        """Parse an S3 URL into bucket and key."""
        if s3_url.startswith("s3://"):
            s3_parts = s3_url.replace("s3://", "").split("/", 1)
            return s3_parts[0], s3_parts[1]
        else:
            raise ValueError(f"Invalid S3 URL format: {s3_url}")

    def _get_file_urls(self, url: str, max_files_per_worker: Optional[int] = None) -> List[str]:
        """Get all file URLs from the given S3 URL.
        
        Args:
            url: The S3 URL to list files from
            max_files_per_worker: Optional limit on files per worker (applied before distribution)
        """
        try:
            # Create S3 client inline
            import boto3
            s3_client = boto3.client("s3", region_name=AWS_REGION)
            
            # Get Ray worker info
            worker_rank = ray.train.get_context().get_world_rank()
            logger.info(f"[TorchDataLoaderFactory] Ray worker {worker_rank}/{self.num_ray_workers} listing files")
            
            # List files
            bucket, prefix = self._parse_s3_url(url)
            file_urls = []
            response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
            
            if "Contents" in response:
                for file in response["Contents"]:
                    file_urls.append(f"s3://{bucket}/{file['Key']}")
            
            # Sort URLs to ensure consistent ordering across workers
            file_urls.sort()

            # If max_files_per_worker is set, limit total files before distribution
            if max_files_per_worker is not None:
                total_files = max_files_per_worker * self.num_ray_workers
                file_urls = file_urls[:total_files]
            
            # True round-robin allocation: each worker gets every Nth file
            worker_urls = file_urls[worker_rank::self.num_ray_workers]
            
            logger.info(f"[TorchDataLoaderFactory] Ray worker {worker_rank} got {len(worker_urls)}/{len(file_urls)} files in round-robin distribution")
            return worker_urls
            
        except Exception as e:
            logger.error(f"[TorchDataLoaderFactory] Error listing files from {url}: {str(e)}")
            raise

    def _create_batch_iterator(self, dataloader: torch.utils.data.DataLoader, device: torch.device) -> Iterator[Tuple[torch.Tensor, torch.Tensor]]:
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
                current_time = time.time()
                time_since_last_batch = current_time - last_batch_time
                
                if time_since_last_batch > 10:  # Log warning if more than 10 seconds between batches
                    logger.warning(
                        f"[DataLoader] Worker {worker_rank}: "
                        f"Long delay ({time_since_last_batch:.2f}s) between batches {batch_idx-1} and {batch_idx}"
                    )
                
                logger.info(
                    f"[DataLoader] Worker {worker_rank} got batch {batch_idx} "
                    f"(time since last: {time_since_last_batch:.2f}s)"
                )
                
                try:
                    images, labels = batch
                    logger.info(f"[DataLoader] Worker {worker_rank} moving batch to {device} (shape: {images.shape})")
                    
                    # Add timeout for device transfer
                    transfer_start = time.time()
                    images = images.to(device, non_blocking=True)
                    labels = labels.to(device, non_blocking=True)
                    transfer_time = time.time() - transfer_start
                    
                    if transfer_time > 5:  # Log warning if transfer takes too long
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
                        exc_info=True
                    )
                    raise
                    
        except Exception as e:
            logger.error(f"[DataLoader] Worker {worker_rank} error in batch iterator: {str(e)}", exc_info=True)
            raise

    def get_train_dataloader(self) -> Iterator[Tuple[torch.Tensor, torch.Tensor]]:
        worker_rank = ray.train.get_context().get_world_rank()
        logger.info(f"[DataLoader] Worker {worker_rank} starting train dataloader creation")
        dataloader_config = self.get_dataloader_config()
        device = self._get_device()

        logger.info(f"[DataLoader] Worker {worker_rank} getting file URLs")
        train_ds = S3ParquetImageIterableDataset(
            file_urls=self._get_file_urls(self.train_url),
            random_transforms=True,
            limit_rows_per_worker=self.limit_rows_per_worker * 2,
            device=None,  # Keep tensors on CPU in dataset
        )

        logger.info(f"[DataLoader] Worker {worker_rank} creating DataLoader")
        dataloader = torch.utils.data.DataLoader(
            dataset=train_ds,
            batch_size=dataloader_config.train_batch_size,
            num_workers=self.num_torch_workers,
            pin_memory=True,  # Enable pinned memory for faster CPU->GPU transfer
            persistent_workers=True,
            multiprocessing_context='spawn',  # Explicitly set spawn context
            # TODO: Adding prefetch factor causes the dataloader to OOM
            # prefetch_factor=dataloader_config.prefetch_batches,
            timeout=30,  # 30 second timeout for collecting batches
            drop_last=True,
        )
        
        return self._create_batch_iterator(dataloader, device)

    def get_val_dataloader(self) -> Iterator[Tuple[torch.Tensor, torch.Tensor]]:
        worker_rank = ray.train.get_context().get_world_rank()
        logger.info(f"[DataLoader] Worker {worker_rank} starting validation dataloader creation")
        dataloader_config = self.get_dataloader_config()
        device = self._get_device()

        # Limit validation dataset size to prevent timeouts
        val_ds = S3ParquetImageIterableDataset(
            file_urls=self._get_file_urls(self.val_url, max_files_per_worker=2),  # Limit to 2 files per worker
            random_transforms=False,
            limit_rows_per_worker=self.limit_rows_per_worker,
            device=None,  # Keep tensors on CPU in dataset
        )

        dataloader = torch.utils.data.DataLoader(
            dataset=val_ds,
            batch_size=dataloader_config.validation_batch_size,
            num_workers=min(4, self.num_torch_workers),  # Limit number of workers for validation
            pin_memory=True,  # Enable pinned memory for faster CPU->GPU transfer
            persistent_workers=True,
            multiprocessing_context='spawn',  # Explicitly set spawn context
            # TODO: Adding prefetch factor causes the dataloader to OOM
            # prefetch_factor=dataloader_config.prefetch_batches,
            timeout=30,  # 30 second timeout for collecting batches
            drop_last=False,  # Don't drop incomplete batches during validation
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