from typing import Iterator, List, Optional, Tuple
import time
import logging
import io
import pandas as pd
import pyarrow.parquet as pq
import multiprocessing
import boto3
from botocore.exceptions import NoCredentialsError

import torch
from torch.utils.data import IterableDataset

import ray.train
import ray

from config import BenchmarkConfig
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


@ray.remote
def _fetch_parquet_metadata(bucket: str, key: str, file_url: str) -> Tuple[str, int]:
    """Standalone Ray task to fetch Parquet metadata.

    Args:
        bucket: S3 bucket name
        key: S3 object key
        file_url: Full S3 URL for logging

    Returns:
        Tuple of (file_url, row_count)
    """
    import boto3
    import pyarrow.parquet as pq
    import io
    import logging

    logger = logging.getLogger(__name__)
    s3_client = boto3.client("s3", region_name=AWS_REGION)

    # Step 1: Read the last 8 bytes to get the footer size
    response = s3_client.get_object(
        Bucket=bucket, Key=key, Range="bytes=-8"  # Read only last 8 bytes
    )
    footer_bytes = response["Body"].read()

    # Extract footer size (last 4 bytes contain the size)
    footer_size = int.from_bytes(footer_bytes[-4:], byteorder="little")

    # Step 2: Read the Parquet metadata footer
    metadata_range = (
        f"bytes=-{footer_size + 8}"  # Include footer + 8-byte metadata indicator
    )
    response = s3_client.get_object(Bucket=bucket, Key=key, Range=metadata_range)

    # Step 3: Parse Parquet metadata
    metadata = pq.read_metadata(io.BytesIO(response["Body"].read()))
    total_rows = metadata.num_rows

    logger.info(f"[DataLoader] File {file_url} has {total_rows} rows")
    return file_url, total_rows


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

    def __init__(self, benchmark_config: Optional[BenchmarkConfig] = None):
        """Initialize the S3Reader.

        Args:
            benchmark_config: Optional benchmark configuration
        """
        self._s3_client = None
        self.benchmark_config = benchmark_config
        self.worker_rank = ray.train.get_context().get_world_rank()

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

    def _get_parquet_metadata(self, bucket: str, key: str) -> pq.FileMetaData:
        """Fetch Parquet metadata efficiently by reading only necessary bytes.

        Args:
            bucket: S3 bucket name
            key: S3 object key

        Returns:
            PyArrow FileMetaData object
        """
        # Step 1: Read the last 8 bytes to get the footer size
        response = self.s3_client.get_object(
            Bucket=bucket, Key=key, Range="bytes=-8"  # Read only last 8 bytes
        )
        footer_bytes = response["Body"].read()

        # Extract footer size (last 4 bytes contain the size)
        footer_size = int.from_bytes(footer_bytes[-4:], byteorder="little")

        # Step 2: Read the Parquet metadata footer
        metadata_range = (
            f"bytes=-{footer_size + 8}"  # Include footer + 8-byte metadata indicator
        )
        response = self.s3_client.get_object(
            Bucket=bucket, Key=key, Range=metadata_range
        )

        # Step 3: Parse Parquet metadata
        metadata = pq.read_metadata(io.BytesIO(response["Body"].read()))
        return metadata

    def _collect_file_info(
        self, bucket: str, prefix: str
    ) -> Tuple[List[str], List[int]]:
        """Collect file URLs and their row counts in parallel using Ray tasks.

        Args:
            bucket: S3 bucket name
            prefix: S3 prefix to list

        Returns:
            Tuple of (file_urls, row_counts)
        """
        response = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

        if "Contents" not in response:
            return [], []

        # Launch parallel tasks for each file
        tasks = []
        for file in response["Contents"]:
            file_url = f"s3://{bucket}/{file['Key']}"
            task = _fetch_parquet_metadata.remote(bucket, file["Key"], file_url)
            tasks.append(task)

        # Wait for all tasks to complete
        logger.info(
            f"[DataLoader] Worker {self.worker_rank}: Waiting for metadata from {len(tasks)} files..."
        )
        results = ray.get(tasks)

        # Unzip results
        file_urls, file_rows = zip(*results) if results else ([], [])

        logger.info(
            f"[DataLoader] Worker {self.worker_rank}: Collected metadata for {len(file_urls)} files"
        )
        return list(file_urls), list(file_rows)

    def _distribute_files(
        self,
        file_urls: List[str],
        file_rows: List[int],
        worker_rank: int,
        num_workers: int,
    ) -> List[str]:
        """Distribute files among workers based on row counts.

        Args:
            file_urls: List of file URLs
            file_rows: List of row counts per file
            worker_rank: Current worker rank
            num_workers: Total number of workers

        Returns:
            List of file URLs assigned to this worker
        """
        # Sort files by row count (descending) for better distribution
        files_with_rows = sorted(
            zip(file_urls, file_rows), key=lambda x: x[1], reverse=True
        )
        file_urls = [f[0] for f in files_with_rows]
        file_rows = [f[1] for f in files_with_rows]

        # If no workers or files, return all files
        if num_workers <= 1 or not file_urls:
            logger.info(
                f"[DataLoader] Worker {worker_rank}: Single worker or no files, returning all {len(file_urls)} files "
                f"with total {sum(file_rows)} rows"
            )
            return file_urls

        # Calculate target rows per worker for balanced distribution
        total_rows = sum(file_rows)
        target_rows_per_worker = total_rows / num_workers
        logger.info(
            f"[DataLoader] Worker {worker_rank}: Total rows: {total_rows}, Target rows per worker: {target_rows_per_worker:.0f}"
        )

        # Distribute files to minimize row count differences between workers
        worker_files = [[] for _ in range(num_workers)]
        worker_rows = [0] * num_workers

        # Assign files to workers, trying to keep row counts balanced
        for file_url, row_count in zip(file_urls, file_rows):
            # Find worker with minimum current row count
            min_rows_worker = min(range(num_workers), key=lambda w: worker_rows[w])
            worker_files[min_rows_worker].append(file_url)
            worker_rows[min_rows_worker] += row_count

        # Get this worker's files
        my_files = worker_files[worker_rank]
        my_row_count = worker_rows[worker_rank]

        logger.info(
            f"[DataLoader] Worker {worker_rank}: Assigned {len(my_files)}/{len(file_urls)} files "
            f"with {my_row_count}/{total_rows} rows "
            f"({my_row_count/total_rows*100:.1f}% of total)"
        )
        return my_files

    def _get_file_urls(self, url: str) -> List[str]:
        """Get file URLs from S3 and distribute them among Ray workers based on row counts.

        Args:
            url: The S3 URL to list files from

        Returns:
            List of S3 URLs assigned to the current Ray worker
        """
        try:
            # Get Ray worker info
            worker_rank = ray.train.get_context().get_world_rank()
            num_workers = ray.train.get_context().get_world_size()
            logger.info(
                f"[DataLoader] Worker {worker_rank}/{num_workers}: Listing files"
            )

            # List files and get row counts
            bucket, prefix = self._parse_s3_url(url)
            file_urls, file_rows = self._collect_file_info(bucket, prefix)

            # Distribute files among workers
            return self._distribute_files(
                file_urls, file_rows, worker_rank, num_workers
            )

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
        super().__init__()
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
            logger.info(
                f"[Dataset] Worker {self.worker_rank}: Reading Parquet file: {file_url}"
            )

            # Get parquet file metadata
            bucket, key = self._parse_s3_url(file_url)
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            parquet_file = pq.ParquetFile(io.BytesIO(response["Body"].read()))
            num_row_groups = parquet_file.num_row_groups

            logger.info(
                f"[Dataset] Worker {self.worker_rank}: Found {num_row_groups} row groups in {file_url}"
            )

            for row_group in range(num_row_groups):
                # Read row group and convert to pandas
                table = parquet_file.read_row_group(row_group)
                df = table.to_pandas()
                yield df

            total_time = time.time() - start_time
            logger.info(
                f"[Dataset] Worker {self.worker_rank}: Completed reading {file_url} in {total_time:.2f}s"
            )

        except Exception as e:
            logger.error(
                f"[Dataset] Worker {self.worker_rank}: Error reading file {file_url}: {str(e)}"
            )
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

            logger.info(f"[Dataset] Worker {worker_id}/{num_workers}: Starting")

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
                f"[Dataset] Worker {worker_id}: Processing {len(files_to_read)} files"
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
                        f"[Dataset] Worker {worker_id}: Reached row limit: {rows_processed}"
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
                                f"[Dataset] Worker {worker_id}: Error processing row: {str(e)}"
                            )
                            continue

            # Log final statistics
            total_time = time.time() - total_start_time
            logger.info(
                f"[Dataset] Worker {worker_id}: Finished: "
                f"{rows_processed} rows in {total_time:.2f}s "
                f"({rows_processed/total_time:.2f} rows/sec)"
            )

        except Exception as e:
            logger.error(
                f"[Dataset] Worker {worker_id}: Fatal error: {str(e)}", exc_info=True
            )
            raise
