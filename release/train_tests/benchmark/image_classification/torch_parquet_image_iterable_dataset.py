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

# TODO Look into https://github.com/webdataset/webdataset for more canonical way to do data
# distribution between Ray Train and Torch Dataloader workers.


@ray.remote(num_cpus=0.25)
def _fetch_parquet_metadata(bucket: str, key: str, file_url: str) -> Tuple[str, int]:
    """Standalone Ray task to fetch Parquet row count using S3 Select.

    Args:
        bucket: S3 bucket name
        key: S3 object key
        file_url: Full S3 URL for logging

    Returns:
        Tuple of (file_url, row_count)
    """
    import boto3
    import logging

    logger = logging.getLogger(__name__)
    s3_client = boto3.client("s3", region_name=AWS_REGION)

    # Use S3 Select to count rows
    query = "SELECT COUNT(*) FROM s3object"
    response = s3_client.select_object_content(
        Bucket=bucket,
        Key=key,
        Expression=query,
        ExpressionType="SQL",
        InputSerialization={"Parquet": {}},
        OutputSerialization={"CSV": {}},
    )

    # Parse the response to get row count
    row_count = 0
    for event in response["Payload"]:
        if "Records" in event:
            row_count = int(event["Records"]["Payload"].decode("utf-8").strip())

    logger.info(f"[DataLoader] File {file_url} has {row_count} rows")
    return file_url, row_count


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
        worker_rank = ray.train.get_context().get_world_rank()
        logger.info(
            f"[DataLoader] Worker {worker_rank}: Waiting for metadata from {len(tasks)} files..."
        )
        results = ray.get(tasks)

        # Unzip results
        file_urls, file_rows = zip(*results) if results else ([], [])

        logger.info(
            f"[DataLoader] Worker {worker_rank}: Collected metadata for {len(file_urls)} files"
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

    def _list_s3_files(self, bucket: str, prefix: str) -> List[str]:
        """List files in an S3 bucket with the given prefix.

        Args:
            bucket: S3 bucket name
            prefix: S3 prefix to list

        Returns:
            List of S3 URLs for the files
        """
        response = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        if "Contents" not in response:
            logger.warning(f"[DataLoader] No files found in s3://{bucket}/{prefix}")
            return []

        return [f"s3://{bucket}/{file['Key']}" for file in response["Contents"]]

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
            num_workers = ray.train.get_context().get_world_size()
            logger.info(
                f"[DataLoader] Worker {worker_rank}/{num_workers}: Listing files"
            )

            # List files
            bucket, prefix = self._parse_s3_url(url)
            file_urls = self._list_s3_files(bucket, prefix)

            if not file_urls:
                return []

            # Log file count
            logger.info(f"[DataLoader] Found {len(file_urls)} files in {url}")

            # Collect metadata for balanced distribution
            logger.info(
                "[DataLoader] Collecting file metadata for balanced distribution"
            )
            file_urls, file_rows = self._collect_file_info(bucket, prefix)
            return self._distribute_files(
                file_urls=file_urls,
                file_rows=file_rows,
                worker_rank=worker_rank,
                num_workers=num_workers,
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
            worker_info = torch.utils.data.get_worker_info()
            worker_id = worker_info.id if worker_info else 0
            logger.info(
                f"[Dataset] Worker {worker_id}: Reading Parquet file: {file_url}"
            )

            # Get parquet file metadata
            bucket, key = self._parse_s3_url(file_url)
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            parquet_file = pq.ParquetFile(io.BytesIO(response["Body"].read()))
            num_row_groups = parquet_file.num_row_groups

            logger.info(
                f"[Dataset] Worker {worker_id}: Found {num_row_groups} row groups in {file_url}"
            )

            for row_group in range(num_row_groups):
                # Read row group and convert to pandas
                table = parquet_file.read_row_group(row_group)
                df = table.to_pandas()
                yield df

            total_time = time.time() - start_time
            logger.info(
                f"[Dataset] Worker {worker_id}: Completed reading {file_url} in {total_time:.2f}s"
            )

        except Exception as e:
            worker_info = torch.utils.data.get_worker_info()
            worker_id = worker_info.id if worker_info else 0
            logger.error(
                f"[Dataset] Worker {worker_id}: Error reading file {file_url}: {str(e)}"
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
                # Round-robin distribution between PyTorch workers
                files_to_read = self.file_urls[worker_id::num_workers]

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
