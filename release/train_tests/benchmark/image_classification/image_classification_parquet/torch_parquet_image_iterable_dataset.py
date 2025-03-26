# Standard library imports
import io
import logging
import time
from typing import Iterator, List, Optional, Tuple

# Third-party imports
import boto3
import pandas as pd
import pyarrow.parquet as pq
import torch
from botocore.exceptions import NoCredentialsError
from torch.utils.data import IterableDataset

# Ray imports
import ray
import ray.train

# Local imports
from .imagenet import get_preprocess_map_fn
from s3_reader import AWS_REGION, S3Reader

logger = logging.getLogger(__name__)


# TODO Look into https://github.com/webdataset/webdataset for more canonical way to do data
# distribution between Ray Train and Torch Dataloader workers.


@ray.remote(num_cpus=0.25)
def _fetch_parquet_metadata(bucket: str, key: str, file_url: str) -> Tuple[str, int]:
    """Fetch Parquet row count using S3 Select in parallel.

    This function is used to get the number of rows in each Parquet file
    without reading the entire file, which helps with balanced distribution.

    Args:
        bucket: S3 bucket name
        key: S3 object key
        file_url: Full S3 URL for logging

    Returns:
        Tuple of (file_url, row_count)
    """
    import logging

    logger = logging.getLogger(__name__)
    s3_client = boto3.client("s3", region_name=AWS_REGION)

    # Use S3 Select to count rows efficiently
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

    logger.info(f"[S3ParquetReader] File {file_url} has {row_count} rows")
    return file_url, row_count


class S3ParquetReader(S3Reader):
    """Extended S3Reader class for Parquet-specific functionality.

    This class provides methods for:
    1. Collecting metadata about Parquet files (row counts)
    2. Distributing files among workers based on row counts
    3. Reading Parquet files efficiently
    """

    def _collect_file_info(
        self, bucket: str, prefix: str
    ) -> Tuple[List[str], List[int]]:
        """Collect file URLs and their row counts in parallel using Ray tasks.

        This method:
        1. Lists all Parquet files in the S3 prefix
        2. Launches parallel tasks to count rows in each file
        3. Returns file URLs and their row counts for balanced distribution

        Args:
            bucket: S3 bucket name
            prefix: S3 prefix to list

        Returns:
            Tuple of (file_urls, row_counts)
        """
        files = self._list_s3_files(bucket, prefix)

        # Launch parallel tasks for each file
        tasks = []
        for file in files:
            file_url = f"s3://{bucket}/{file['Key']}"
            task = _fetch_parquet_metadata.remote(bucket, file["Key"], file_url)
            tasks.append(task)

        # Wait for all tasks to complete
        worker_rank = ray.train.get_context().get_world_rank()
        logger.info(
            f"[S3ParquetReader] Worker {worker_rank}: Waiting for metadata from {len(tasks)} files..."
        )
        results = ray.get(tasks)

        # Unzip results
        file_urls, file_rows = zip(*results) if results else ([], [])

        logger.info(
            f"[S3ParquetReader] Worker {worker_rank}: Collected metadata for {len(file_urls)} files"
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

        This method:
        1. Sorts files by row count for better distribution
        2. Calculates target rows per worker
        3. Assigns files to minimize row count differences between workers

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
                f"[S3ParquetReader] Worker {worker_rank}: Single worker or no files, returning all "
                f"{len(file_urls)} files with total {sum(file_rows)} rows"
            )
            return file_urls

        # Calculate target rows per worker for balanced distribution
        total_rows = sum(file_rows)
        target_rows_per_worker = total_rows / num_workers
        logger.info(
            f"[S3ParquetReader] Worker {worker_rank}: Total rows: {total_rows}, Target rows per "
            f"worker: {target_rows_per_worker:.0f}"
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
            f"[S3ParquetReader] Worker {worker_rank}: Assigned {len(my_files)}/{len(file_urls)} files "
            f"with {my_row_count}/{total_rows} rows "
            f"({my_row_count/total_rows*100:.1f}% of total)"
        )
        return my_files

    def _list_s3_files(self, bucket: str, prefix: str) -> List[dict]:
        """List files in an S3 bucket with the given prefix.

        Args:
            bucket: S3 bucket name
            prefix: S3 prefix to list

        Returns:
            List of file objects from S3 response
        """
        all_contents = []
        continuation_token = None

        while True:
            # Prepare list_objects_v2 parameters
            list_params = {"Bucket": bucket, "Prefix": prefix}
            if continuation_token:
                list_params["ContinuationToken"] = continuation_token

            # Make the request
            response = self.s3_client.list_objects_v2(**list_params)

            # Handle no contents case
            if "Contents" not in response:
                if not all_contents:  # Only warn if this is the first request
                    logger.warning(
                        f"[S3ParquetReader] No files found in s3://{bucket}/{prefix}"
                    )
                break

            # Add contents from this page
            all_contents.extend(response["Contents"])

            # Log progress
            logger.info(
                f"[S3ParquetReader] Listed {len(all_contents)} files so far from "
                f"s3://{bucket}/{prefix}"
            )

            # Check if there are more pages
            if not response.get("IsTruncated"):  # No more pages
                break

            # Get token for next page
            continuation_token = response.get("NextContinuationToken")
            if not continuation_token:  # Shouldn't happen if IsTruncated is True
                break

        return all_contents

    def _get_file_urls(self, url: str) -> List[str]:
        """Get file URLs from S3 and distribute them among Ray workers.

        Args:
            url: The S3 URL to list files from

        Returns:
            List of S3 URLs assigned to the current Ray worker

        Raises:
            S3CredentialsError: If AWS credentials are not found
            S3FileError: If there's an error listing files
        """
        try:
            # Get Ray worker info
            worker_rank = ray.train.get_context().get_world_rank()
            num_workers = ray.train.get_context().get_world_size()

            # Parse bucket and prefix
            bucket, prefix = self._parse_s3_url(url)

            # Collect metadata for balanced distribution
            logger.info(
                f"[S3ParquetReader] Worker {worker_rank}: Collecting file metadata for balanced "
                "distribution"
            )
            file_urls, file_rows = self._collect_file_info(bucket, prefix)
            logger.info(f"[S3ParquetReader] Found {len(file_urls)} files in {url}")

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


class S3ParquetImageIterableDataset(S3ParquetReader, IterableDataset):
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

        worker_rank = ray.train.get_context().get_world_rank()
        logger.info(
            f"[S3ParquetImageIterableDataset] Worker {worker_rank}: Initialized with "
            f"{len(file_urls)} files"
            f"{f' (limit: {limit_rows_per_worker} rows)' if limit_rows_per_worker else ''}"
        )

    def _read_parquet_file(self, file_url: str) -> Iterator[pd.DataFrame]:
        """Read a Parquet file from S3 one row group at a time.

        This method:
        1. Fetches the Parquet file from S3
        2. Reads it row group by row group
        3. Converts each row group to a pandas DataFrame

        Args:
            file_url: S3 URL of the Parquet file

        Yields:
            DataFrame containing one row group at a time

        Raises:
            Exception: If there's an error reading the file
        """
        try:
            start_time = time.time()
            worker_info = torch.utils.data.get_worker_info()
            worker_id = worker_info.id if worker_info else 0
            logger.info(
                f"[S3ParquetImageIterableDataset] Worker {worker_id}: Reading Parquet file: "
                f"{file_url}"
            )

            # Get parquet file metadata
            bucket, key = self._parse_s3_url(file_url)
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            parquet_file = pq.ParquetFile(io.BytesIO(response["Body"].read()))
            num_row_groups = parquet_file.num_row_groups

            logger.info(
                f"[S3ParquetImageIterableDataset] Worker {worker_id}: Found {num_row_groups} row "
                f"groups in {file_url}"
            )

            for row_group in range(num_row_groups):
                # Read row group and convert to pandas
                table = parquet_file.read_row_group(row_group)
                df = table.to_pandas()
                yield df

            total_time = time.time() - start_time
            logger.info(
                f"[S3ParquetImageIterableDataset] Worker {worker_id}: Completed reading {file_url} "
                f"in {total_time:.2f}s"
            )

        except Exception as e:
            worker_info = torch.utils.data.get_worker_info()
            worker_id = worker_info.id if worker_info else 0
            logger.error(
                f"[S3ParquetImageIterableDataset] Worker {worker_id}: Error reading file "
                f"{file_url}: {str(e)}"
            )
            raise

    def __iter__(self) -> Iterator[Tuple[torch.Tensor, torch.Tensor]]:
        """Main iteration method that processes files and yields (image, label) tensors.

        This method:
        1. Distributes files among workers
        2. Processes rows with image transforms
        3. Converts to tensors
        4. Respects row limits per worker

        Yields:
            Tuple of (image_tensor, label_tensor)

        Raises:
            Exception: If there's a fatal error during processing
        """
        try:
            # Get worker info for file distribution
            worker_info = torch.utils.data.get_worker_info()
            worker_id = worker_info.id if worker_info else 0
            num_workers = worker_info.num_workers if worker_info else 1

            logger.info(
                f"[S3ParquetImageIterableDataset] Worker {worker_id}/{num_workers}: Starting"
            )

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
                f"[S3ParquetImageIterableDataset] Worker {worker_id}: Processing "
                f"{len(files_to_read)} files"
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
                        f"[S3ParquetImageIterableDataset] Worker {worker_id}: Reached row limit: "
                        f"{rows_processed}"
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
                                    f"[S3ParquetImageIterableDataset] Worker {worker_id}: "
                                    f"Processed {rows_processed} rows ({rows_per_second:.2f} "
                                    "rows/sec)"
                                )
                                last_log_time = current_time

                            yield image, label

                        except Exception as e:
                            logger.error(
                                f"[S3ParquetImageIterableDataset] Worker {worker_id}: Error "
                                f"processing row: {str(e)}"
                            )
                            continue

            # Log final statistics
            total_time = time.time() - total_start_time
            logger.info(
                f"[S3ParquetImageIterableDataset] Worker {worker_id}: Finished: "
                f"{rows_processed} rows in {total_time:.2f}s "
                f"({rows_processed/total_time:.2f} rows/sec)"
            )

        except Exception as e:
            logger.error(
                f"[S3ParquetImageIterableDataset] Worker {worker_id}: Fatal error: {str(e)}",
                exc_info=True,
            )
            raise
