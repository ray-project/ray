# Standard library imports
from typing import List, Tuple, Optional, Iterator, Callable
import logging
import io
import time

# Third-party imports
import pandas as pd
import pyarrow.parquet as pq
import torch
from torch.utils.data import IterableDataset

# Ray imports
import ray
import ray.train

# Local imports
from s3_parquet_reader import S3ParquetReader
from .imagenet import get_preprocess_map_fn
from logutils import log_with_context

logger = logging.getLogger(__name__)


# TODO Look into https://github.com/webdataset/webdataset for more canonical way to do data
# distribution between Ray Train and Torch Dataloader workers.


class S3ParquetImageIterableDataset(S3ParquetReader, IterableDataset):
    """An iterable dataset that loads images from S3-stored Parquet files.

    This dataset:
    1. Reads Parquet files from S3 one row group at a time
    2. Processes images with optional random transforms in batches
    3. Yields (image, label) tensors
    4. Supports row limits per worker for controlled data processing
    """

    # Constants
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
        S3ParquetReader.__init__(self)
        self.file_urls = file_urls
        self.limit_rows_per_worker = limit_rows_per_worker
        self.random_transforms = random_transforms

        worker_rank = ray.train.get_context().get_world_rank()
        log_with_context(
            f"Worker {worker_rank}: Initialized with {len(file_urls)} files"
            f"{f' (limit: {limit_rows_per_worker} rows)' if limit_rows_per_worker else ''}"
        )

    def _has_reached_row_limit(self, rows_processed: int) -> bool:
        """Check if we've reached the row limit per worker.

        Args:
            rows_processed: Number of rows processed so far

        Returns:
            True if we've reached the limit, False otherwise
        """
        return (
            self.limit_rows_per_worker is not None
            and rows_processed >= self.limit_rows_per_worker
        )

    def _handle_processing_error(
        self, worker_id: int, error: Exception, context: str
    ) -> None:
        """Handle processing errors with consistent logging.

        Args:
            worker_id: ID of the current worker
            error: The exception that occurred
            context: Description of where the error occurred
        """
        log_with_context(
            f"Worker {worker_id}: Error {context}: {str(error)}", level="error"
        )

    def _get_worker_info(self) -> Tuple[int, int]:
        """Get current worker information.

        Returns:
            Tuple of (worker_id, num_workers)
        """
        worker_info = torch.utils.data.get_worker_info()
        worker_id = worker_info.id if worker_info else 0
        num_workers = worker_info.num_workers if worker_info else 1
        return worker_id, num_workers

    def _get_files_for_worker(self) -> List[str]:
        """Get files to process for current worker.

        Returns:
            List of file URLs to process
        """
        worker_id, num_workers = self._get_worker_info()
        return (
            self.file_urls[worker_id::num_workers]
            if num_workers > 1
            else self.file_urls
        )

    def _log_progress(
        self, worker_id: int, rows_processed: int, last_log_time: float
    ) -> float:
        """Log processing progress and return updated last_log_time.

        Args:
            worker_id: ID of the current worker
            rows_processed: Number of rows processed so far
            last_log_time: Time of last progress log

        Returns:
            Updated last_log_time
        """
        if rows_processed % self.LOG_FREQUENCY == 0:
            current_time = time.time()
            elapsed_time = current_time - last_log_time
            rows_per_second = (
                self.LOG_FREQUENCY / elapsed_time if elapsed_time > 0 else 0
            )
            log_with_context(
                f"Worker {worker_id}: Processed {rows_processed} rows "
                f"({rows_per_second:.2f} rows/sec)"
            )
            return current_time
        return last_log_time

    def _log_completion(
        self, worker_id: int, rows_processed: int, total_start_time: float
    ) -> None:
        """Log completion statistics.

        Args:
            worker_id: ID of the current worker
            rows_processed: Total number of rows processed
            total_start_time: Start time of processing
        """
        total_time = time.time() - total_start_time
        log_with_context(
            f"Worker {worker_id}: Finished: {rows_processed} rows in {total_time:.2f}s "
            f"({rows_processed/total_time:.2f} rows/sec)"
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
            log_with_context(f"Worker {worker_id}: Reading Parquet file: {file_url}")

            # Get parquet file metadata
            bucket, key = self._parse_s3_url(file_url)
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            parquet_file = pq.ParquetFile(io.BytesIO(response["Body"].read()))
            num_row_groups = parquet_file.num_row_groups

            log_with_context(
                f"Worker {worker_id}: Found {num_row_groups} row groups in {file_url}"
            )

            for row_group in range(num_row_groups):
                # Read row group and convert to pandas
                table = parquet_file.read_row_group(row_group)
                df = table.to_pandas()
                yield df

            total_time = time.time() - start_time
            log_with_context(
                f"Worker {worker_id}: Completed reading {file_url} in {total_time:.2f}s"
            )

        except Exception as e:
            worker_info = torch.utils.data.get_worker_info()
            worker_id = worker_info.id if worker_info else 0
            log_with_context(
                f"Worker {worker_id}: Error reading file {file_url}: {str(e)}",
                level="error",
            )
            raise

    def _process_file(
        self,
        file_url: str,
        preprocess_fn: Callable,
        worker_id: int,
        rows_processed: int,
        last_log_time: float,
    ) -> Iterator[Tuple[torch.Tensor, torch.Tensor, int, float]]:
        """Process a single Parquet file and its contents.

        Args:
            file_url: URL of the Parquet file to process
            preprocess_fn: Preprocessing function to apply to rows
            worker_id: ID of the current worker
            rows_processed: Total number of rows processed so far
            last_log_time: Time of last progress log

        Yields:
            Tuple containing:
                - image: Image tensor in (C, H, W) format
                - label: Label tensor
                - updated rows_processed count
                - updated last_log_time
        """
        for df in self._read_parquet_file(file_url):
            # Skip DataFrame if we've reached the limit
            if self._has_reached_row_limit(rows_processed):
                return

            try:
                # Format entire row group as a batch
                batch = [
                    {"image": row["image"], "label": row["label"]}
                    for _, row in df.iterrows()
                ]

                # Process batch
                processed_batch = preprocess_fn(batch)

                # Yield one image at a time from processed batch
                for item in processed_batch:
                    # Skip if we've reached the limit
                    if self._has_reached_row_limit(rows_processed):
                        return

                    # Convert to tensors
                    image = torch.as_tensor(item["image"], dtype=torch.float32)
                    label = torch.as_tensor(item["label"], dtype=torch.int64)

                    # Update progress and get new last_log_time
                    rows_processed += 1
                    last_log_time = self._log_progress(
                        worker_id, rows_processed, last_log_time
                    )

                    yield image, label, rows_processed, last_log_time

            except Exception as e:
                self._handle_processing_error(worker_id, e, "processing row group")
                continue

    def _initialize_iteration(
        self,
    ) -> Tuple[int, int, Callable, List[str], float, float]:
        """Initialize iteration parameters and resources.

        Returns:
            Tuple containing:
                - worker_id: ID of the current worker
                - num_workers: Total number of workers
                - preprocess_fn: Initialized preprocessing function
                - files_to_read: List of files to process
                - last_log_time: Initial timestamp
                - total_start_time: Initial timestamp
        """
        # Get worker info for file distribution
        worker_id, num_workers = self._get_worker_info()

        log_with_context(f"Worker {worker_id}/{num_workers}: Starting")

        # Initialize preprocessing function
        preprocess_fn = get_preprocess_map_fn(
            decode_image=True, random_transforms=self.random_transforms
        )

        # Distribute files among workers
        files_to_read = self._get_files_for_worker()

        log_with_context(f"Worker {worker_id}: Processing {len(files_to_read)} files")

        # Initialize timing variables
        last_log_time = time.time()
        total_start_time = time.time()

        return (
            worker_id,
            num_workers,
            preprocess_fn,
            files_to_read,
            last_log_time,
            total_start_time,
        )

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
            # Initialize iteration
            (
                worker_id,
                num_workers,
                preprocess_fn,
                files_to_read,
                last_log_time,
                total_start_time,
            ) = self._initialize_iteration()

            # Process files and rows
            rows_processed = 0

            for file_url in files_to_read:
                # Skip file if we've reached the limit
                if self._has_reached_row_limit(rows_processed):
                    log_with_context(
                        f"Worker {worker_id}: Reached row limit: {rows_processed}"
                    )
                    break

                # Process file and update state
                for (
                    image,
                    label,
                    updated_rows_processed,
                    updated_last_log_time,
                ) in self._process_file(
                    file_url,
                    preprocess_fn,
                    worker_id,
                    rows_processed,
                    last_log_time,
                ):
                    rows_processed = updated_rows_processed
                    last_log_time = updated_last_log_time
                    yield image, label

            # Log final statistics
            self._log_completion(worker_id, rows_processed, total_start_time)

        except Exception as e:
            self._handle_processing_error(worker_id, e, "fatal error")
            raise
