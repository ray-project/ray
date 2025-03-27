# Standard library imports
from typing import List, Tuple, Optional, Iterator
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

logger = logging.getLogger(__name__)


# TODO Look into https://github.com/webdataset/webdataset for more canonical way to do data
# distribution between Ray Train and Torch Dataloader workers.


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
