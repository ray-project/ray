# Standard library imports
import io
import logging
import time
from typing import Iterator, List, Optional, Tuple, Callable

# Third-party imports
import numpy as np
from PIL import Image as PILImage
import torch
from torch.utils.data import IterableDataset

# Ray imports
import ray
import ray.train

# Local imports
from s3_jpeg_reader import S3JpegReader
from .imagenet import get_preprocess_map_fn
from logger_utils import ContextLoggerAdapter

logger = ContextLoggerAdapter(logging.getLogger(__name__))


class S3JpegImageIterableDataset(S3JpegReader, IterableDataset):
    """An iterable dataset that loads images from S3-stored JPEG files.

    Features:
    - Direct image fetching from S3
    - Optional random transforms for training
    - Row limits per worker for controlled processing
    - Progress logging and performance metrics
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
            limit_rows_per_worker: Maximum number of rows to process per worker
        """
        super().__init__()
        self.file_urls = file_urls
        self.limit_rows_per_worker = limit_rows_per_worker
        self.random_transforms = random_transforms

        worker_rank = ray.train.get_context().get_world_rank()
        logger.info(
            f"Worker {worker_rank}: Initialized with {len(file_urls)} files"
            f"{f' (limit: {limit_rows_per_worker} rows)' if limit_rows_per_worker else ''}"
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
            logger.info(
                f"Worker {worker_id}: Processed {rows_processed} rows "
                f"({rows_per_second:.2f} rows/sec)"
            )
            return current_time
        return last_log_time

    def _fetch_image(self, file_url: str) -> Tuple[str, Optional[PILImage.Image]]:
        """Fetch a single image from S3.

        Args:
            file_url: S3 URL to fetch (e.g., "s3://bucket/path/to/image.jpg")

        Returns:
            Tuple of (file_url, PIL Image) where PIL Image is None if fetch failed
        """
        worker_id, _ = self._get_worker_info()
        try:
            bucket = file_url.replace("s3://", "").split("/")[0]
            key = "/".join(file_url.replace("s3://", "").split("/")[1:])

            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            image_data = response["Body"].read()
            image = PILImage.open(io.BytesIO(image_data))
            return file_url, image

        except Exception as e:
            logger.error(
                f"Worker {worker_id}: Error fetching image from {file_url}: {str(e)}",
                exc_info=True,
            )
            return file_url, None

    def _process_image(
        self,
        image: PILImage.Image,
        file_url: str,
        preprocess_fn: Callable,
    ) -> Tuple[torch.Tensor, torch.Tensor]:
        """Process a single image and convert to tensors.

        Args:
            image: PIL Image to process
            file_url: URL of the image file
            preprocess_fn: Preprocessing function to apply

        Returns:
            Tuple of (image_tensor, label_tensor)
        """
        try:
            # Convert to RGB and numpy array
            if image.mode != "RGB":
                image = image.convert("RGB")
            image_array = np.array(image, dtype=np.uint8)

            # Ensure HWC format (Height x Width x Channels)
            if len(image_array.shape) == 2:  # Grayscale
                image_array = np.stack([image_array] * 3, axis=-1)
            elif len(image_array.shape) == 3 and image_array.shape[0] == 3:  # CHW
                image_array = np.transpose(image_array, (1, 2, 0))

            wnid = file_url.split("/")[-2]  # Extract WNID from path

            processed = preprocess_fn({"image": image_array, "class": wnid})

            image = torch.as_tensor(processed["image"], dtype=torch.float32)
            label = torch.as_tensor(processed["label"], dtype=torch.int64)

            return image, label

        except Exception as e:
            logger.error(
                f"Error processing {file_url}: {str(e)}",
                exc_info=True,
            )
            raise

    def _process_files(
        self, files_to_read: List[str], preprocess_fn: Callable, worker_id: int
    ) -> Iterator[Tuple[torch.Tensor, torch.Tensor]]:
        """Process multiple files and yield processed rows.

        Args:
            files_to_read: List of file URLs to process
            preprocess_fn: Preprocessing function to apply
            worker_id: ID of the current worker

        Yields:
            Tuple of (image_tensor, label_tensor)
        """
        rows_processed = 0
        last_log_time = time.time()
        total_start_time = time.time()

        for file_url in files_to_read:
            if self._has_reached_row_limit(rows_processed):
                logger.info(f"Worker {worker_id}: Reached row limit: {rows_processed}")
                break

            file_url, image = self._fetch_image(file_url)
            if image is None:
                continue

            try:
                image, label = self._process_image(
                    image,
                    file_url,
                    preprocess_fn,
                )
                rows_processed += 1
                last_log_time = self._log_progress(
                    worker_id, rows_processed, last_log_time
                )
                yield image, label
            except Exception:
                continue

        # Log final statistics
        total_time = time.time() - total_start_time
        logger.info(
            f"Worker {worker_id}: Finished: {rows_processed} rows in {total_time:.2f}s "
            f"({rows_processed/total_time:.2f} rows/sec)"
        )

    def __iter__(self) -> Iterator[Tuple[torch.Tensor, torch.Tensor]]:
        """Iterate through the dataset and yield (image, label) tensors.

        Yields:
            Tuple[torch.Tensor, torch.Tensor]: (image, label) tensors

        Raises:
            Exception: If there's a fatal error during iteration
        """
        try:
            # Get worker info for file distribution
            worker_id, num_workers = self._get_worker_info()
            logger.info(f"Worker {worker_id}/{num_workers}: Starting")

            # Distribute files among workers
            files_to_read = (
                self.file_urls
                if num_workers == 1
                else self.file_urls[worker_id::num_workers]
            )

            logger.info(f"Worker {worker_id}: Processing {len(files_to_read)} files")

            # Initialize preprocessing function
            preprocess_fn = get_preprocess_map_fn(
                random_transforms=self.random_transforms
            )

            # Process files and yield results
            yield from self._process_files(files_to_read, preprocess_fn, worker_id)

        except Exception as e:
            logger.error(
                f"Worker {worker_id}: Fatal error: {str(e)}",
                exc_info=True,
            )
            raise
