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
from logutils import log_with_context

logger = logging.getLogger(__name__)


class S3JpegImageIterableDataset(S3JpegReader, IterableDataset):
    """An iterable dataset that loads images from S3-stored JPEG files.

    Features:
    - Direct image fetching from S3
    - Optional random transforms for training
    - Row limits per worker for controlled processing
    - Progress logging and performance metrics
    """

    # Constants
    BATCH_SIZE = 1  # Number of images to fetch
    LOG_FREQUENCY = 1000  # Log progress every 1000 rows

    def __init__(
        self,
        file_urls: List[str],
        random_transforms: bool = True,
        limit_rows_per_worker: Optional[int] = None,
        batch_size: int = BATCH_SIZE,
    ):
        """Initialize the dataset.

        Args:
            file_urls: List of S3 URLs to load
            random_transforms: Whether to use random transforms for training
            limit_rows_per_worker: Maximum number of rows to process per worker
            batch_size: Number of images to fetch
        """
        super().__init__()
        self.file_urls = file_urls
        self.limit_rows_per_worker = limit_rows_per_worker
        self.random_transforms = random_transforms
        self.batch_size = batch_size

        worker_rank = ray.train.get_context().get_world_rank()
        log_with_context(
            f"Worker {worker_rank}: Initialized with {len(file_urls)} files"
            f"{f' (limit: {limit_rows_per_worker} rows)' if limit_rows_per_worker else ''} "
            f"(batch size: {batch_size})"
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
            log_with_context(
                f"Worker {worker_id}: Processed {rows_processed} rows "
                f"({rows_per_second:.2f} rows/sec)"
            )
            return current_time
        return last_log_time

    def _fetch_image_batch(
        self, file_urls: List[str]
    ) -> List[Tuple[str, PILImage.Image]]:
        """Fetch a batch of images from S3.

        Args:
            file_urls: List of S3 URLs to fetch (e.g., "s3://bucket/path/to/image.jpg")

        Returns:
            List of (file_url, PIL Image) tuples where:
                - file_url: The original S3 URL
                - PIL Image: The loaded image, or None if fetch failed
        """
        results = []
        worker_id, _ = self._get_worker_info()

        for file_url in file_urls:
            try:
                bucket = file_url.replace("s3://", "").split("/")[0]
                key = "/".join(file_url.replace("s3://", "").split("/")[1:])

                response = self.s3_client.get_object(Bucket=bucket, Key=key)
                image_data = response["Body"].read()
                image = PILImage.open(io.BytesIO(image_data))
                results.append((file_url, image))

            except Exception as e:
                log_with_context(
                    f"Worker {worker_id}: Error fetching image from {file_url}: {str(e)}",
                    level="error",
                    exc_info=True,
                )
                results.append((file_url, None))

        return results

    def _fetch_images_batch(
        self, file_urls: List[str]
    ) -> List[Tuple[str, PILImage.Image]]:
        """Fetch a batch of images from S3.

        Args:
            file_urls: List of S3 URLs to fetch

        Returns:
            List of (file_url, PIL Image) tuples

        Raises:
            Exception: If there's an error fetching the batch
        """
        try:
            results = self._fetch_image_batch(file_urls)
            worker_id, _ = self._get_worker_info()

            valid_results = [(url, img) for url, img in results if img is not None]
            if len(valid_results) < len(results):
                log_with_context(
                    f"Worker {worker_id}: Failed to fetch {len(results) - len(valid_results)} images",
                    level="warning",
                )

            return valid_results
        except Exception as e:
            worker_id, _ = self._get_worker_info()
            log_with_context(
                f"Worker {worker_id}: Error fetching batch: {str(e)}", level="error"
            )
            raise

    def _process_image(
        self,
        image: PILImage.Image,
        file_url: str,
        preprocess_fn: Callable,
        worker_id: int,
        rows_processed: int,
        last_log_time: float,
    ) -> Tuple[torch.Tensor, torch.Tensor, int, float]:
        """Process a single image and convert to tensors.

        Args:
            image: PIL Image to process
            file_url: URL of the image file
            preprocess_fn: Preprocessing function to apply
            worker_id: ID of the current worker
            rows_processed: Total number of rows processed so far
            last_log_time: Time of last progress log

        Returns:
            Tuple containing:
                - image: Processed image tensor
                - label: Processed label tensor
                - updated rows_processed count
                - updated last_log_time
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

            rows_processed += 1
            last_log_time = self._log_progress(worker_id, rows_processed, last_log_time)

            return image, label, rows_processed, last_log_time

        except Exception as e:
            log_with_context(
                f"Worker {worker_id}: Error processing {file_url}: {str(e)}",
                level="error",
                exc_info=True,
            )
            raise

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

            # Distribute files among workers
            files_to_read = (
                self.file_urls
                if num_workers == 1
                else self.file_urls[worker_id::num_workers]
            )

            log_with_context(
                f"Worker {worker_id}/{num_workers}: Processing {len(files_to_read)} files"
            )

            preprocess_fn = get_preprocess_map_fn(
                random_transforms=self.random_transforms
            )

            rows_processed = 0
            last_log_time = time.time()
            total_start_time = time.time()

            for i in range(0, len(files_to_read), self.batch_size):
                batch_urls = files_to_read[i : i + self.batch_size]

                if self._has_reached_row_limit(rows_processed):
                    log_with_context(
                        f"Worker {worker_id}: Reached row limit of {self.limit_rows_per_worker}"
                    )
                    break

                try:
                    batch_results = self._fetch_images_batch(batch_urls)

                    for file_url, image in batch_results:
                        try:
                            # Process image and get results
                            (
                                image,
                                label,
                                rows_processed,
                                last_log_time,
                            ) = self._process_image(
                                image,
                                file_url,
                                preprocess_fn,
                                worker_id,
                                rows_processed,
                                last_log_time,
                            )
                            yield image, label

                        except Exception:
                            continue

                except Exception:
                    continue

            # Log final statistics
            total_time = time.time() - total_start_time
            log_with_context(
                f"Worker {worker_id}: Completed {rows_processed} rows in {total_time:.2f}s "
                f"({rows_processed/total_time:.2f} rows/sec)"
            )

        except Exception as e:
            log_with_context(
                f"Worker {worker_id}: Fatal error: {str(e)}",
                level="error",
                exc_info=True,
            )
            raise
