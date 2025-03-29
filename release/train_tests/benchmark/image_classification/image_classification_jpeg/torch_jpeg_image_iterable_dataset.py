# Standard library imports
from typing import List, Tuple, Optional, Iterator, Callable
import logging
import io
import time

# Third-party imports
import numpy as np
import PIL.Image as PILImage
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
    """An iterable dataset that loads images from S3-stored JPEG files."""

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
        S3JpegReader.__init__(self)
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

    def _fetch_image(self, file_url: str) -> Tuple[str, PILImage.Image]:
        """Fetch a single image from S3.

        Args:
            file_url: S3 URL to fetch

        Returns:
            Tuple of (file_url, PIL Image)

        Raises:
            Exception: If there's an error fetching the image
        """
        bucket = file_url.replace("s3://", "").split("/")[0]
        key = "/".join(file_url.replace("s3://", "").split("/")[1:])

        response = self.s3_client.get_object(Bucket=bucket, Key=key)
        image_data = response["Body"].read()
        image = PILImage.open(io.BytesIO(image_data))
        return file_url, image

    def _process_image(
        self,
        file_url: str,
        image: PILImage.Image,
        preprocess_fn: Callable,
        worker_id: int,
        rows_processed: int,
        last_log_time: float,
    ) -> Tuple[torch.Tensor, torch.Tensor, int, float]:
        """Process a single image and return (image, label) tensors.

        Args:
            file_url: URL of the image file
            image: PIL Image to process
            preprocess_fn: Preprocessing function to apply to image
            worker_id: ID of the current worker
            rows_processed: Number of rows processed so far
            last_log_time: Time of last progress log

        Returns:
            Tuple containing:
                - image: Image tensor in (C, H, W) format
                - label: Label tensor
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
            formatted_item = {"image": image_array, "class": wnid}

            # Process image
            processed_item = preprocess_fn([formatted_item])[0]

            # Convert to PyTorch tensor with correct shape (C, H, W)
            image = torch.as_tensor(
                processed_item["image"], dtype=torch.float32
            ).permute(2, 0, 1)
            label = torch.tensor(int(processed_item["label"]), dtype=torch.int64)

            # Update progress and get new last_log_time
            rows_processed += 1
            last_log_time = self._log_progress(worker_id, rows_processed, last_log_time)

            return image, label, rows_processed, last_log_time

        except Exception as e:
            self._handle_processing_error(worker_id, e, "processing image")
            raise

    def __iter__(self) -> Iterator[Tuple[torch.Tensor, torch.Tensor]]:
        """Iterate through the dataset and yield (image, label) tensors.

        Yields:
            Tuple[torch.Tensor, torch.Tensor]: (image, label) tensors

        Raises:
            Exception: If there's a fatal error during iteration
        """
        try:
            worker_id, num_workers = self._get_worker_info()
            files_to_read = self._get_files_for_worker()

            log_with_context(
                f"Worker {worker_id}/{num_workers}: Processing {len(files_to_read)} files"
            )

            preprocess_fn = get_preprocess_map_fn(
                random_transforms=self.random_transforms
            )

            rows_processed = 0
            last_log_time = time.time()
            total_start_time = time.time()

            for file_url in files_to_read:
                if self._has_reached_row_limit(rows_processed):
                    log_with_context(
                        f"Worker {worker_id}: Reached row limit of "
                        f"{self.limit_rows_per_worker}"
                    )
                    break

                try:
                    # Fetch and process one image at a time
                    file_url, image = self._fetch_image(file_url)
                    image, label, rows_processed, last_log_time = self._process_image(
                        file_url,
                        image,
                        preprocess_fn,
                        worker_id,
                        rows_processed,
                        last_log_time,
                    )
                    yield image, label

                except Exception as e:
                    self._handle_processing_error(worker_id, e, "processing file")
                    continue

            self._log_completion(worker_id, rows_processed, total_start_time)

        except Exception as e:
            self._handle_processing_error(worker_id, e, "fatal error")
            raise
