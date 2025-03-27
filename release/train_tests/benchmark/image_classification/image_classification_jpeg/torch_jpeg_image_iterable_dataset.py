# Standard library imports
import io
import logging
import time
from typing import Iterator, List, Optional, Tuple

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
        logger.info(
            f"[S3JpegImageIterableDataset] Worker {worker_rank}: Initialized with "
            f"{len(file_urls)} files"
            f"{f' (limit: {limit_rows_per_worker} rows)' if limit_rows_per_worker else ''} "
            f"(batch size: {batch_size})"
        )

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

        for file_url in file_urls:
            try:
                bucket = file_url.replace("s3://", "").split("/")[0]
                key = "/".join(file_url.replace("s3://", "").split("/")[1:])

                response = self.s3_client.get_object(Bucket=bucket, Key=key)
                image_data = response["Body"].read()
                image = PILImage.open(io.BytesIO(image_data))
                results.append((file_url, image))

            except Exception as e:
                logger.error(
                    f"[S3JpegImageIterableDataset] Error fetching image from {file_url}: {str(e)}",
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

            valid_results = [(url, img) for url, img in results if img is not None]
            if len(valid_results) < len(results):
                logger.warning(
                    f"[S3JpegImageIterableDataset] Failed to fetch "
                    f"{len(results) - len(valid_results)} images"
                )

            return valid_results
        except Exception as e:
            logger.error(f"[S3JpegImageIterableDataset] Error fetching batch: {str(e)}")
            raise

    def __iter__(self) -> Iterator[Tuple[torch.Tensor, torch.Tensor]]:
        """Iterate through the dataset and yield (image, label) tensors.

        Yields:
            Tuple[torch.Tensor, torch.Tensor]: (image, label) tensors

        Raises:
            Exception: If there's a fatal error during iteration
        """
        try:
            worker_info = torch.utils.data.get_worker_info()
            worker_id = worker_info.id if worker_info else 0
            num_workers = worker_info.num_workers if worker_info else 1

            # Distribute files among workers
            files_to_read = (
                self.file_urls[worker_id::num_workers]
                if worker_info
                else self.file_urls
            )

            logger.info(
                f"[S3JpegImageIterableDataset] Worker {worker_id}/{num_workers}: "
                f"Processing {len(files_to_read)} files"
            )

            preprocess_fn = get_preprocess_map_fn(
                random_transforms=self.random_transforms
            )

            rows_processed = 0
            last_log_time = time.time()
            total_start_time = time.time()

            for i in range(0, len(files_to_read), self.batch_size):
                batch_urls = files_to_read[i : i + self.batch_size]

                if (
                    self.limit_rows_per_worker is not None
                    and rows_processed >= self.limit_rows_per_worker
                ):
                    logger.info(
                        f"[S3JpegImageIterableDataset] Worker {worker_id}: Reached row limit of "
                        f"{self.limit_rows_per_worker}"
                    )
                    break

                try:
                    batch_results = self._fetch_images_batch(batch_urls)

                    for file_url, image in batch_results:
                        try:
                            # Convert to RGB and numpy array
                            if image.mode != "RGB":
                                image = image.convert("RGB")
                            image_array = np.array(image, dtype=np.uint8)

                            # Ensure HWC format (Height x Width x Channels)
                            if len(image_array.shape) == 2:  # Grayscale
                                image_array = np.stack([image_array] * 3, axis=-1)
                            elif (
                                len(image_array.shape) == 3
                                and image_array.shape[0] == 3
                            ):  # CHW
                                image_array = np.transpose(image_array, (1, 2, 0))

                            wnid = file_url.split("/")[-2]  # Extract WNID from path

                            processed = preprocess_fn(
                                {"image": image_array, "class": wnid}
                            )

                            image = torch.as_tensor(
                                processed["image"], dtype=torch.float32
                            )
                            label = torch.as_tensor(
                                processed["label"], dtype=torch.int64
                            )

                            rows_processed += 1

                            if rows_processed % self.LOG_FREQUENCY == 0:
                                current_time = time.time()
                                elapsed_time = current_time - last_log_time
                                rows_per_second = (
                                    self.LOG_FREQUENCY / elapsed_time
                                    if elapsed_time > 0
                                    else 0
                                )
                                logger.info(
                                    f"[S3JpegImageIterableDataset] Worker {worker_id}: "
                                    f"Processed {rows_processed} rows ({rows_per_second:.2f} "
                                    "rows/sec)"
                                )
                                last_log_time = current_time

                            yield image, label

                        except Exception as e:
                            logger.error(
                                f"[S3JpegImageIterableDataset] Worker {worker_id}: Error "
                                f"processing {file_url}: {str(e)}",
                                exc_info=True,
                            )
                            continue

                except Exception as e:
                    logger.error(
                        f"[S3JpegImageIterableDataset] Worker {worker_id}: Error processing "
                        f"batch: {str(e)}",
                        exc_info=True,
                    )
                    continue

            total_time = time.time() - total_start_time
            logger.info(
                f"[S3JpegImageIterableDataset] Worker {worker_id}: Completed {rows_processed} "
                f"rows in {total_time:.2f}s ({rows_processed/total_time:.2f} rows/sec)"
            )

        except Exception as e:
            logger.error(
                f"[S3JpegImageIterableDataset] Worker {worker_id}: Fatal error: {str(e)}",
                exc_info=True,
            )
            raise
