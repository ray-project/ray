# Standard library imports
from typing import Iterator, List, Optional, Tuple
import time
import logging
import io
import numpy as np

# Third-party imports
import boto3
from botocore.exceptions import NoCredentialsError
from PIL import Image as PILImage
import torch
from torch.utils.data import IterableDataset

# Ray imports
import ray
import ray.train

# Local imports
from s3_reader import S3Reader
from .imagenet import get_preprocess_map_fn

logger = logging.getLogger(__name__)


# TODO Look into https://github.com/webdataset/webdataset for more canonical way to do data
# distribution between Ray Train and Torch Dataloader workers.


@ray.remote(num_cpus=0.25)
def _list_s3_batch(
    bucket: str,
    prefix: str,
    continuation_token: Optional[str] = None,
    batch_size: int = 1000,
) -> Tuple[List[Tuple[str, int]], Optional[str]]:
    """List a batch of files from S3 in parallel.

    This function is used to efficiently list files from S3 in batches:
    1. Makes a paginated request to S3's list_objects_v2 API
    2. Processes the response to extract file URLs and sizes
    3. Returns both the current batch and a token for the next batch

    Args:
        bucket: S3 bucket name to list files from
        prefix: S3 prefix to filter files (e.g., "path/to/directory/")
        continuation_token: Token from previous request for pagination
        batch_size: Maximum number of files to return in one request (default: 1000)

    Returns:
        Tuple containing:
            - List of (file_url, size) tuples, where:
                - file_url: Full S3 URL (e.g., "s3://bucket/path/to/file")
                - size: File size in bytes
            - Optional[str]: Token for the next batch (None if no more files)
    """
    # Create S3 client for this task
    s3_client = boto3.client("s3")

    # Prepare request parameters
    list_params = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": batch_size}
    if continuation_token:
        list_params["ContinuationToken"] = continuation_token

    # Make the request to S3
    response = s3_client.list_objects_v2(**list_params)

    # Handle case where no files are found
    if "Contents" not in response:
        return [], None

    # Process the batch of files
    batch_files = response["Contents"]
    results = [(f"s3://{bucket}/{f['Key']}", f["Size"]) for f in batch_files]

    # Get token for next batch if there are more files
    next_token = (
        response.get("NextContinuationToken") if response.get("IsTruncated") else None
    )

    return results, next_token


class S3JpegReader(S3Reader):
    """Extended S3Reader class for JPEG-specific functionality.

    This class provides methods for:
    1. Collecting metadata about JPEG files (sizes)
    2. Distributing files among workers based on file sizes
    3. Managing parallel S3 operations with Ray tasks
    """

    def _collect_file_info(
        self,
        bucket: str,
        prefix: str,
        batch_size: int = 1000,
        max_concurrent_tasks: int = 10,
    ) -> Tuple[List[str], List[int]]:
        """Collect file URLs and their sizes using parallel Ray tasks.

        This method:
        1. Starts a parallel task to list files from S3
        2. Manages concurrent tasks for pagination
        3. Aggregates results from all tasks

        Args:
            bucket: S3 bucket name to list files from
            prefix: S3 prefix to filter files
            batch_size: Number of files to process in each request
            max_concurrent_tasks: Maximum number of concurrent Ray tasks

        Returns:
            Tuple of (file_urls, file_sizes_in_bytes)
        """
        worker_rank = ray.train.get_context().get_world_rank()
        all_files = []
        all_sizes = []
        total_files = 0

        # Start first task
        active_tasks = {}  # Dict of task_id -> (task, token)
        first_task = _list_s3_batch.remote(bucket, prefix, None, batch_size)
        active_tasks[first_task] = (first_task, None)

        while active_tasks:
            # Wait for the next task to complete
            ready_tasks, _ = ray.wait(list(active_tasks.keys()), num_returns=1)
            completed_task = ready_tasks[0]
            original_token = active_tasks[completed_task][1]
            del active_tasks[completed_task]

            # Get results from completed task
            results, next_token = ray.get(completed_task)

            # Process results
            if results:
                file_urls, file_sizes = zip(*results)
                all_files.extend(file_urls)
                all_sizes.extend(file_sizes)
                total_files += len(results)

                # Log progress
                logger.info(
                    f"[S3JpegReader] Worker {worker_rank}: Listed {total_files} files "
                    f"from s3://{bucket}/{prefix}"
                    + (f" (token: {original_token})" if original_token else "")
                )

            # Start next task if there are more pages and we're under the limit
            if next_token and len(active_tasks) < max_concurrent_tasks:
                next_task = _list_s3_batch.remote(
                    bucket, prefix, next_token, batch_size
                )
                active_tasks[next_task] = (next_task, next_token)
                logger.debug(
                    f"[S3JpegReader] Worker {worker_rank}: Starting task with token "
                    f"{next_token}"
                )

        logger.info(
            f"[S3JpegReader] Worker {worker_rank}: Collected metadata for "
            f"{len(all_files)} files"
        )
        return all_files, all_sizes

    def _distribute_files(
        self,
        file_urls: List[str],
        file_rows: List[int],
        worker_rank: int,
        num_workers: int,
    ) -> List[str]:
        """Distribute files among workers based on file sizes.

        This method:
        1. Sorts files by size for optimal distribution
        2. Calculates target size per worker
        3. Assigns files to minimize size differences between workers

        Args:
            file_urls: List of file URLs to distribute
            file_rows: List of file sizes in bytes
            worker_rank: Current worker's rank
            num_workers: Total number of workers

        Returns:
            List of file URLs assigned to this worker
        """
        # Sort files by size (descending) for better distribution
        files_with_sizes = sorted(
            zip(file_urls, file_rows), key=lambda x: x[1], reverse=True
        )
        file_urls = [f[0] for f in files_with_sizes]
        file_rows = [f[1] for f in files_with_sizes]

        # If no workers or files, return all files
        if num_workers <= 1 or not file_urls:
            logger.info(
                f"[S3JpegReader] Worker {worker_rank}: Single worker or no files, "
                f"returning all {len(file_urls)} files with total {sum(file_rows)} bytes"
            )
            return file_urls

        # Calculate target size per worker for balanced distribution
        total_size = sum(file_rows)
        target_size_per_worker = total_size / num_workers
        logger.info(
            f"[S3JpegReader] Worker {worker_rank}: Total size: {total_size} bytes, "
            f"Target per worker: {target_size_per_worker:.0f} bytes"
        )

        # Distribute files to minimize size differences between workers
        worker_files = [[] for _ in range(num_workers)]
        worker_sizes = [0] * num_workers

        # Assign files to workers, trying to keep sizes balanced
        for file_url, file_size in zip(file_urls, file_rows):
            # Find worker with minimum current size
            min_size_worker = min(range(num_workers), key=lambda w: worker_sizes[w])
            worker_files[min_size_worker].append(file_url)
            worker_sizes[min_size_worker] += file_size

        # Get this worker's files
        my_files = worker_files[worker_rank]
        my_size = worker_sizes[worker_rank]

        logger.info(
            f"[S3JpegImageIterableDataset] Worker {worker_rank}: Assigned {len(my_files)}/"
            f"{len(file_urls)} files with {my_size}/{total_size} bytes "
            f"({my_size/total_size*100:.1f}%)"
        )
        return my_files

    def _get_file_urls(self, url: str) -> List[str]:
        """Get file URLs from S3 and distribute them among Ray workers.

        This method:
        1. Gets worker information from Ray
        2. Collects file metadata from S3
        3. Distributes files among workers

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
                f"[S3JpegReader] Worker {worker_rank}: Collecting file metadata for "
                "balanced distribution"
            )
            file_urls, file_size_bytes = self._collect_file_info(bucket, prefix)
            logger.info(f"[S3JpegReader] Found {len(file_urls)} files in {url}")

            return self._distribute_files(
                file_urls=file_urls,
                file_rows=file_size_bytes,
                worker_rank=worker_rank,
                num_workers=num_workers,
            )

        except NoCredentialsError:
            raise self.S3CredentialsError(
                "AWS credentials not found. Ensure you have configured them."
            )
        except Exception as e:
            raise self.S3FileError(f"Error listing files from {url}: {str(e)}")


@ray.remote(num_cpus=0.25)
def _fetch_image_batch(
    file_urls: List[str], batch_size: int = 32
) -> List[Tuple[str, PILImage.Image]]:
    """Fetch a batch of images from S3 in parallel.

    This function:
    1. Takes a list of S3 URLs for JPEG images
    2. Fetches each image in parallel using Ray tasks
    3. Returns a list of (URL, PIL Image) tuples, with None for failed fetches

    Args:
        file_urls: List of S3 URLs to fetch (e.g., "s3://bucket/path/to/image.jpg")
        batch_size: Number of images to fetch in parallel (default: 10)

    Returns:
        List of (file_url, PIL Image) tuples where:
            - file_url: The original S3 URL
            - PIL Image: The loaded image, or None if fetch failed
    """
    # Create S3 client for this task
    s3_client = boto3.client("s3")
    results = []

    for file_url in file_urls:
        try:
            # Parse S3 URL into bucket and key
            bucket = file_url.replace("s3://", "").split("/")[0]
            key = "/".join(file_url.replace("s3://", "").split("/")[1:])

            # Fetch and decode image
            response = s3_client.get_object(Bucket=bucket, Key=key)
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


class S3JpegImageIterableDataset(S3JpegReader, IterableDataset):
    """An iterable dataset that loads images from S3-stored JPEG files.

    This dataset:
    1. Reads JPEG files from S3
    2. Processes images with optional random transforms
    3. Yields (image, label) tensors
    4. Supports row limits per worker for controlled data processing
    """

    LOG_FREQUENCY = 1000  # Log progress every 1000 rows
    BATCH_SIZE = 10  # Number of images to fetch in parallel

    def __init__(
        self,
        file_urls: List[str],
        random_transforms: bool = True,
        limit_rows_per_worker: Optional[int] = None,
        batch_size: int = 10,
    ):
        """Initialize the dataset.

        Args:
            file_urls: List of S3 URLs to load
            random_transforms: Whether to use random transforms for training
            limit_rows_per_worker: Maximum number of rows to process per worker
            batch_size: Number of images to fetch in parallel
        """
        super().__init__()
        self.file_urls = file_urls
        self.limit_rows_per_worker = limit_rows_per_worker
        self.random_transforms = random_transforms
        self.batch_size = batch_size

        worker_rank = ray.train.get_context().get_world_rank()
        logger.info(
            f"[S3JpegImageIterableDataset] Worker {worker_rank}: Initialized with {len(file_urls)} "
            f"files{f' (limit: {limit_rows_per_worker} rows)' if limit_rows_per_worker else ''} "
            f"(batch size: {batch_size})"
        )

    def _fetch_images_batch(
        self, file_urls: List[str]
    ) -> List[Tuple[str, PILImage.Image]]:
        """Fetch a batch of images from S3 in parallel.

        Args:
            file_urls: List of S3 URLs to fetch

        Returns:
            List of (file_url, PIL Image) tuples

        Raises:
            Exception: If there's an error fetching the batch
        """
        try:
            future = _fetch_image_batch.remote(file_urls, self.batch_size)
            results = ray.get(future)

            valid_results = [(url, img) for url, img in results if img is not None]
            if len(valid_results) < len(results):
                logger.warning(
                    f"[S3JpegImageIterableDataset] Failed to fetch {len(results) - len(valid_results)} "
                    "images"
                )

            return valid_results
        except Exception as e:
            logger.error(f"[S3JpegImageIterableDataset] Error fetching batch: {str(e)}")
            raise

    def __iter__(self) -> Iterator[Tuple[torch.Tensor, torch.Tensor]]:
        """Main iteration method that processes files and yields (image, label) tensors.

        This method:
        1. Distributes files among workers
        2. Processes rows with image transforms
        3. Converts to tensors
        4. Respects row limits per worker

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
            if worker_info is None:
                files_to_read = self.file_urls
            else:
                files_to_read = self.file_urls[worker_id::num_workers]

            logger.info(
                f"[S3JpegImageIterableDataset] Worker {worker_id}/{num_workers}: Processing "
                f"{len(files_to_read)} files"
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
                                    f"[S3JpegImageIterableDataset] Worker {worker_id}: Processed "
                                    f"{rows_processed} rows ({rows_per_second:.2f} rows/sec)"
                                )
                                last_log_time = current_time

                            yield image, label

                        except Exception as e:
                            logger.error(
                                f"[S3JpegImageIterableDataset] Worker {worker_id}: Error processing "
                                f"{file_url}: {str(e)}",
                                exc_info=True,
                            )
                            continue

                except Exception as e:
                    logger.error(
                        f"[S3JpegImageIterableDataset] Worker {worker_id}: Error processing batch: "
                        f"{str(e)}",
                        exc_info=True,
                    )
                    continue

            total_time = time.time() - total_start_time
            logger.info(
                f"[S3JpegImageIterableDataset] Worker {worker_id}: Completed {rows_processed} rows "
                f"in {total_time:.2f}s ({rows_processed/total_time:.2f} rows/sec)"
            )

        except Exception as e:
            logger.error(
                f"[S3JpegImageIterableDataset] Worker {worker_id}: Fatal error: {str(e)}",
                exc_info=True,
            )
            raise
