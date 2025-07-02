# Standard library imports
from typing import Tuple, List, Optional
import logging

# Third-party imports
import boto3
import ray
import ray.train

# Local imports
from logger_utils import ContextLoggerAdapter

# AWS configuration
AWS_REGION = "us-west-2"

logger = ContextLoggerAdapter(logging.getLogger(__name__))


@ray.remote(num_cpus=0.25)
def _list_s3_batch(
    bucket: str,
    prefix: str,
    continuation_token: Optional[str] = None,
    batch_size: int = 1000,
) -> Tuple[List[Tuple[str, int]], Optional[str]]:
    """List a batch of files from S3 in parallel.

    Makes a paginated request to S3's list_objects_v2 API to efficiently list files
    in batches. Each file is returned with its size in bytes.

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
    s3_client = boto3.client("s3")

    # Prepare request parameters
    list_params = {
        "Bucket": bucket,
        "Prefix": prefix,
        "MaxKeys": batch_size,
    }
    if continuation_token:
        list_params["ContinuationToken"] = continuation_token

    # List objects from S3
    response = s3_client.list_objects_v2(**list_params)

    # Return empty results if no files found
    if "Contents" not in response:
        return [], None

    # Extract file URLs and sizes
    batch_files = response["Contents"]
    results = [(f"s3://{bucket}/{f['Key']}", f["Size"]) for f in batch_files]

    # Get token for next batch
    next_token = (
        response.get("NextContinuationToken") if response.get("IsTruncated") else None
    )

    return results, next_token


class S3Reader:
    """Base class for reading files from S3.

    Provides common functionality for:
    1. S3 client initialization and management
    2. URL parsing and validation
    3. File listing with pagination
    4. Worker-based file distribution
    5. Error handling for S3 operations
    """

    class S3Error(Exception):
        """Base exception for S3-related errors."""

        pass

    class S3CredentialsError(S3Error):
        """Raised when AWS credentials are not found or invalid."""

        pass

    class S3FileError(S3Error):
        """Raised when there's an error accessing S3 files."""

        pass

    def __init__(self) -> None:
        """Initialize the S3Reader with lazy client initialization."""
        self._s3_client = None

    @property
    def s3_client(self) -> "boto3.client":
        """Get or create the S3 client with AWS region configuration.

        Uses lazy initialization to avoid serialization issues with Ray.

        Returns:
            boto3.client: Configured S3 client
        """
        if self._s3_client is None:
            self._s3_client = boto3.client("s3", region_name=AWS_REGION)
        return self._s3_client

    def _parse_s3_url(self, s3_url: str) -> Tuple[str, str]:
        """Parse an S3 URL into bucket and key components.

        Args:
            s3_url: S3 URL in format "s3://bucket/key"

        Returns:
            Tuple[str, str]: (bucket, key) components

        Raises:
            S3FileError: If URL is not a valid S3 URL
        """
        if not s3_url.startswith("s3://"):
            raise self.S3FileError(f"Invalid S3 URL format: {s3_url}")
        s3_parts = s3_url.replace("s3://", "").split("/", 1)
        return s3_parts[0], s3_parts[1]

    def _list_s3_files(self, bucket: str, prefix: str) -> Tuple[List[str], List[int]]:
        """List files in an S3 bucket with the given prefix.

        Uses Ray tasks to make parallel requests to S3's list_objects_v2 API,
        handling pagination automatically. Returns file URLs and their sizes.

        Args:
            bucket: S3 bucket name
            prefix: S3 prefix to filter files

        Returns:
            Tuple containing:
                - List of file URLs (e.g., "s3://bucket/path/to/file")
                - List of file sizes in bytes
        """
        file_urls = []
        file_sizes = []
        continuation_token = None
        batch_size = 1000  # Maximum allowed by S3 API

        while True:
            # Get next batch of files
            batch_results, next_token = ray.get(
                _list_s3_batch.remote(
                    bucket=bucket,
                    prefix=prefix,
                    continuation_token=continuation_token,
                    batch_size=batch_size,
                )
            )

            # Handle empty results
            if not batch_results:
                if not file_urls:  # Only warn on first request
                    logger.info(
                        f"No files found in s3://{bucket}/{prefix}", level="warning"
                    )
                break

            # Process batch results
            batch_urls, batch_sizes = zip(*batch_results)
            file_urls.extend(batch_urls)
            file_sizes.extend(batch_sizes)

            # Log progress
            logger.info(f"Listed {len(file_urls)} files from s3://{bucket}/{prefix}")

            # Continue if there are more files
            if not next_token:
                break
            continuation_token = next_token

        return file_urls, file_sizes

    def _distribute_files(
        self,
        file_urls: List[str],
        file_weights: List[int],
        worker_rank: int,
        num_workers: int,
        weight_unit: str = "units",
    ) -> List[str]:
        """Distribute files among workers based on weights.

        Uses a greedy algorithm to distribute files among workers while trying to
        minimize the difference in total weight between workers. Files are sorted
        by weight (descending) before distribution for better balance.

        Args:
            file_urls: List of file URLs to distribute
            file_weights: List of weights for each file (e.g., size, row count)
            worker_rank: Current worker's rank
            num_workers: Total number of workers
            weight_unit: Unit of measurement for weights (e.g., "bytes", "rows")

        Returns:
            List of file URLs assigned to this worker
        """
        # Sort files by weight
        files_with_weights = sorted(
            zip(file_urls, file_weights), key=lambda x: x[1], reverse=True
        )
        file_urls = [f[0] for f in files_with_weights]
        file_weights = [f[1] for f in files_with_weights]

        # Handle single worker case
        if num_workers <= 1 or not file_urls:
            logger.info(
                f"Worker {worker_rank}: Single worker or no files, "
                f"returning all {len(file_urls)} files with total {sum(file_weights)} "
                f"{weight_unit}"
            )
            return file_urls

        # Calculate target weight per worker
        total_weight = sum(file_weights)
        target_weight_per_worker = total_weight / num_workers
        logger.info(
            f"Worker {worker_rank}: Total {weight_unit}: {total_weight}, "
            f"Target per worker: {target_weight_per_worker:.0f} {weight_unit}"
        )

        # Initialize worker assignments
        worker_files = [[] for _ in range(num_workers)]
        worker_weights = [0] * num_workers

        # Distribute files using greedy algorithm
        for file_url, weight in zip(file_urls, file_weights):
            min_weight_worker = min(range(num_workers), key=lambda w: worker_weights[w])
            worker_files[min_weight_worker].append(file_url)
            worker_weights[min_weight_worker] += weight

        # Get this worker's assignment
        my_files = worker_files[worker_rank]
        my_weight = worker_weights[worker_rank]

        logger.info(
            f"Worker {worker_rank}: Assigned {len(my_files)}/{len(file_urls)} "
            f"files with {my_weight}/{total_weight} {weight_unit} "
            f"({my_weight/total_weight*100:.1f}%)"
        )
        return my_files
