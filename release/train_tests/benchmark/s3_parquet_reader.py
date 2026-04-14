# Standard library imports
from typing import List, Tuple
import logging

# Third-party imports
import boto3
from botocore.exceptions import NoCredentialsError
import ray
import ray.train

# Local imports
from s3_reader import S3Reader, AWS_REGION
from logger_utils import ContextLoggerAdapter

logger = ContextLoggerAdapter(logging.getLogger(__name__))


@ray.remote(num_cpus=0.25)
def _fetch_parquet_metadata(bucket: str, key: str, file_url: str) -> Tuple[str, int]:
    """Fetch Parquet row count using S3 Select in parallel.

    Uses S3 Select to efficiently count rows in a Parquet file without reading
    the entire file contents, enabling balanced distribution based on row counts.

    Args:
        bucket: S3 bucket name containing the Parquet file
        key: S3 object key for the Parquet file
        file_url: Full S3 URL for logging purposes

    Returns:
        Tuple containing:
            - file_url: Full S3 URL of the processed file
            - row_count: Number of rows in the Parquet file
    """
    s3_client = boto3.client("s3", region_name=AWS_REGION)

    # Execute S3 Select query to count rows
    query = "SELECT COUNT(*) FROM s3object"
    response = s3_client.select_object_content(
        Bucket=bucket,
        Key=key,
        Expression=query,
        ExpressionType="SQL",
        InputSerialization={"Parquet": {}},
        OutputSerialization={"CSV": {}},
    )

    # Extract row count from response
    row_count = 0
    for event in response["Payload"]:
        if "Records" in event:
            row_count = int(event["Records"]["Payload"].decode("utf-8").strip())

    logger.info(f"File {file_url} has {row_count} rows")
    return file_url, row_count


class S3ParquetReader(S3Reader):
    """Extended S3Reader class for Parquet-specific functionality.

    Provides specialized methods for:
    1. Collecting Parquet file metadata (row counts) from S3
    2. Distributing files among workers based on row counts
    3. Managing parallel S3 operations with Ray tasks
    """

    def _collect_file_info(
        self, bucket: str, prefix: str
    ) -> Tuple[List[str], List[int]]:
        """Collect file URLs and their row counts in parallel using Ray tasks.

        Lists all Parquet files in the specified S3 prefix and launches parallel
        tasks to count rows in each file using S3 Select for efficient metadata
        collection.

        Args:
            bucket: S3 bucket name to list files from
            prefix: S3 prefix to filter files

        Returns:
            Tuple containing:
                - List of file URLs (e.g., "s3://bucket/path/to/file")
                - List of row counts for each file
        """
        file_urls, _ = self._list_s3_files(bucket, prefix)

        # Launch parallel metadata collection tasks
        tasks = []
        for file_url in file_urls:
            # Extract key from file_url
            key = file_url.replace(f"s3://{bucket}/", "")
            task = _fetch_parquet_metadata.remote(bucket, key, file_url)
            tasks.append(task)

        # Wait for all tasks to complete
        worker_rank = ray.train.get_context().get_world_rank()
        logger.info(
            f"Worker {worker_rank}: Waiting for metadata from {len(tasks)} files..."
        )
        results = ray.get(tasks)

        # Process results
        file_urls, file_rows = zip(*results) if results else ([], [])

        logger.info(
            f"Worker {worker_rank}: Collected metadata for {len(file_urls)} files"
        )
        return list(file_urls), list(file_rows)

    def _get_file_urls(self, url: str) -> List[str]:
        """Get file URLs from S3 and distribute them among Ray workers.

        Collects file metadata from S3 and distributes files among workers based on
        row counts to ensure balanced workload distribution.

        Args:
            url: S3 URL to list files from (e.g., "s3://bucket/path/to/directory")

        Returns:
            List of S3 URLs assigned to the current Ray worker

        Raises:
            S3CredentialsError: If AWS credentials are not found or invalid
            S3FileError: If there's an error listing files from S3
        """
        try:
            # Get Ray worker configuration
            worker_rank = ray.train.get_context().get_world_rank()
            num_workers = ray.train.get_context().get_world_size()

            # Parse S3 URL components
            bucket, prefix = self._parse_s3_url(url)

            # Collect file metadata for balanced distribution
            logger.info(
                f"Worker {worker_rank}: Collecting file metadata for balanced distribution"
            )
            file_urls, file_rows = self._collect_file_info(bucket, prefix)
            logger.info(f"Found {len(file_urls)} files in {url}")

            # Distribute files based on row counts
            return self._distribute_files(
                file_urls=file_urls,
                file_weights=file_rows,
                worker_rank=worker_rank,
                num_workers=num_workers,
                weight_unit="rows",
            )

        except NoCredentialsError:
            raise self.S3CredentialsError(
                "AWS credentials not found. Ensure you have configured them."
            )
        except Exception as e:
            raise self.S3FileError(f"Error listing files from {url}: {str(e)}")
