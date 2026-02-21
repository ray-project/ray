# Standard library imports
from typing import List
import logging

# Third-party imports
from botocore.exceptions import NoCredentialsError
import ray
import ray.train

# Local imports
from s3_reader import S3Reader
from logger_utils import ContextLoggerAdapter

logger = ContextLoggerAdapter(logging.getLogger(__name__))


class S3JpegReader(S3Reader):
    """Extended S3Reader class for JPEG-specific functionality.

    Provides specialized methods for:
    1. Collecting JPEG file metadata (sizes) from S3
    2. Distributing files among workers based on file sizes
    3. Managing parallel S3 operations with Ray tasks
    """

    def _get_file_urls(self, url: str) -> List[str]:
        """Get file URLs from S3 and distribute them among Ray workers.

        Collects file metadata from S3 and distributes files among workers based on
        file sizes to ensure balanced workload distribution.

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
            file_urls, file_size_bytes = self._list_s3_files(bucket, prefix)
            logger.info(f"Found {len(file_urls)} files in {url}")

            # Distribute files based on size
            return self._distribute_files(
                file_urls=file_urls,
                file_weights=file_size_bytes,
                worker_rank=worker_rank,
                num_workers=num_workers,
                weight_unit="bytes",
            )

        except NoCredentialsError:
            raise self.S3CredentialsError(
                "AWS credentials not found. Ensure you have configured them."
            )
        except Exception as e:
            raise self.S3FileError(f"Error listing files from {url}: {str(e)}")
