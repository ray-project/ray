"""S3-specific filesystem implementation using boto3.

This module provides an S3-specific implementation that uses boto3 (AWS SDK for Python)
for reliable and efficient S3 operations.
"""

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Optional, Union

import boto3
from botocore import UNSIGNED
from botocore.client import BaseClient
from botocore.config import Config

from ray.llm._internal.common.observability.logging import get_logger
from ray.llm._internal.common.utils.cloud_filesystem.base import BaseCloudFileSystem

logger = get_logger(__name__)


class S3FileSystem(BaseCloudFileSystem):
    """S3-specific implementation of cloud filesystem operations using boto3.

    This implementation uses boto3 (AWS SDK for Python) for reliable and efficient
    operations with S3 storage.
    """

    @staticmethod
    def _parse_s3_uri(uri: str) -> tuple[str, str, bool]:
        """Parse S3 URI into bucket and key.

        Args:
            uri: S3 URI (e.g., s3://bucket/path/to/object or s3://anonymous@bucket/path/to/object)

        Returns:
            Tuple of (bucket_name, key, is_anonymous)

        Raises:
            ValueError: If URI is not a valid S3 URI
        """
        # Check if anonymous@ prefix exists
        is_anonymous = False
        if uri.startswith("s3://anonymous@"):
            is_anonymous = True
            uri = uri.replace("s3://anonymous@", "s3://", 1)

        if not uri.startswith("s3://"):
            raise ValueError(f"Invalid S3 URI: {uri}")

        # Remove s3:// prefix and split into bucket and key
        path = uri[5:]  # Remove "s3://"
        parts = path.split("/", 1)
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ""

        return bucket, key, is_anonymous

    @staticmethod
    def _get_s3_client(max_pool_connections: int = 50, anonymous: bool = False):
        """Create a new S3 client instance with connection pooling.

        Args:
            max_pool_connections: Maximum number of connections in the pool.
                Should be >= max_workers for optimal performance.
            anonymous: Whether to use anonymous access to S3.

        Returns:
            boto3 S3 client with connection pooling configured
        """
        # Configure connection pooling for better concurrent performance
        config = Config(
            max_pool_connections=max_pool_connections,
            # Retry configuration for transient failures
            retries={
                "max_attempts": 3,
                "mode": "adaptive",  # Adapts retry behavior based on error type
            },
            # TCP keepalive helps with long-running connections
            tcp_keepalive=True,
            signature_version=UNSIGNED if anonymous else None,
        )

        return boto3.client("s3", config=config)

    @staticmethod
    def get_file(
        object_uri: str, decode_as_utf_8: bool = True
    ) -> Optional[Union[str, bytes]]:
        """Download a file from cloud storage into memory.

        Args:
            object_uri: URI of the file (s3://)
            decode_as_utf_8: If True, decode the file as UTF-8

        Returns:
            File contents as string or bytes, or None if file doesn't exist
        """
        try:
            bucket, key, is_anonymous = S3FileSystem._parse_s3_uri(object_uri)
            s3_client = S3FileSystem._get_s3_client(anonymous=is_anonymous)

            # Download file directly into memory
            response = s3_client.get_object(Bucket=bucket, Key=key)
            body = response["Body"].read()

            if decode_as_utf_8:
                return body.decode("utf-8")
            return body
        except Exception as e:
            logger.error(f"Error reading {object_uri}: {e}")

    @staticmethod
    def list_subfolders(folder_uri: str) -> List[str]:
        """List the immediate subfolders in a cloud directory.

        Args:
            folder_uri: URI of the directory (s3://)

        Returns:
            List of subfolder names (without trailing slashes)
        """
        try:
            bucket, prefix, is_anonymous = S3FileSystem._parse_s3_uri(folder_uri)

            # Ensure that the prefix has a trailing slash
            if prefix and not prefix.endswith("/"):
                prefix = f"{prefix}/"

            s3_client = S3FileSystem._get_s3_client(anonymous=is_anonymous)

            # List objects with delimiter to get only immediate subfolders
            response = s3_client.list_objects_v2(
                Bucket=bucket, Prefix=prefix, Delimiter="/"
            )

            subfolders = []
            # CommonPrefixes contains the subdirectories
            for common_prefix in response.get("CommonPrefixes", []):
                folder_path = common_prefix["Prefix"]
                # Extract the folder name from the full prefix
                # Remove the parent prefix and trailing slash
                folder_name = folder_path[len(prefix) :].rstrip("/")
                if folder_name:
                    subfolders.append(folder_name)

            return subfolders
        except Exception as e:
            logger.error(f"Error listing subfolders in {folder_uri}: {e}")
            return []

    @staticmethod
    def _calculate_optimal_workers(
        num_files: int, total_size: int, default_max: int = 100, default_min: int = 10
    ) -> int:
        """Calculate optimal number of workers based on file characteristics.

        Args:
            num_files: Number of files to download
            total_size: Total size of all files in bytes
            default_max: Maximum workers to cap at
            default_min: Minimum workers to use

        Returns:
            Optimal number of workers between default_min and default_max
        """
        if num_files == 0:
            return default_min

        avg_file_size = total_size / num_files if total_size > 0 else 0

        # Strategy: More workers for smaller files, fewer for larger files
        if avg_file_size < 1024 * 1024:  # < 1MB (small files)
            # Use more workers for many small files
            workers = min(num_files, default_max)
        elif avg_file_size < 10 * 1024 * 1024:  # 1-10MB (medium files)
            # Use moderate workers
            workers = min(num_files // 2, default_max // 2)
        else:  # > 10MB (large files)
            # Use fewer workers since each download is bandwidth-intensive
            workers = min(20, num_files)

        # Ensure workers is between min and max
        return max(default_min, min(workers, default_max))

    @staticmethod
    def _download_single_file(
        s3_client: BaseClient, bucket: str, key: str, local_file_path: str
    ) -> tuple[str, bool]:
        """Download a single file from S3.

        Args:
            s3_client: Shared boto3 S3 client
            bucket: S3 bucket name
            key: S3 object key
            local_file_path: Local path where file will be saved

        Returns:
            Tuple of (key, success)
        """
        try:
            # Create parent directories if needed
            os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

            s3_client.download_file(bucket, key, local_file_path)
            logger.debug(f"Downloaded {key} to {local_file_path}")
            return key, True
        except Exception as e:
            logger.error(f"Failed to download {key}: {e}")
            return key, False

    @staticmethod
    def download_files(
        path: str,
        bucket_uri: str,
        substrings_to_include: Optional[List[str]] = None,
        suffixes_to_exclude: Optional[List[str]] = None,
        max_workers: Optional[int] = None,
    ) -> None:
        """Download files from cloud storage to a local directory concurrently.

        Args:
            path: Local directory where files will be downloaded
            bucket_uri: URI of cloud directory
            substrings_to_include: Only include files containing these substrings
            suffixes_to_exclude: Exclude certain files from download (e.g .safetensors)
            max_workers: Maximum number of concurrent downloads. If None, automatically
                calculated based on file count and sizes (min: 10, max: 100)
        """
        try:
            bucket, prefix, is_anonymous = S3FileSystem._parse_s3_uri(bucket_uri)

            # Ensure the destination directory exists
            os.makedirs(path, exist_ok=True)

            # Ensure prefix has trailing slash for directory listing
            if prefix and not prefix.endswith("/"):
                prefix = f"{prefix}/"

            # Create initial client for listing (will recreate with proper pool size later)
            s3_client = S3FileSystem._get_s3_client(anonymous=is_anonymous)

            # List all objects in the bucket with the given prefix
            paginator = s3_client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

            # Collect all files to download and track total size
            files_to_download = []
            total_size = 0

            for page in pages:
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    size = obj.get("Size", 0)

                    # Skip if it's a directory marker
                    if key.endswith("/"):
                        continue

                    # Get the relative path (remove the prefix)
                    relative_path = key[len(prefix) :]

                    # Apply include filters
                    if substrings_to_include:
                        if not any(
                            substr in relative_path for substr in substrings_to_include
                        ):
                            continue

                    # Apply exclude filters
                    if suffixes_to_exclude:
                        if any(
                            relative_path.endswith(suffix.lstrip("*"))
                            for suffix in suffixes_to_exclude
                        ):
                            continue

                    # Construct local file path
                    local_file_path = os.path.join(path, relative_path)
                    files_to_download.append((bucket, key, local_file_path))
                    total_size += size

            # Download files concurrently
            if not files_to_download:
                logger.info(f"No files matching filters to download from {bucket_uri}")
                return

            # Dynamically calculate workers if not provided
            if max_workers is None:
                max_workers = S3FileSystem._calculate_optimal_workers(
                    num_files=len(files_to_download),
                    total_size=total_size,
                    default_max=100,
                    default_min=10,
                )

            # Create shared client with proper connection pool size for downloads
            s3_client = S3FileSystem._get_s3_client(
                max_pool_connections=max_workers + 10, anonymous=is_anonymous
            )

            failed_downloads = []

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all download tasks with shared client
                future_to_key = {
                    executor.submit(
                        S3FileSystem._download_single_file,
                        s3_client,  # Pass shared client to each worker
                        bucket,
                        key,
                        local_path,
                    ): key
                    for bucket, key, local_path in files_to_download
                }

                # Process completed downloads
                for future in as_completed(future_to_key):
                    key, success = future.result()
                    if not success:
                        failed_downloads.append(key)

            # Report any failures
            if failed_downloads:
                logger.error(
                    f"Failed to download {len(failed_downloads)} files: {failed_downloads[:5]}..."
                )

        except Exception as e:
            logger.exception(f"Error downloading files from {bucket_uri}: {e}")
            raise

    @staticmethod
    def upload_files(
        local_path: str,
        bucket_uri: str,
    ) -> None:
        """Upload files to cloud storage.

        Args:
            local_path: The local path of the files to upload.
            bucket_uri: The bucket uri to upload the files to, must start with `s3://`.
        """
        try:
            bucket, prefix, is_anonymous = S3FileSystem._parse_s3_uri(bucket_uri)

            # Ensure prefix has trailing slash for directory upload
            if prefix and not prefix.endswith("/"):
                prefix = f"{prefix}/"

            s3_client = S3FileSystem._get_s3_client(anonymous=is_anonymous)

            local_path_obj = Path(local_path)

            # Walk through the local directory and upload each file
            if local_path_obj.is_file():
                # Upload a single file
                file_name = local_path_obj.name
                s3_key = f"{prefix}{file_name}" if prefix else file_name
                s3_client.upload_file(str(local_path_obj), bucket, s3_key)
                logger.debug(f"Uploaded {local_path_obj} to s3://{bucket}/{s3_key}")
            elif local_path_obj.is_dir():
                # Upload directory recursively
                for file_path in local_path_obj.rglob("*"):
                    if file_path.is_file():
                        # Calculate relative path from local_path
                        relative_path = file_path.relative_to(local_path_obj)
                        # Construct S3 key
                        s3_key = f"{prefix}{relative_path.as_posix()}"
                        # Upload file
                        s3_client.upload_file(str(file_path), bucket, s3_key)
                        logger.debug(f"Uploaded {file_path} to s3://{bucket}/{s3_key}")
            else:
                raise ValueError(
                    f"Path {local_path} does not exist or is not a file/directory"
                )

        except Exception as e:
            logger.exception(f"Error uploading files to {bucket_uri}: {e}")
            raise
