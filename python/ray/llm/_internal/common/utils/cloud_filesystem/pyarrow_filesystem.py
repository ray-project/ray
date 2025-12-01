"""PyArrow-based filesystem implementation for cloud storage.

This module provides a PyArrow-based implementation of the cloud filesystem
interface, supporting S3, GCS, and Azure storage providers.
"""

import os
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional, Tuple, Union
from urllib.parse import urlparse

import pyarrow.fs as pa_fs

from ray.llm._internal.common.observability.logging import get_logger
from ray.llm._internal.common.utils.cloud_filesystem.base import BaseCloudFileSystem

logger = get_logger(__name__)


class PyArrowFileSystem(BaseCloudFileSystem):
    """PyArrow-based implementation of cloud filesystem operations.

    This class provides a unified interface for cloud storage operations using
    PyArrow's filesystem abstraction. It supports S3, GCS, and Azure storage
    providers.
    """

    @staticmethod
    def get_fs_and_path(object_uri: str) -> Tuple[pa_fs.FileSystem, str]:
        """Get the appropriate filesystem and path from a URI.

        Args:
            object_uri: URI of the file (s3://, gs://, abfss://, or azure://)
                If URI contains 'anonymous@', anonymous access is used.
                Example: s3://anonymous@bucket/path

        Returns:
            Tuple of (filesystem, path)
        """

        if object_uri.startswith("pyarrow-"):
            object_uri = object_uri[8:]

        anonymous = False
        # Check for anonymous access pattern (only for S3/GCS)
        # e.g. s3://anonymous@bucket/path
        if "@" in object_uri and not (
            object_uri.startswith("abfss://") or object_uri.startswith("azure://")
        ):
            parts = object_uri.split("@", 1)
            # Check if the first part ends with "anonymous"
            if parts[0].endswith("anonymous"):
                anonymous = True
                # Remove the anonymous@ part, keeping the scheme
                scheme = parts[0].split("://")[0]
                object_uri = f"{scheme}://{parts[1]}"

        if object_uri.startswith("s3://"):
            endpoint = os.getenv("AWS_ENDPOINT_URL_S3", None)
            virtual_hosted_style = os.getenv("AWS_S3_ADDRESSING_STYLE", None)
            fs = pa_fs.S3FileSystem(
                anonymous=anonymous,
                endpoint_override=endpoint,
                force_virtual_addressing=(virtual_hosted_style == "virtual"),
            )
            path = object_uri[5:]  # Remove "s3://"
        elif object_uri.startswith("gs://"):
            fs = pa_fs.GcsFileSystem(anonymous=anonymous)
            path = object_uri[5:]  # Remove "gs://"
        elif object_uri.startswith("abfss://"):
            fs, path = PyArrowFileSystem._create_abfss_filesystem(object_uri)
        elif object_uri.startswith("azure://"):
            fs, path = PyArrowFileSystem._create_azure_filesystem(object_uri)
        else:
            raise ValueError(f"Unsupported URI scheme: {object_uri}")

        return fs, path

    @staticmethod
    def _create_azure_filesystem(object_uri: str) -> Tuple[pa_fs.FileSystem, str]:
        """Create an Azure filesystem for Azure Blob Storage or ABFSS.

        Args:
            object_uri: Azure URI (azure://container@account.blob.core.windows.net/path or
                       abfss://container@account.dfs.core.windows.net/path)

        Returns:
            Tuple of (PyArrow FileSystem, path without scheme prefix)

        Raises:
            ImportError: If required dependencies are not installed.
            ValueError: If the Azure URI format is invalid.
        """
        try:
            import adlfs
            from azure.identity import DefaultAzureCredential
        except ImportError:
            raise ImportError(
                "You must `pip install adlfs azure-identity` "
                "to use Azure/ABFSS URIs. "
                "Note that these must be preinstalled on all nodes in the Ray cluster."
            )

        # Parse and validate the Azure URI
        parsed = urlparse(object_uri)
        scheme = parsed.scheme.lower()

        # Validate URI format: scheme://container@account.domain/path
        if not parsed.netloc or "@" not in parsed.netloc:
            raise ValueError(
                f"Invalid {scheme.upper()} URI format - missing container@account: {object_uri}"
            )

        container_part, hostname_part = parsed.netloc.split("@", 1)

        # Validate container name (must be non-empty)
        if not container_part:
            raise ValueError(
                f"Invalid {scheme.upper()} URI format - empty container name: {object_uri}"
            )

        # Validate hostname format based on scheme
        valid_hostname = False
        if scheme == "abfss":
            valid_hostname = hostname_part.endswith(".dfs.core.windows.net")
            expected_domains = ".dfs.core.windows.net"
        elif scheme == "azure":
            valid_hostname = hostname_part.endswith(
                ".blob.core.windows.net"
            ) or hostname_part.endswith(".dfs.core.windows.net")
            expected_domains = ".blob.core.windows.net or .dfs.core.windows.net"

        if not hostname_part or not valid_hostname:
            raise ValueError(
                f"Invalid {scheme.upper()} URI format - invalid hostname (must end with {expected_domains}): {object_uri}"
            )

        # Extract and validate account name
        azure_storage_account_name = hostname_part.split(".")[0]
        if not azure_storage_account_name:
            raise ValueError(
                f"Invalid {scheme.upper()} URI format - empty account name: {object_uri}"
            )

        # Create the adlfs filesystem
        adlfs_fs = adlfs.AzureBlobFileSystem(
            account_name=azure_storage_account_name,
            credential=DefaultAzureCredential(),
        )

        # Wrap with PyArrow's PyFileSystem for compatibility
        fs = pa_fs.PyFileSystem(pa_fs.FSSpecHandler(adlfs_fs))

        # Return the path without the scheme prefix
        path = f"{container_part}{parsed.path}"

        return fs, path

    @staticmethod
    def _create_abfss_filesystem(object_uri: str) -> Tuple[pa_fs.FileSystem, str]:
        """Create an ABFSS filesystem for Azure Data Lake Storage Gen2.

        This is a wrapper around _create_azure_filesystem for backward compatibility.

        Args:
            object_uri: ABFSS URI (abfss://container@account.dfs.core.windows.net/path)

        Returns:
            Tuple of (PyArrow FileSystem, path without abfss:// prefix)
        """
        return PyArrowFileSystem._create_azure_filesystem(object_uri)

    @staticmethod
    def _filter_files(
        fs: pa_fs.FileSystem,
        source_path: str,
        destination_path: str,
        substrings_to_include: Optional[List[str]] = None,
        suffixes_to_exclude: Optional[List[str]] = None,
    ) -> List[Tuple[str, str]]:
        """Filter files from cloud storage based on inclusion and exclusion criteria.

        Args:
            fs: PyArrow filesystem instance
            source_path: Source path in cloud storage
            destination_path: Local destination path
            substrings_to_include: Only include files containing these substrings
            suffixes_to_exclude: Exclude files ending with these suffixes

        Returns:
            List of tuples containing (source_file_path, destination_file_path)
        """
        file_selector = pa_fs.FileSelector(source_path, recursive=True)
        file_infos = fs.get_file_info(file_selector)

        path_pairs = []
        for file_info in file_infos:
            if file_info.type != pa_fs.FileType.File:
                continue

            rel_path = file_info.path[len(source_path) :].lstrip("/")

            # Apply filters
            if substrings_to_include:
                if not any(
                    substring in rel_path for substring in substrings_to_include
                ):
                    continue

            if suffixes_to_exclude:
                if any(rel_path.endswith(suffix) for suffix in suffixes_to_exclude):
                    continue

            path_pairs.append(
                (file_info.path, os.path.join(destination_path, rel_path))
            )

        return path_pairs

    @staticmethod
    def get_file(
        object_uri: str, decode_as_utf_8: bool = True
    ) -> Optional[Union[str, bytes]]:
        """Download a file from cloud storage into memory.

        Args:
            object_uri: URI of the file (s3://, gs://, abfss://, or azure://)
            decode_as_utf_8: If True, decode the file as UTF-8

        Returns:
            File contents as string or bytes, or None if file doesn't exist
        """
        try:
            fs, path = PyArrowFileSystem.get_fs_and_path(object_uri)

            # Check if file exists
            if not fs.get_file_info(path).type == pa_fs.FileType.File:
                logger.info(f"URI {object_uri} does not exist.")
                return None

            # Read file
            with fs.open_input_file(path) as f:
                body = f.read()

            if decode_as_utf_8:
                body = body.decode("utf-8")
            return body
        except Exception as e:
            logger.warning(f"Error reading {object_uri}: {e}")
            return None

    @staticmethod
    def list_subfolders(folder_uri: str) -> List[str]:
        """List the immediate subfolders in a cloud directory.

        Args:
            folder_uri: URI of the directory (s3://, gs://, abfss://, or azure://)

        Returns:
            List of subfolder names (without trailing slashes)
        """
        # Ensure that the folder_uri has a trailing slash.
        folder_uri = f"{folder_uri.rstrip('/')}/"

        try:
            fs, path = PyArrowFileSystem.get_fs_and_path(folder_uri)

            # List directory contents
            file_infos = fs.get_file_info(pa_fs.FileSelector(path, recursive=False))

            # Filter for directories and extract subfolder names
            subfolders = []
            for file_info in file_infos:
                if file_info.type == pa_fs.FileType.Directory:
                    # Extract just the subfolder name without the full path
                    subfolder = os.path.basename(file_info.path.rstrip("/"))
                    subfolders.append(subfolder)

            return subfolders
        except Exception as e:
            logger.error(f"Error listing subfolders in {folder_uri}: {e}")
            return []

    @staticmethod
    def download_files(
        path: str,
        bucket_uri: str,
        substrings_to_include: Optional[List[str]] = None,
        suffixes_to_exclude: Optional[List[str]] = None,
        max_concurrency: int = 10,
        chunk_size: int = 64 * 1024 * 1024,
    ) -> None:
        """Download files from cloud storage to a local directory.

        Args:
            path: Local directory where files will be downloaded
            bucket_uri: URI of cloud directory
            substrings_to_include: Only include files containing these substrings
            suffixes_to_exclude: Exclude certain files from download
            max_concurrency: Maximum number of concurrent files to download (default: 10)
            chunk_size: Size of transfer chunks (default: 64MB)
        """
        try:
            fs, source_path = PyArrowFileSystem.get_fs_and_path(bucket_uri)

            # Ensure destination exists
            os.makedirs(path, exist_ok=True)

            # If no filters, use direct copy_files
            if not substrings_to_include and not suffixes_to_exclude:
                pa_fs.copy_files(
                    source=source_path,
                    destination=path,
                    source_filesystem=fs,
                    destination_filesystem=pa_fs.LocalFileSystem(),
                    use_threads=True,
                    chunk_size=chunk_size,
                )
                return

            # List and filter files
            files_to_download = PyArrowFileSystem._filter_files(
                fs, source_path, path, substrings_to_include, suffixes_to_exclude
            )

            if not files_to_download:
                logger.info("Filters do not match any of the files, skipping download")
                return

            def download_single_file(file_paths):
                source_file_path, dest_file_path = file_paths
                # Create destination directory if needed
                dest_dir = os.path.dirname(dest_file_path)
                if dest_dir:
                    os.makedirs(dest_dir, exist_ok=True)

                # Use PyArrow's copy_files for individual files,
                pa_fs.copy_files(
                    source=source_file_path,
                    destination=dest_file_path,
                    source_filesystem=fs,
                    destination_filesystem=pa_fs.LocalFileSystem(),
                    use_threads=True,
                    chunk_size=chunk_size,
                )
                return dest_file_path

            max_workers = min(max_concurrency, len(files_to_download))
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [
                    executor.submit(download_single_file, file_paths)
                    for file_paths in files_to_download
                ]

                for future in futures:
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f"Failed to download file: {e}")
                        raise

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
            bucket_uri: The bucket uri to upload the files to, must start with
                `s3://`, `gs://`, `abfss://`, or `azure://`.
        """
        try:
            fs, dest_path = PyArrowFileSystem.get_fs_and_path(bucket_uri)

            pa_fs.copy_files(
                source=local_path,
                destination=dest_path,
                source_filesystem=pa_fs.LocalFileSystem(),
                destination_filesystem=fs,
            )
        except Exception as e:
            logger.exception(f"Error uploading files to {bucket_uri}: {e}")
            raise
