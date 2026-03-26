"""Azure-specific filesystem implementation.

This module provides an Azure-specific implementation that delegates to PyArrowFileSystem.
This maintains backward compatibility while allowing for future optimizations using
native Azure tools (azcopy, azure-storage-blob SDK).
"""

from typing import List, Optional, Union

from ray.llm._internal.common.utils.cloud_filesystem.base import BaseCloudFileSystem
from ray.llm._internal.common.utils.cloud_filesystem.pyarrow_filesystem import (
    PyArrowFileSystem,
)


class AzureFileSystem(BaseCloudFileSystem):
    """Azure-specific implementation of cloud filesystem operations.

    **Note**: This implementation currently delegates to PyArrowFileSystem to maintain
    stability. Optimized implementation using azure-storage-blob SDK and azcopy
    will be added in a future PR.
    """

    @staticmethod
    def get_file(
        object_uri: str, decode_as_utf_8: bool = True
    ) -> Optional[Union[str, bytes]]:
        """Download a file from cloud storage into memory.

        Args:
            object_uri: URI of the file (abfss:// or azure://)
            decode_as_utf_8: If True, decode the file as UTF-8

        Returns:
            File contents as string or bytes, or None if file doesn't exist
        """
        return PyArrowFileSystem.get_file(object_uri, decode_as_utf_8)

    @staticmethod
    def list_subfolders(folder_uri: str) -> List[str]:
        """List the immediate subfolders in a cloud directory.

        Args:
            folder_uri: URI of the directory (abfss:// or azure://)

        Returns:
            List of subfolder names (without trailing slashes)
        """
        return PyArrowFileSystem.list_subfolders(folder_uri)

    @staticmethod
    def download_files(
        path: str,
        bucket_uri: str,
        substrings_to_include: Optional[List[str]] = None,
        suffixes_to_exclude: Optional[List[str]] = None,
    ) -> None:
        """Download files from cloud storage to a local directory.

        Args:
            path: Local directory where files will be downloaded
            bucket_uri: URI of cloud directory
            substrings_to_include: Only include files containing these substrings
            suffixes_to_exclude: Exclude certain files from download (e.g .safetensors)
        """
        PyArrowFileSystem.download_files(
            path, bucket_uri, substrings_to_include, suffixes_to_exclude
        )

    @staticmethod
    def upload_files(
        local_path: str,
        bucket_uri: str,
    ) -> None:
        """Upload files to cloud storage.

        Args:
            local_path: The local path of the files to upload.
            bucket_uri: The bucket uri to upload the files to, must start with
                `abfss://` or `azure://`.
        """
        PyArrowFileSystem.upload_files(local_path, bucket_uri)
