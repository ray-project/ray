"""Abstract base class for cloud filesystem implementations.

This module defines the interface that all cloud storage provider implementations
must follow, ensuring consistency across different providers while allowing
provider-specific optimizations.
"""

from abc import ABC, abstractmethod
from typing import List, Optional, Union


class BaseCloudFileSystem(ABC):
    """Abstract base class for cloud filesystem implementations.

    This class defines the interface that all cloud storage provider implementations
    must implement. Provider-specific classes (S3FileSystem, GCSFileSystem, etc.)
    will inherit from this base class and provide optimized implementations for
    their respective cloud storage platforms.
    """

    @staticmethod
    @abstractmethod
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
        pass

    @staticmethod
    @abstractmethod
    def list_subfolders(folder_uri: str) -> List[str]:
        """List the immediate subfolders in a cloud directory.

        Args:
            folder_uri: URI of the directory (s3://, gs://, abfss://, or azure://)

        Returns:
            List of subfolder names (without trailing slashes)
        """
        pass

    @staticmethod
    @abstractmethod
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
        pass

    @staticmethod
    @abstractmethod
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
        pass
