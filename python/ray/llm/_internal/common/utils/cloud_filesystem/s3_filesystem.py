"""S3-specific filesystem implementation using AWS CLI.

This module provides an S3-specific implementation that uses AWS CLI for optimal
performance. This leverages native AWS tools for significantly faster transfers
compared to PyArrow-based implementations.
"""

import os
import subprocess
import tempfile
from typing import List, Optional, Union

from ray.llm._internal.common.observability.logging import get_logger
from ray.llm._internal.common.utils.cloud_filesystem.base import BaseCloudFileSystem

logger = get_logger(__name__)


class S3FileSystem(BaseCloudFileSystem):
    """S3-specific implementation of cloud filesystem operations using AWS CLI.

    This implementation uses AWS CLI (aws s3 cp, aws s3 ls) for optimal performance
    when working with S3 storage. It provides significantly faster transfers
    compared to PyArrow-based implementations, especially for large files.
    """

    @staticmethod
    def _run_command(cmd: List[str]) -> subprocess.CompletedProcess:
        """Run a command and handle errors.

        Args:
            cmd: List of command arguments (e.g., ['aws', 's3', 'cp', ...])

        Returns:
            CompletedProcess object from subprocess.run

        Raises:
            subprocess.CalledProcessError: If the command fails
            FileNotFoundError: If the command is not installed
        """
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
            )
            return result
        except FileNotFoundError:
            raise FileNotFoundError(f"Command '{cmd[0]}' is not installed.")
        except subprocess.CalledProcessError as e:
            print(f"Command failed: {' '.join(cmd)}")
            print(f"Error: {e.stderr}")
            logger.error(f"Command failed: {' '.join(cmd)}")
            logger.error(f"Error output: {e.stderr}")
            raise

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
        if not object_uri.startswith("s3://"):
            raise ValueError(f"Invalid S3 URI: {object_uri}")

        try:
            # Create a temporary file to download to
            with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                tmp_path = tmp_file.name

            try:
                # Download file using AWS CLI
                cmd = ["aws", "s3", "cp", object_uri, tmp_path]
                S3FileSystem._run_command(cmd)

                # Read the file
                mode = "r" if decode_as_utf_8 else "rb"
                with open(tmp_path, mode) as f:
                    body = f.read()

                return body
            finally:
                # Clean up temporary file
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)

        except subprocess.CalledProcessError as e:
            # Check if file doesn't exist (AWS CLI returns non-zero exit code)
            if "NoSuchKey" in e.stderr or "does not exist" in e.stderr.lower():
                logger.info(f"URI {object_uri} does not exist.")
                return None
            logger.info(f"Error reading {object_uri}: {e.stderr}")
            return None
        except Exception as e:
            logger.info(f"Error reading {object_uri}: {e}")
            return None

    @staticmethod
    def list_subfolders(folder_uri: str) -> List[str]:
        """List the immediate subfolders in a cloud directory.

        Args:
            folder_uri: URI of the directory (s3://)

        Returns:
            List of subfolder names (without trailing slashes)
        """
        if not folder_uri.startswith("s3://"):
            raise ValueError(f"Invalid S3 URI: {folder_uri}")

        # Ensure that the folder_uri has a trailing slash.
        folder_uri = f"{folder_uri.rstrip('/')}/"

        try:
            # Use AWS CLI to list objects with common prefix
            cmd = ["aws", "s3", "ls", folder_uri]
            result = S3FileSystem._run_command(cmd)

            subfolders = []
            for line in result.stdout.strip().split("\n"):
                if not line.strip():
                    continue
                # AWS CLI ls output format: "PRE folder_name/" or "timestamp size key"
                # We're looking for lines starting with "PRE" (prefixes/directories)
                if line.startswith("PRE"):
                    # Extract folder name: "PRE folder_name/" -> "folder_name"
                    folder_name = line.split()[-1].rstrip("/")
                    subfolders.append(folder_name)

            return subfolders
        except Exception as e:
            logger.info(f"Error listing subfolders in {folder_uri}: {e}")
            return []

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
        if not bucket_uri.startswith("s3://"):
            raise ValueError(f"Invalid S3 URI: {bucket_uri}")

        try:
            # Ensure the destination directory exists
            os.makedirs(path, exist_ok=True)

            # Ensure bucket_uri has trailing slash for directory listing
            source_uri = f"{bucket_uri.rstrip('/')}/"

            # Build AWS CLI command
            cmd = ["aws", "s3", "cp", source_uri, path, "--recursive"]

            # AWS CLI filter logic:
            # - By default, all files are included
            # - --exclude removes files matching the pattern
            # - --include adds files matching the pattern (even if excluded)
            # - Order matters: filters are processed sequentially

            # If we have include filters, we need to exclude everything first,
            # then include only what we want
            if substrings_to_include:
                # Exclude everything first
                cmd.extend(["--exclude", "*"])
                # Then include files matching any of the substring patterns
                for substring in substrings_to_include:
                    # Create wildcard pattern: *substring* matches files containing substring
                    pattern = f"*{substring}*"
                    cmd.extend(["--include", pattern])

            # Add exclude filters (suffixes_to_exclude)
            # These will exclude files ending with the specified suffixes
            if suffixes_to_exclude:
                for suffix in suffixes_to_exclude:
                    # Ensure suffix starts with * if it doesn't already
                    if not suffix.startswith("*"):
                        pattern = f"*{suffix}"
                    else:
                        pattern = suffix
                    cmd.extend(["--exclude", pattern])

            # Run the download command
            S3FileSystem._run_command(cmd)

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
        if not bucket_uri.startswith("s3://"):
            raise ValueError(f"Invalid S3 URI: {bucket_uri}")

        try:
            # Ensure bucket_uri has trailing slash for directory upload
            dest_uri = f"{bucket_uri.rstrip('/')}/"

            # Build AWS CLI command for recursive upload
            cmd = ["aws", "s3", "cp", local_path, dest_uri, "--recursive"]

            # Run the upload command
            S3FileSystem._run_command(cmd)

        except Exception as e:
            logger.exception(f"Error uploading files to {bucket_uri}: {e}")
            raise
