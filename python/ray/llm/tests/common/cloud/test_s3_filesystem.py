"""Tests for S3FileSystem class."""

import json
import os
import sys
import tempfile
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from ray.llm._internal.common.utils.cloud_filesystem.s3_filesystem import (
    S3FileSystem,
)


class TestS3FileSystem:
    """Tests for the S3FileSystem class."""

    @patch("boto3.client")
    @pytest.mark.parametrize(
        "decode_as_utf_8,file_content,expected_content",
        [
            (True, b"test file content", "test file content"),
            (False, b"test file content", b"test file content"),
        ],
    )
    def test_get_file(
        self,
        mock_boto_client,
        decode_as_utf_8,
        file_content,
        expected_content,
    ):
        """Test getting a file from S3 as string or bytes."""
        # Setup mock S3 client
        mock_s3_client = MagicMock()
        mock_boto_client.return_value = mock_s3_client

        # Mock get_object response
        mock_body = MagicMock()
        mock_body.read.return_value = file_content
        mock_s3_client.get_object.return_value = {
            "Body": mock_body,
            "ContentLength": len(file_content),
        }

        # Test getting file
        content = S3FileSystem.get_file(
            "s3://bucket/test.txt", decode_as_utf_8=decode_as_utf_8
        )

        assert content == expected_content
        mock_s3_client.get_object.assert_called_once_with(
            Bucket="bucket", Key="test.txt"
        )

    @patch("boto3.client")
    def test_get_file_not_found(self, mock_boto_client):
        """Test getting a non-existent file from S3."""
        # Setup mock S3 client
        mock_s3_client = MagicMock()
        mock_boto_client.return_value = mock_s3_client

        # Simulate NoSuchKey error
        mock_s3_client.get_object.side_effect = ClientError(
            {
                "Error": {
                    "Code": "NoSuchKey",
                    "Message": "The specified key does not exist.",
                }
            },
            "GetObject",
        )

        assert S3FileSystem.get_file("s3://bucket/nonexistent.txt") is None

    @patch("boto3.client")
    def test_get_file_anonymous(self, mock_boto_client):
        """Test getting a file from S3 with anonymous access."""
        # Setup mock S3 client
        mock_s3_client = MagicMock()
        mock_boto_client.return_value = mock_s3_client

        # Mock get_object response
        mock_body = MagicMock()
        mock_body.read.return_value = b"anonymous content"
        mock_s3_client.get_object.return_value = {
            "Body": mock_body,
        }

        # Test getting file with anonymous URI
        content = S3FileSystem.get_file("s3://anonymous@bucket/test.txt")

        assert content == "anonymous content"
        # Verify anonymous config was used (UNSIGNED signature)
        assert mock_boto_client.called

    @patch("boto3.client")
    @pytest.mark.parametrize(
        "uri,expected_prefix",
        [
            ("s3://bucket/parent", "parent/"),
            ("s3://bucket/parent/", "parent/"),
        ],
    )
    def test_list_subfolders(self, mock_boto_client, uri, expected_prefix):
        """Test listing subfolders in S3."""
        # Setup mock S3 client
        mock_s3_client = MagicMock()
        mock_boto_client.return_value = mock_s3_client

        # Mock list_objects_v2 response
        mock_s3_client.list_objects_v2.return_value = {
            "CommonPrefixes": [
                {"Prefix": f"{expected_prefix}folder1/"},
                {"Prefix": f"{expected_prefix}folder2/"},
            ]
        }

        folders = S3FileSystem.list_subfolders(uri)
        assert sorted(folders) == ["folder1", "folder2"]
        mock_s3_client.list_objects_v2.assert_called_once_with(
            Bucket="bucket", Prefix=expected_prefix, Delimiter="/"
        )

    @patch("boto3.client")
    def test_list_subfolders_exception(self, mock_boto_client):
        """Test listing subfolders when operation fails."""
        # Setup mock S3 client
        mock_s3_client = MagicMock()
        mock_boto_client.return_value = mock_s3_client

        mock_s3_client.list_objects_v2.side_effect = Exception("Network error")
        assert S3FileSystem.list_subfolders("s3://bucket/parent") == []

    def test_list_subfolders_invalid_uri(self):
        """Test listing subfolders with invalid URI."""
        # list_subfolders catches all exceptions and returns empty list
        result = S3FileSystem.list_subfolders("gs://bucket/parent")
        assert result == []

    @patch("boto3.client")
    @pytest.mark.parametrize(
        "uri,expected_prefix",
        [
            ("s3://bucket/dir", "dir/"),
            ("s3://bucket/dir/", "dir/"),
        ],
    )
    def test_download_files(self, mock_boto_client, uri, expected_prefix):
        """Test downloading files from S3."""
        # Setup mock S3 client
        mock_s3_client = MagicMock()
        mock_boto_client.return_value = mock_s3_client

        # Mock paginator
        mock_paginator = MagicMock()
        mock_s3_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {
                "Contents": [
                    {"Key": f"{expected_prefix}file1.txt", "Size": 100},
                    {"Key": f"{expected_prefix}file2.txt", "Size": 200},
                ]
            }
        ]

        # Mock download_file to do nothing
        mock_s3_client.download_file = MagicMock()

        with tempfile.TemporaryDirectory() as tempdir:
            S3FileSystem.download_files(tempdir, uri, max_workers=2)

            # Verify paginator was called correctly
            mock_s3_client.get_paginator.assert_called_with("list_objects_v2")
            mock_paginator.paginate.assert_called_once_with(
                Bucket="bucket", Prefix=expected_prefix
            )

            # Verify files were downloaded
            assert mock_s3_client.download_file.call_count == 2

    @patch("boto3.client")
    def test_download_files_with_filters(self, mock_boto_client):
        """Test downloading files with inclusion and exclusion filters."""
        # Setup mock S3 client
        mock_s3_client = MagicMock()
        mock_boto_client.return_value = mock_s3_client

        # Mock paginator with various files
        mock_paginator = MagicMock()
        mock_s3_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "dir/config.json", "Size": 100},
                    {"Key": "dir/tokenizer.json", "Size": 200},
                    {"Key": "dir/data.tmp", "Size": 300},
                    {"Key": "dir/readme.txt", "Size": 400},
                    {"Key": "dir/other.bin", "Size": 500},
                ]
            }
        ]

        # Mock download_file to do nothing
        mock_s3_client.download_file = MagicMock()

        with tempfile.TemporaryDirectory() as tempdir:
            S3FileSystem.download_files(
                tempdir,
                "s3://bucket/dir",
                substrings_to_include=["config", "tokenizer"],
                suffixes_to_exclude=[".tmp", ".txt"],
                max_workers=2,
            )

            # Should only download config.json and tokenizer.json
            # (included by substring, not excluded by suffix)
            assert mock_s3_client.download_file.call_count == 2

            # Get the keys that were downloaded
            downloaded_keys = [
                call[0][1] for call in mock_s3_client.download_file.call_args_list
            ]
            assert "dir/config.json" in downloaded_keys
            assert "dir/tokenizer.json" in downloaded_keys

    @patch("boto3.client")
    def test_download_files_no_matching_files(self, mock_boto_client):
        """Test downloading when no files match filters."""
        # Setup mock S3 client
        mock_s3_client = MagicMock()
        mock_boto_client.return_value = mock_s3_client

        # Mock paginator with files that won't match
        mock_paginator = MagicMock()
        mock_s3_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "dir/file1.txt", "Size": 100},
                ]
            }
        ]

        with tempfile.TemporaryDirectory() as tempdir:
            # This should not raise, just return without downloading
            S3FileSystem.download_files(
                tempdir,
                "s3://bucket/dir",
                substrings_to_include=["nonexistent"],
                max_workers=2,
            )

            # Verify no files were downloaded
            mock_s3_client.download_file.assert_not_called()

    @patch("boto3.client")
    @patch("ray.llm._internal.common.utils.cloud_filesystem.s3_filesystem.logger")
    def test_download_files_concurrent_failure(self, mock_logger, mock_boto_client):
        """Test downloading files when some downloads fail."""
        # Setup mock S3 client
        mock_s3_client = MagicMock()
        mock_boto_client.return_value = mock_s3_client

        # Mock paginator
        mock_paginator = MagicMock()
        mock_s3_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "dir/file1.txt", "Size": 100},
                    {"Key": "dir/file2.txt", "Size": 200},
                ]
            }
        ]

        # Make download_file fail
        mock_s3_client.download_file.side_effect = Exception("Download failed")

        with tempfile.TemporaryDirectory() as tempdir:
            # Should complete without raising, but log errors
            S3FileSystem.download_files(tempdir, "s3://bucket/dir", max_workers=2)

            # Verify error was logged for failed downloads
            mock_logger.error.assert_called()
            error_call = mock_logger.error.call_args_list[0][0][0]
            assert "Failed to download" in error_call

    def test_download_files_invalid_uri(self):
        """Test downloading files with invalid URI."""
        with tempfile.TemporaryDirectory() as tempdir:
            with pytest.raises(ValueError, match="Invalid S3 URI"):
                S3FileSystem.download_files(tempdir, "gs://bucket/dir")

    @patch("boto3.client")
    @pytest.mark.parametrize(
        "uri,expected_prefix",
        [
            ("s3://bucket/dir", "dir/"),
            ("s3://bucket/dir/", "dir/"),
        ],
    )
    def test_upload_files_directory(self, mock_boto_client, uri, expected_prefix):
        """Test uploading a directory to S3."""
        # Setup mock S3 client
        mock_s3_client = MagicMock()
        mock_boto_client.return_value = mock_s3_client
        mock_s3_client.upload_file = MagicMock()

        with tempfile.TemporaryDirectory() as tempdir:
            # Create some test files
            test_file1 = os.path.join(tempdir, "file1.txt")
            test_file2 = os.path.join(tempdir, "subdir", "file2.txt")
            os.makedirs(os.path.dirname(test_file2), exist_ok=True)

            with open(test_file1, "w") as f:
                f.write("test1")
            with open(test_file2, "w") as f:
                f.write("test2")

            S3FileSystem.upload_files(tempdir, uri)

            # Verify files were uploaded
            assert mock_s3_client.upload_file.call_count == 2

            # Check the S3 keys that were used
            uploaded_keys = [
                call[0][2] for call in mock_s3_client.upload_file.call_args_list
            ]
            assert f"{expected_prefix}file1.txt" in uploaded_keys
            assert f"{expected_prefix}subdir/file2.txt" in uploaded_keys

    @patch("boto3.client")
    def test_upload_files_single_file(self, mock_boto_client):
        """Test uploading a single file to S3."""
        # Setup mock S3 client
        mock_s3_client = MagicMock()
        mock_boto_client.return_value = mock_s3_client
        mock_s3_client.upload_file = MagicMock()

        with tempfile.TemporaryDirectory() as tempdir:
            # Create a test file
            test_file = os.path.join(tempdir, "single.txt")
            with open(test_file, "w") as f:
                f.write("test content")

            S3FileSystem.upload_files(test_file, "s3://bucket/dir/")

            # Verify single file was uploaded
            mock_s3_client.upload_file.assert_called_once()
            call_args = mock_s3_client.upload_file.call_args[0]
            assert call_args[2] == "dir/single.txt"

    @patch("boto3.client")
    def test_upload_files_exception(self, mock_boto_client):
        """Test uploading files when operation fails."""
        # Setup mock S3 client
        mock_s3_client = MagicMock()
        mock_boto_client.return_value = mock_s3_client
        mock_s3_client.upload_file.side_effect = Exception("Network error")

        with tempfile.TemporaryDirectory() as tempdir:
            # Create a test file
            test_file = os.path.join(tempdir, "test.txt")
            with open(test_file, "w") as f:
                f.write("test")

            with pytest.raises(Exception, match="Network error"):
                S3FileSystem.upload_files(tempdir, "s3://bucket/dir")

    def test_upload_files_invalid_uri(self):
        """Test uploading files with invalid URI."""
        with tempfile.TemporaryDirectory() as tempdir:
            with pytest.raises(ValueError, match="Invalid S3 URI"):
                S3FileSystem.upload_files(tempdir, "gs://bucket/dir")

    def test_upload_files_nonexistent_path(self):
        """Test uploading from a path that doesn't exist."""
        with pytest.raises(ValueError, match="does not exist"):
            S3FileSystem.upload_files("/nonexistent/path", "s3://bucket/dir")

    def test_parse_s3_uri(self):
        """Test parsing S3 URIs."""
        # Standard URI
        bucket, key, is_anon = S3FileSystem._parse_s3_uri(
            "s3://bucket/path/to/file.txt"
        )
        assert bucket == "bucket"
        assert key == "path/to/file.txt"
        assert is_anon is False

        # Anonymous URI
        bucket, key, is_anon = S3FileSystem._parse_s3_uri(
            "s3://anonymous@bucket/file.txt"
        )
        assert bucket == "bucket"
        assert key == "file.txt"
        assert is_anon is True

        # Bucket only
        bucket, key, is_anon = S3FileSystem._parse_s3_uri("s3://bucket")
        assert bucket == "bucket"
        assert key == ""
        assert is_anon is False

    def test_calculate_optimal_workers(self):
        """Test worker calculation based on file characteristics."""
        # Many small files (< 1MB)
        workers = S3FileSystem._calculate_optimal_workers(
            num_files=50, total_size=50 * 500 * 1024  # 50 files * 500KB each
        )
        assert workers == 50  # Should use many workers for small files

        # Medium files (1-10MB)
        workers = S3FileSystem._calculate_optimal_workers(
            num_files=50, total_size=50 * 5 * 1024 * 1024  # 50 files * 5MB each
        )
        assert workers == 25  # Should use moderate workers

        # Large files (> 10MB)
        workers = S3FileSystem._calculate_optimal_workers(
            num_files=50, total_size=50 * 50 * 1024 * 1024  # 50 files * 50MB each
        )
        assert workers == 20  # Should cap at 20 for large files

        # Zero files
        workers = S3FileSystem._calculate_optimal_workers(num_files=0, total_size=0)
        assert workers == 10  # Should return default_min


class TestS3FileSystemIntegration:
    """Integration tests for S3FileSystem (requires actual S3 access)."""

    def test_list_subfolders_real_s3(self):
        """Test listing subfolders from real S3 bucket."""
        # Test listing subfolders in the parent directory which has actual subfolders
        folders = S3FileSystem.list_subfolders(
            "s3://anonymous@air-example-data/rayllm-ossci/"
        )
        # Verify we get expected subfolders
        assert isinstance(folders, list)
        assert "meta-Llama-3.2-1B-Instruct" in folders
        assert len(folders) > 0

    def test_get_file_real_s3(self):
        """Test getting a file from real S3 bucket."""
        # Test getting a small config file
        content = S3FileSystem.get_file(
            "s3://anonymous@air-example-data/rayllm-ossci/meta-Llama-3.2-1B-Instruct/config.json"
        )
        assert content is not None
        assert isinstance(content, str)
        # Verify it's valid JSON
        config = json.loads(content)
        assert "model_type" in config or "vocab_size" in config

    def test_download_files_with_exclusion(self):
        """Test downloading files with exclusion filter (exclude safetensors files)."""
        with tempfile.TemporaryDirectory() as tempdir:
            # Download files excluding safetensors
            S3FileSystem.download_files(
                tempdir,
                "s3://anonymous@air-example-data/rayllm-ossci/meta-Llama-3.2-1B-Instruct/",
                suffixes_to_exclude=[".safetensors"],
            )

            # Get list of downloaded files
            downloaded_files = set()
            for root, dirs, files in os.walk(tempdir):
                for file in files:
                    rel_path = os.path.relpath(os.path.join(root, file), tempdir)
                    downloaded_files.add(rel_path)

            # Verify safetensors file is excluded
            assert (
                "model.safetensors" not in downloaded_files
            ), "safetensors file should be excluded"

            # Verify other files are downloaded
            assert "config.json" in downloaded_files
            assert "tokenizer.json" in downloaded_files
            assert len(downloaded_files) > 0

    def test_download_files_with_inclusion(self):
        """Test downloading files with inclusion filter (include only .json files)."""
        with tempfile.TemporaryDirectory() as tempdir:
            # Download only .json files
            S3FileSystem.download_files(
                tempdir,
                "s3://anonymous@air-example-data/rayllm-ossci/meta-Llama-3.2-1B-Instruct/",
                substrings_to_include=[".json"],
            )

            # Get list of downloaded files
            downloaded_files = set()
            for root, dirs, files in os.walk(tempdir):
                for file in files:
                    rel_path = os.path.relpath(os.path.join(root, file), tempdir)
                    downloaded_files.add(rel_path)

            # Verify only .json files are downloaded
            expected_json_files = {
                "config.json",
                "generation_config.json",
                "special_tokens_map.json",
                "tokenizer.json",
                "tokenizer_config.json",
            }
            assert (
                downloaded_files == expected_json_files
            ), f"Expected {expected_json_files}, got {downloaded_files}"

            # Verify non-json files are excluded
            assert "model.safetensors" not in downloaded_files
            assert "README.md" not in downloaded_files
            assert "LICENSE.txt" not in downloaded_files


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
