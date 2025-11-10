"""Tests for S3FileSystem class."""

import json
import os
import subprocess
import sys
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from ray.llm._internal.common.utils.cloud_filesystem.s3_filesystem import (
    S3FileSystem,
)


class TestS3FileSystem:
    """Tests for the S3FileSystem class."""

    @patch("os.unlink")
    @patch("os.path.exists")
    @patch("builtins.open", create=True)
    @patch("subprocess.run")
    @patch("tempfile.NamedTemporaryFile")
    @pytest.mark.parametrize(
        "decode_as_utf_8,expected_content,expected_mode",
        [
            (True, "test file content", "r"),
            (False, b"test file content", "rb"),
        ],
    )
    def test_get_file(
        self,
        mock_tempfile,
        mock_run,
        mock_open,
        mock_exists,
        mock_unlink,
        decode_as_utf_8,
        expected_content,
        expected_mode,
    ):
        """Test getting a file from S3 as string or bytes."""
        # Setup mock tempfile
        mock_tmp_file = MagicMock()
        mock_tmp_file.name = "/tmp/test_file_123"
        mock_tmp_file.__enter__.return_value = mock_tmp_file
        mock_tempfile.return_value = mock_tmp_file

        # Setup mock subprocess result
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_run.return_value = mock_result

        # Setup mock file reading
        mock_file = MagicMock()
        mock_file.read.return_value = expected_content
        mock_open.return_value.__enter__.return_value = mock_file
        mock_exists.return_value = True

        # Test getting file
        content = S3FileSystem.get_file(
            "s3://bucket/test.txt", decode_as_utf_8=decode_as_utf_8
        )

        assert content == expected_content
        mock_open.assert_called_once_with("/tmp/test_file_123", expected_mode)

    @patch("subprocess.run")
    @pytest.mark.parametrize(
        "stderr",
        [
            "An error occurred (NoSuchKey) when calling the GetObject operation",
            "The file does not exist",
        ],
    )
    def test_get_file_not_found(self, mock_run, stderr):
        """Test getting a non-existent file from S3."""
        mock_run.side_effect = subprocess.CalledProcessError(
            returncode=1, cmd=["aws", "s3", "cp"], stderr=stderr
        )
        assert S3FileSystem.get_file("s3://bucket/nonexistent.txt") is None

    @patch("subprocess.run")
    def test_get_file_invalid_uri(self, mock_run):
        """Test getting a file with invalid URI."""
        with pytest.raises(ValueError, match="Invalid S3 URI"):
            S3FileSystem.get_file("gs://bucket/test.txt")

    @patch("subprocess.run")
    @pytest.mark.parametrize(
        "uri,expected_path",
        [
            ("s3://bucket/parent", "s3://bucket/parent/"),
            ("s3://bucket/parent/", "s3://bucket/parent/"),
        ],
    )
    def test_list_subfolders(self, mock_run, uri, expected_path):
        """Test listing subfolders in S3."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = (
            "PRE folder1/\nPRE folder2/\n2024-01-01 12:00:00    12345 file.txt\n"
        )
        mock_run.return_value = mock_result

        folders = S3FileSystem.list_subfolders(uri)
        assert sorted(folders) == ["folder1", "folder2"]
        assert mock_run.call_args[0][0][3] == expected_path

    @patch("subprocess.run")
    def test_list_subfolders_exception(self, mock_run):
        """Test listing subfolders when command fails."""
        mock_run.side_effect = Exception("Network error")
        assert S3FileSystem.list_subfolders("s3://bucket/parent") == []

    @patch("subprocess.run")
    def test_list_subfolders_invalid_uri(self, mock_run):
        """Test listing subfolders with invalid URI."""
        with pytest.raises(ValueError, match="Invalid S3 URI"):
            S3FileSystem.list_subfolders("gs://bucket/parent")

    @patch("subprocess.run")
    @pytest.mark.parametrize(
        "uri,expected_path",
        [
            ("s3://bucket/dir", "s3://bucket/dir/"),
            ("s3://bucket/dir/", "s3://bucket/dir/"),
        ],
    )
    def test_download_files(self, mock_run, uri, expected_path):
        """Test downloading files from S3."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_run.return_value = mock_result

        with tempfile.TemporaryDirectory() as tempdir:
            S3FileSystem.download_files(tempdir, uri)
            call_args = mock_run.call_args[0][0]
            assert call_args[3] == expected_path
            assert "--recursive" in call_args

    @patch("subprocess.run")
    def test_download_files_with_filters(self, mock_run):
        """Test downloading files with inclusion and exclusion filters."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_run.return_value = mock_result

        with tempfile.TemporaryDirectory() as tempdir:
            S3FileSystem.download_files(
                tempdir,
                "s3://bucket/dir",
                substrings_to_include=["config", "tokenizer"],
                suffixes_to_exclude=[".tmp", "*.txt"],
            )

            call_args = mock_run.call_args[0][0]
            assert "--exclude" in call_args
            assert "--include" in call_args
            assert "*config*" in call_args
            assert "*tokenizer*" in call_args
            assert "*.tmp" in call_args
            assert "*.txt" in call_args

    @patch("subprocess.run")
    def test_download_files_exception(self, mock_run):
        """Test downloading files when command fails."""
        mock_run.side_effect = Exception("Network error")
        with tempfile.TemporaryDirectory() as tempdir:
            with pytest.raises(Exception, match="Network error"):
                S3FileSystem.download_files(tempdir, "s3://bucket/dir")

    @patch("subprocess.run")
    def test_download_files_invalid_uri(self, mock_run):
        """Test downloading files with invalid URI."""
        with tempfile.TemporaryDirectory() as tempdir:
            with pytest.raises(ValueError, match="Invalid S3 URI"):
                S3FileSystem.download_files(tempdir, "gs://bucket/dir")

    @patch("subprocess.run")
    @pytest.mark.parametrize(
        "uri,expected_path",
        [
            ("s3://bucket/dir", "s3://bucket/dir/"),
            ("s3://bucket/dir/", "s3://bucket/dir/"),
        ],
    )
    def test_upload_files(self, mock_run, uri, expected_path):
        """Test uploading files to S3."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_run.return_value = mock_result

        with tempfile.TemporaryDirectory() as tempdir:
            S3FileSystem.upload_files(tempdir, uri)
            call_args = mock_run.call_args[0][0]
            assert call_args[4] == expected_path
            assert "--recursive" in call_args

    @patch("subprocess.run")
    def test_upload_files_exception(self, mock_run):
        """Test uploading files when command fails."""
        mock_run.side_effect = Exception("Network error")
        with tempfile.TemporaryDirectory() as tempdir:
            with pytest.raises(Exception, match="Network error"):
                S3FileSystem.upload_files(tempdir, "s3://bucket/dir")

    @patch("subprocess.run")
    def test_upload_files_invalid_uri(self, mock_run):
        """Test uploading files with invalid URI."""
        with tempfile.TemporaryDirectory() as tempdir:
            with pytest.raises(ValueError, match="Invalid S3 URI"):
                S3FileSystem.upload_files(tempdir, "gs://bucket/dir")

    @patch("subprocess.run")
    def test_run_command_success(self, mock_run):
        """Test _run_command with successful execution."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_run.return_value = mock_result
        result = S3FileSystem._run_command(["aws", "s3", "ls"])
        assert result == mock_result
        mock_run.assert_called_once_with(
            ["aws", "s3", "ls"], capture_output=True, text=True, check=True
        )

    @patch("subprocess.run")
    def test_run_command_file_not_found(self, mock_run):
        """Test _run_command when command is not found."""
        mock_run.side_effect = FileNotFoundError()
        with pytest.raises(FileNotFoundError, match="is not installed"):
            S3FileSystem._run_command(["nonexistent", "command"])

    @patch("subprocess.run")
    def test_run_command_called_process_error(self, mock_run):
        """Test _run_command when command fails."""
        mock_run.side_effect = subprocess.CalledProcessError(
            returncode=1, cmd=["aws", "s3", "cp"], stderr="Access Denied"
        )
        with pytest.raises(subprocess.CalledProcessError):
            S3FileSystem._run_command(["aws", "s3", "cp", "s3://bucket/file", "/tmp"])


class TestS3FileSystemIntegration:
    """Integration tests for S3FileSystem (requires actual S3 access)."""

    def test_list_subfolders_real_s3(self):
        """Test listing subfolders from real S3 bucket."""
        # Test listing subfolders in the parent directory which has actual subfolders
        folders = S3FileSystem.list_subfolders("s3://air-example-data/rayllm-ossci/")
        # Verify we get expected subfolders
        assert isinstance(folders, list)
        assert "meta-Llama-3.2-1B-Instruct" in folders
        assert len(folders) > 0

    def test_get_file_real_s3(self):
        """Test getting a file from real S3 bucket."""
        # Test getting a small config file
        content = S3FileSystem.get_file(
            "s3://air-example-data/rayllm-ossci/meta-Llama-3.2-1B-Instruct/config.json"
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
                "s3://air-example-data/rayllm-ossci/meta-Llama-3.2-1B-Instruct/",
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
                "s3://air-example-data/rayllm-ossci/meta-Llama-3.2-1B-Instruct/",
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
