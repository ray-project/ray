"""Tests for PyArrowFileSystem class."""

import os
import sys
import tempfile
from unittest.mock import ANY, MagicMock, patch

import pyarrow.fs as pa_fs
import pytest

from ray.llm._internal.common.utils.cloud_filesystem.pyarrow_filesystem import (
    PyArrowFileSystem,
)


class TestPyArrowFileSystem:
    """Tests for the PyArrowFileSystem class."""

    @patch("pyarrow.fs.S3FileSystem")
    def test_get_file(self, mock_s3fs):
        """Test getting a file from cloud storage."""
        # Setup mock filesystem and file
        mock_fs = MagicMock()
        mock_s3fs.return_value = mock_fs

        # Mock file content and info
        mock_file = MagicMock()
        mock_file.read.return_value = b"test file content"
        mock_fs.open_input_file.return_value.__enter__.return_value = mock_file
        mock_fs.get_file_info.return_value.type = pa_fs.FileType.File

        # Test getting file as string (default)
        content = PyArrowFileSystem.get_file("s3://bucket/test.txt")
        assert content == "test file content"

        # Test getting file as bytes
        content_bytes = PyArrowFileSystem.get_file(
            "s3://bucket/test.txt", decode_as_utf_8=False
        )
        assert content_bytes == b"test file content"

        # Test non-existent file
        mock_fs.get_file_info.return_value.type = pa_fs.FileType.NotFound
        assert PyArrowFileSystem.get_file("s3://bucket/nonexistent.txt") is None

    @patch("pyarrow.fs.GcsFileSystem")
    def test_list_subfolders(self, mock_gcsfs):
        """Test listing subfolders in cloud storage."""
        # Setup mock filesystem
        mock_fs = MagicMock()
        mock_gcsfs.return_value = mock_fs

        # Create mock file infos for directory listing
        dir1 = MagicMock()
        dir1.type = pa_fs.FileType.Directory
        dir1.path = "bucket/parent/dir1"

        dir2 = MagicMock()
        dir2.type = pa_fs.FileType.Directory
        dir2.path = "bucket/parent/dir2"

        file1 = MagicMock()
        file1.type = pa_fs.FileType.File
        file1.path = "bucket/parent/file.txt"

        mock_fs.get_file_info.return_value = [dir1, dir2, file1]

        # Test listing subfolders
        folders = PyArrowFileSystem.list_subfolders("gs://bucket/parent")
        assert sorted(folders) == ["dir1", "dir2"]

    @patch.object(PyArrowFileSystem, "get_fs_and_path")
    def test_list_subfolders_exception_handling(self, mock_get_fs_and_path):
        """Test that list_subfolders returns empty list when get_fs_and_path raises exception."""
        # Make get_fs_and_path raise an exception
        mock_get_fs_and_path.side_effect = ValueError("Example exception")

        # Test that list_subfolders handles the exception gracefully
        folders = PyArrowFileSystem.list_subfolders("gs://bucket/parent")
        assert folders == []

        # Verify get_fs_and_path was called
        mock_get_fs_and_path.assert_called_once_with("gs://bucket/parent/")

    @patch("pyarrow.fs.copy_files")
    @patch("pyarrow.fs.S3FileSystem")
    def test_download_files_no_filters(self, mock_s3fs, mock_copy_files):
        """Test downloading files from cloud storage without filters."""

        # Setup mock filesystem
        mock_fs = MagicMock()
        mock_s3fs.return_value = mock_fs

        # Create temp directory for testing
        with tempfile.TemporaryDirectory() as tempdir:
            # Test downloading files without filters
            PyArrowFileSystem.download_files(tempdir, "s3://bucket/dir")

            # Verify copy_files was called with correct arguments
            mock_copy_files.assert_called_once_with(
                source="bucket/dir",
                destination=tempdir,
                source_filesystem=mock_fs,
                destination_filesystem=ANY,
                use_threads=True,
                chunk_size=64 * 1024 * 1024,
            )

    @patch("pyarrow.fs.copy_files")
    @patch("pyarrow.fs.S3FileSystem")
    def test_download_files_with_filters(self, mock_s3fs, mock_copy_files):
        """Test downloading files from cloud storage with filters."""

        # Setup mock filesystem
        mock_fs = MagicMock()
        mock_s3fs.return_value = mock_fs

        # Create mock file infos for listing
        file_info1 = MagicMock()
        file_info1.type = pa_fs.FileType.File
        file_info1.path = "bucket/dir/file1.txt"

        file_info2 = MagicMock()
        file_info2.type = pa_fs.FileType.File
        file_info2.path = "bucket/dir/subdir/file2.json"

        file_info3 = MagicMock()
        file_info3.type = pa_fs.FileType.File
        file_info3.path = "bucket/dir/file3.tmp"

        dir_info = MagicMock()
        dir_info.type = pa_fs.FileType.Directory
        dir_info.path = "bucket/dir/subdir"

        mock_fs.get_file_info.return_value = [
            file_info1,
            file_info2,
            file_info3,
            dir_info,
        ]

        # Create temp directory for testing
        with tempfile.TemporaryDirectory() as tempdir:
            # Test downloading files with filters
            PyArrowFileSystem.download_files(
                tempdir,
                "s3://bucket/dir",
                substrings_to_include=["file1", "file2"],
                suffixes_to_exclude=[".tmp"],
            )

            # Verify copy_files was called for each filtered file
            assert mock_copy_files.call_count == 2

            # Get all calls to copy_files
            calls = mock_copy_files.call_args_list

            # Verify the calls (order may vary due to threading)
            expected_sources = {"bucket/dir/file1.txt", "bucket/dir/subdir/file2.json"}
            expected_dests = {
                os.path.join(tempdir, "file1.txt"),
                os.path.join(tempdir, "subdir", "file2.json"),
            }

            actual_sources = {call.kwargs["source"] for call in calls}
            actual_dests = {call.kwargs["destination"] for call in calls}

            assert actual_sources == expected_sources
            assert actual_dests == expected_dests

            # Verify all calls have correct filesystem and options
            for call in calls:
                assert call.kwargs["source_filesystem"] == mock_fs
                assert call.kwargs["destination_filesystem"] is not None
                assert call.kwargs["use_threads"] is True
                assert call.kwargs["chunk_size"] == 64 * 1024 * 1024

    @patch("pyarrow.fs.copy_files")
    @patch("pyarrow.fs.S3FileSystem")
    def test_upload_files(self, mock_s3fs, mock_copy_files):
        """Test uploading files to cloud storage."""

        # Setup mock filesystem
        mock_fs = MagicMock()
        mock_s3fs.return_value = mock_fs

        # Create temp directory for testing
        with tempfile.TemporaryDirectory() as tempdir:
            # Test uploading files
            PyArrowFileSystem.upload_files(tempdir, "s3://bucket/dir")

            # Check that the files are copied
            mock_copy_files.assert_called_once_with(
                source=tempdir,
                destination="bucket/dir",
                source_filesystem=ANY,
                destination_filesystem=ANY,
            )


class TestFilterFiles:
    """Tests for the _filter_files method in PyArrowFileSystem."""

    def test_filter_files_no_filters(self):
        """Test filtering files with no inclusion or exclusion filters."""
        # Setup mock filesystem
        mock_fs = MagicMock()

        # Create mock file infos
        file_info1 = MagicMock()
        file_info1.type = pa_fs.FileType.File
        file_info1.path = "bucket/model/file1.txt"

        file_info2 = MagicMock()
        file_info2.type = pa_fs.FileType.File
        file_info2.path = "bucket/model/subdir/file2.json"

        dir_info = MagicMock()
        dir_info.type = pa_fs.FileType.Directory
        dir_info.path = "bucket/model/subdir"

        mock_fs.get_file_info.return_value = [file_info1, file_info2, dir_info]

        # Test filtering with no filters
        result = PyArrowFileSystem._filter_files(
            fs=mock_fs, source_path="bucket/model", destination_path="/local/dest"
        )

        # Should include all files, exclude directories
        expected = [
            ("bucket/model/file1.txt", "/local/dest/file1.txt"),
            ("bucket/model/subdir/file2.json", "/local/dest/subdir/file2.json"),
        ]
        assert sorted(result) == sorted(expected)

        # Verify filesystem was called correctly
        mock_fs.get_file_info.assert_called_once()
        call_args = mock_fs.get_file_info.call_args[0][0]
        assert call_args.base_dir == "bucket/model"
        assert call_args.recursive is True

    def test_filter_files_with_inclusion_substrings(self):
        """Test filtering files with inclusion substrings."""
        # Setup mock filesystem
        mock_fs = MagicMock()

        # Create mock file infos
        file_info1 = MagicMock()
        file_info1.type = pa_fs.FileType.File
        file_info1.path = "bucket/model/config.json"

        file_info2 = MagicMock()
        file_info2.type = pa_fs.FileType.File
        file_info2.path = "bucket/model/weights.bin"

        file_info3 = MagicMock()
        file_info3.type = pa_fs.FileType.File
        file_info3.path = "bucket/model/tokenizer.json"

        mock_fs.get_file_info.return_value = [file_info1, file_info2, file_info3]

        # Test filtering with inclusion substrings
        result = PyArrowFileSystem._filter_files(
            fs=mock_fs,
            source_path="bucket/model",
            destination_path="/local/dest",
            substrings_to_include=["config", "tokenizer"],
        )

        # Should only include files with "config" or "tokenizer" in path
        expected = [
            ("bucket/model/config.json", "/local/dest/config.json"),
            ("bucket/model/tokenizer.json", "/local/dest/tokenizer.json"),
        ]
        assert sorted(result) == sorted(expected)

    def test_filter_files_with_exclusion_suffixes(self):
        """Test filtering files with exclusion suffixes."""
        # Setup mock filesystem
        mock_fs = MagicMock()

        # Create mock file infos
        file_info1 = MagicMock()
        file_info1.type = pa_fs.FileType.File
        file_info1.path = "bucket/model/model.bin"

        file_info2 = MagicMock()
        file_info2.type = pa_fs.FileType.File
        file_info2.path = "bucket/model/config.json"

        file_info3 = MagicMock()
        file_info3.type = pa_fs.FileType.File
        file_info3.path = "bucket/model/temp.tmp"

        file_info4 = MagicMock()
        file_info4.type = pa_fs.FileType.File
        file_info4.path = "bucket/model/log.txt"

        mock_fs.get_file_info.return_value = [
            file_info1,
            file_info2,
            file_info3,
            file_info4,
        ]

        # Test filtering with exclusion suffixes
        result = PyArrowFileSystem._filter_files(
            fs=mock_fs,
            source_path="bucket/model",
            destination_path="/local/dest",
            suffixes_to_exclude=[".tmp", ".txt"],
        )

        # Should exclude files ending with .tmp or .txt
        expected = [
            ("bucket/model/model.bin", "/local/dest/model.bin"),
            ("bucket/model/config.json", "/local/dest/config.json"),
        ]
        assert sorted(result) == sorted(expected)

    def test_filter_files_with_both_filters(self):
        """Test filtering files with both inclusion and exclusion filters."""
        # Setup mock filesystem
        mock_fs = MagicMock()

        # Create mock file infos
        file_info1 = MagicMock()
        file_info1.type = pa_fs.FileType.File
        file_info1.path = "bucket/model/config.json"

        file_info2 = MagicMock()
        file_info2.type = pa_fs.FileType.File
        file_info2.path = "bucket/model/config.tmp"

        file_info3 = MagicMock()
        file_info3.type = pa_fs.FileType.File
        file_info3.path = "bucket/model/weights.bin"

        file_info4 = MagicMock()
        file_info4.type = pa_fs.FileType.File
        file_info4.path = "bucket/model/tokenizer.json"

        mock_fs.get_file_info.return_value = [
            file_info1,
            file_info2,
            file_info3,
            file_info4,
        ]

        # Test filtering with both inclusion and exclusion
        result = PyArrowFileSystem._filter_files(
            fs=mock_fs,
            source_path="bucket/model",
            destination_path="/local/dest",
            substrings_to_include=["config", "tokenizer"],
            suffixes_to_exclude=[".tmp"],
        )

        # Should include files with "config" or "tokenizer" but exclude .tmp files
        expected = [
            ("bucket/model/config.json", "/local/dest/config.json"),
            ("bucket/model/tokenizer.json", "/local/dest/tokenizer.json"),
        ]
        assert sorted(result) == sorted(expected)


class TestPyArrowFileSystemAzureSupport:
    """Tests for Azure/ABFSS support in PyArrowFileSystem."""

    @patch("adlfs.AzureBlobFileSystem")
    @patch("azure.identity.DefaultAzureCredential")
    @patch("pyarrow.fs.PyFileSystem")
    @patch("pyarrow.fs.FSSpecHandler")
    def test_get_fs_and_path_abfss(
        self, mock_handler, mock_pyfs, mock_cred, mock_adlfs
    ):
        """Test getting ABFSS filesystem and path."""
        mock_adlfs_instance = MagicMock()
        mock_adlfs.return_value = mock_adlfs_instance
        mock_pyfs_instance = MagicMock()
        mock_pyfs.return_value = mock_pyfs_instance

        fs, path = PyArrowFileSystem.get_fs_and_path(
            "abfss://container@account.dfs.core.windows.net/path/to/file"
        )

        assert fs == mock_pyfs_instance
        assert path == "container/path/to/file"

        # Verify the adlfs filesystem was created with correct parameters
        mock_adlfs.assert_called_once_with(
            account_name="account", credential=mock_cred.return_value
        )
        mock_handler.assert_called_once_with(mock_adlfs_instance)
        mock_pyfs.assert_called_once_with(mock_handler.return_value)

    @patch("adlfs.AzureBlobFileSystem")
    @patch("azure.identity.DefaultAzureCredential")
    @patch("pyarrow.fs.PyFileSystem")
    @patch("pyarrow.fs.FSSpecHandler")
    def test_get_fs_and_path_azure(
        self, mock_handler, mock_pyfs, mock_cred, mock_adlfs
    ):
        """Test getting Azure filesystem and path."""
        mock_adlfs_instance = MagicMock()
        mock_adlfs.return_value = mock_adlfs_instance
        mock_pyfs_instance = MagicMock()
        mock_pyfs.return_value = mock_pyfs_instance

        fs, path = PyArrowFileSystem.get_fs_and_path(
            "azure://container@account.blob.core.windows.net/path/to/file"
        )

        assert fs == mock_pyfs_instance
        assert path == "container/path/to/file"

        # Verify the adlfs filesystem was created with correct parameters
        mock_adlfs.assert_called_once_with(
            account_name="account", credential=mock_cred.return_value
        )

    def test_abfss_uri_validation(self):
        """Test ABFSS URI validation."""
        # Test valid URIs
        valid_uris = [
            "abfss://container@account.dfs.core.windows.net/path",
            "abfss://my-container@myaccount.dfs.core.windows.net/deep/nested/path",
        ]

        for uri in valid_uris:
            with patch("adlfs.AzureBlobFileSystem"), patch(
                "azure.identity.DefaultAzureCredential"
            ), patch("pyarrow.fs.PyFileSystem"), patch("pyarrow.fs.FSSpecHandler"):
                # Should not raise an exception
                PyArrowFileSystem._create_abfss_filesystem(uri)

        # Test invalid URIs
        invalid_uris = [
            "abfss://container",  # Missing @account
            "abfss://@account.dfs.core.windows.net/path",  # Empty container
            "abfss://container@account.wrong.domain/path",  # Wrong domain
            "abfss://container@.dfs.core.windows.net/path",  # Empty account
            "abfss://container@account.dfs.core.windows.net",  # No path (but this is actually valid)
        ]

        for uri in invalid_uris[:-1]:  # Skip the last one as it's actually valid
            with pytest.raises(ValueError):
                PyArrowFileSystem._create_abfss_filesystem(uri)

    def test_azure_uri_validation(self):
        """Test Azure URI validation."""
        # Test valid URIs
        valid_uris = [
            "azure://container@account.blob.core.windows.net/path",
            "azure://container@account.dfs.core.windows.net/path",
            "azure://my-container@myaccount.blob.core.windows.net/deep/nested/path",
        ]

        for uri in valid_uris:
            with patch("adlfs.AzureBlobFileSystem"), patch(
                "azure.identity.DefaultAzureCredential"
            ), patch("pyarrow.fs.PyFileSystem"), patch("pyarrow.fs.FSSpecHandler"):
                # Should not raise an exception
                PyArrowFileSystem._create_azure_filesystem(uri)

        # Test invalid URIs
        invalid_uris = [
            "azure://container",  # Missing @account
            "azure://@account.blob.core.windows.net/path",  # Empty container
            "azure://container@account.wrong.domain/path",  # Wrong domain
            "azure://container@.blob.core.windows.net/path",  # Empty account
        ]

        for uri in invalid_uris:
            with pytest.raises(ValueError):
                PyArrowFileSystem._create_azure_filesystem(uri)

    def test_abfss_import_error(self):
        """Test ImportError when adlfs is not available."""
        with patch(
            "builtins.__import__", side_effect=ImportError("No module named 'adlfs'")
        ):
            with pytest.raises(
                ImportError, match="You must `pip install adlfs azure-identity`"
            ):
                PyArrowFileSystem._create_abfss_filesystem(
                    "abfss://container@account.dfs.core.windows.net/path"
                )

    def test_azure_import_error(self):
        """Test ImportError when adlfs is not available for Azure."""
        with patch(
            "builtins.__import__", side_effect=ImportError("No module named 'adlfs'")
        ):
            with pytest.raises(
                ImportError, match="You must `pip install adlfs azure-identity`"
            ):
                PyArrowFileSystem._create_azure_filesystem(
                    "azure://container@account.blob.core.windows.net/path"
                )

    @patch("adlfs.AzureBlobFileSystem")
    @patch("azure.identity.DefaultAzureCredential")
    @patch("pyarrow.fs.PyFileSystem")
    @patch("pyarrow.fs.FSSpecHandler")
    def test_abfss_anonymous_access_ignored(
        self, mock_handler, mock_pyfs, mock_cred, mock_adlfs
    ):
        """Test that anonymous access pattern is ignored for ABFSS URIs."""
        mock_adlfs_instance = MagicMock()
        mock_adlfs.return_value = mock_adlfs_instance
        mock_pyfs_instance = MagicMock()
        mock_pyfs.return_value = mock_pyfs_instance

        # ABFSS URI with @ symbol should not trigger anonymous access logic
        fs, path = PyArrowFileSystem.get_fs_and_path(
            "abfss://container@account.dfs.core.windows.net/path"
        )

        assert fs == mock_pyfs_instance
        assert path == "container/path"

        # Verify that DefaultAzureCredential was used, not anonymous access
        mock_cred.assert_called_once()
        mock_adlfs.assert_called_once_with(
            account_name="account", credential=mock_cred.return_value
        )

    @patch("adlfs.AzureBlobFileSystem")
    @patch("azure.identity.DefaultAzureCredential")
    @patch("pyarrow.fs.PyFileSystem")
    @patch("pyarrow.fs.FSSpecHandler")
    def test_get_file_abfss(self, mock_handler, mock_pyfs, mock_cred, mock_adlfs):
        """Test getting a file from ABFSS storage."""
        # Setup mock filesystem and file
        mock_adlfs_instance = MagicMock()
        mock_adlfs.return_value = mock_adlfs_instance
        mock_fs = MagicMock()
        mock_pyfs.return_value = mock_fs

        # Mock file content and info
        mock_file = MagicMock()
        mock_file.read.return_value = b"test abfss content"
        mock_fs.open_input_file.return_value.__enter__.return_value = mock_file
        mock_fs.get_file_info.return_value.type = pa_fs.FileType.File

        # Test getting file as string (default)
        content = PyArrowFileSystem.get_file(
            "abfss://container@account.dfs.core.windows.net/test.txt"
        )
        assert content == "test abfss content"

        # Verify the correct path was used
        mock_fs.get_file_info.assert_called_with("container/test.txt")
        mock_fs.open_input_file.assert_called_with("container/test.txt")

    @patch("adlfs.AzureBlobFileSystem")
    @patch("azure.identity.DefaultAzureCredential")
    @patch("pyarrow.fs.PyFileSystem")
    @patch("pyarrow.fs.FSSpecHandler")
    def test_list_subfolders_abfss(
        self, mock_handler, mock_pyfs, mock_cred, mock_adlfs
    ):
        """Test listing subfolders in ABFSS storage."""
        # Setup mock filesystem
        mock_adlfs_instance = MagicMock()
        mock_adlfs.return_value = mock_adlfs_instance
        mock_fs = MagicMock()
        mock_pyfs.return_value = mock_fs

        # Create mock file infos for directory listing
        dir1 = MagicMock()
        dir1.type = pa_fs.FileType.Directory
        dir1.path = "container/parent/subdir1"

        dir2 = MagicMock()
        dir2.type = pa_fs.FileType.Directory
        dir2.path = "container/parent/subdir2"

        file1 = MagicMock()
        file1.type = pa_fs.FileType.File
        file1.path = "container/parent/file.txt"

        mock_fs.get_file_info.return_value = [dir1, dir2, file1]

        # Test listing subfolders
        folders = PyArrowFileSystem.list_subfolders(
            "abfss://container@account.dfs.core.windows.net/parent"
        )
        assert sorted(folders) == ["subdir1", "subdir2"]

        # Verify the correct path was used
        mock_fs.get_file_info.assert_called_once()
        call_args = mock_fs.get_file_info.call_args[0][0]
        assert call_args.base_dir == "container/parent/"
        assert call_args.recursive is False


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
