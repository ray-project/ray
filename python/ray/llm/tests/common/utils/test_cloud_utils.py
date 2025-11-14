import asyncio
import os
import sys
import tempfile
from unittest.mock import ANY, MagicMock, call, patch

import pyarrow.fs as pa_fs
import pytest
from pytest import raises

from ray.llm._internal.common.utils.cloud_utils import (
    CloudFileSystem,
    CloudMirrorConfig,
    CloudObjectCache,
    LoraMirrorConfig,
    is_remote_path,
    remote_object_cache,
)


class MockSyncFetcher:
    def __init__(self):
        self.call_count = 0
        self.calls = []

    def __call__(self, key: str):
        self.call_count += 1
        self.calls.append(key)
        if key == "missing":
            return -1
        return f"value-{key}"


class MockAsyncFetcher:
    def __init__(self):
        self.call_count = 0
        self.calls = []

    async def __call__(self, key: str):
        self.call_count += 1
        self.calls.append(key)
        if key == "missing":
            return -1
        return f"value-{key}"


class TestCloudObjectCache:
    """Tests for the CloudObjectCache class."""

    def test_sync_cache_basic(self):
        """Test basic synchronous cache functionality."""
        fetcher = MockSyncFetcher()
        cache = CloudObjectCache(max_size=2, fetch_fn=fetcher)

        # Test fetching a value (should be a miss)
        assert cache.get("key1") == "value-key1"
        assert fetcher.call_count == 1
        assert fetcher.calls == ["key1"]

        # Test cache hit (should not call fetcher)
        assert cache.get("key1") == "value-key1"
        assert fetcher.call_count == 1  # Count should not increase
        assert fetcher.calls == ["key1"]  # Calls should not change

        # Test cache size limit
        assert cache.get("key2") == "value-key2"  # Miss, should call fetcher
        assert fetcher.call_count == 2
        assert fetcher.calls == ["key1", "key2"]

        assert (
            cache.get("key3") == "value-key3"
        )  # Miss, should call fetcher and evict key1
        assert fetcher.call_count == 3
        assert fetcher.calls == ["key1", "key2", "key3"]

        assert len(cache) == 2

        # Verify key1 was evicted by checking if it's fetched again
        assert cache.get("key1") == "value-key1"  # Miss, should call fetcher
        assert fetcher.call_count == 4
        assert fetcher.calls == ["key1", "key2", "key3", "key1"]

        # Verify final cache state
        assert len(cache) == 2
        assert "key3" in cache._cache  # key3 should still be in cache
        assert "key1" in cache._cache  # key1 should be back in cache
        assert "key2" not in cache._cache  # key2 should have been evicted

    @pytest.mark.asyncio
    async def test_async_cache_missing_object_expiration(self):
        """Test cache expiration for missing objects in async mode."""
        fetcher = MockAsyncFetcher()
        cache = CloudObjectCache(
            max_size=2,
            fetch_fn=fetcher,
            missing_expire_seconds=1,  # 1 second to expire missing object
            exists_expire_seconds=3,  # 3 seconds to expire existing object
            missing_object_value=-1,
        )

        # Test missing object expiration
        assert await cache.aget("missing") is -1  # First fetch
        assert fetcher.call_count == 1
        assert fetcher.calls == ["missing"]

        # Should still be cached
        assert await cache.aget("missing") is -1  # Cache hit
        assert fetcher.call_count == 1  # No new fetch
        assert fetcher.calls == ["missing"]

        await asyncio.sleep(1.5)  # Wait for missing object to expire
        assert await cache.aget("missing") is -1  # Should fetch again after expiration
        assert fetcher.call_count == 2  # New fetch
        assert fetcher.calls == ["missing", "missing"]

    @pytest.mark.asyncio
    async def test_async_cache_existing_object_expiration(self):
        """Test expiration of existing objects in async mode."""
        fetcher = MockAsyncFetcher()
        cache = CloudObjectCache(
            max_size=2,
            fetch_fn=fetcher,
            missing_expire_seconds=1,  # 1 second to expire missing object
            exists_expire_seconds=3,  # 3 seconds to expire existing object
            missing_object_value=-1,
        )

        # Test existing object expiration
        assert await cache.aget("key1") == "value-key1"  # First fetch
        assert fetcher.call_count == 1
        assert fetcher.calls == ["key1"]

        # Should still be cached (not expired)
        assert await cache.aget("key1") == "value-key1"  # Cache hit
        assert fetcher.call_count == 1  # No new fetch

        await asyncio.sleep(1.5)  # Not expired yet (exists_expire_seconds=3)
        assert await cache.aget("key1") == "value-key1"  # Should still hit cache
        assert fetcher.call_count == 1  # No new fetch
        assert fetcher.calls == ["key1"]  # No change in calls

        await asyncio.sleep(2)  # Now expired (total > 2 seconds)
        assert await cache.aget("key1") == "value-key1"  # Should fetch again
        assert fetcher.call_count == 2  # New fetch
        assert fetcher.calls == ["key1", "key1"]

        # Verify final cache state
        assert len(cache) == 1


class TestRemoteObjectCacheDecorator:
    """Tests for the remote_object_cache decorator."""

    @pytest.mark.asyncio
    async def test_basic_functionality(self):
        """Test basic remote_object_cache decorator functionality."""
        call_count = 0
        MISSING = object()

        @remote_object_cache(
            max_size=2,
            missing_expire_seconds=1,
            exists_expire_seconds=3,
            missing_object_value=MISSING,
        )
        async def fetch(key: str):
            nonlocal call_count
            call_count += 1
            if key == "missing":
                return MISSING
            return f"value-{key}"

        # Test cache hit
        assert await fetch("key1") == "value-key1"
        assert call_count == 1
        assert await fetch("key1") == "value-key1"  # Should hit cache
        assert call_count == 1  # Count should not increase

        # Test cache size limit
        assert await fetch("key2") == "value-key2"
        assert call_count == 2
        assert await fetch("key3") == "value-key3"  # Should evict key1
        assert call_count == 3

        # Verify key1 was evicted
        assert await fetch("key1") == "value-key1"
        assert call_count == 4

    @pytest.mark.asyncio
    async def test_expiration(self):
        """Test cache expiration for both missing and existing objects."""
        call_count = 0
        MISSING = object()

        @remote_object_cache(
            max_size=2,
            missing_expire_seconds=1,  # 1 second to expire missing object
            exists_expire_seconds=3,  # 3 seconds to expire existing object
            missing_object_value=MISSING,
        )
        async def fetch(key: str):
            nonlocal call_count
            call_count += 1
            if key == "missing":
                return MISSING
            return f"value-{key}"

        # Test missing object expiration
        assert await fetch("missing") is MISSING
        assert call_count == 1
        assert await fetch("missing") is MISSING  # Should hit cache
        assert call_count == 1

        await asyncio.sleep(1.5)  # Wait for missing object to expire
        assert await fetch("missing") is MISSING  # Should fetch again
        assert call_count == 2

        # Test existing object expiration
        assert await fetch("key1") == "value-key1"
        assert call_count == 3
        assert await fetch("key1") == "value-key1"  # Should hit cache
        assert call_count == 3

        await asyncio.sleep(1.5)  # Not expired yet
        assert await fetch("key1") == "value-key1"  # Should still hit cache
        assert call_count == 3

        await asyncio.sleep(2)  # Now expired (total > 3 seconds)
        assert await fetch("key1") == "value-key1"  # Should fetch again
        assert call_count == 4

    @pytest.mark.asyncio
    async def test_error_handling(self):
        """Test error handling in remote_object_cache decorator."""
        call_count = 0

        @remote_object_cache(max_size=2)
        async def fetch(key: str):
            nonlocal call_count
            call_count += 1
            if key == "error":
                raise ValueError("Test error")
            return f"value-{key}"

        # Test successful case
        assert await fetch("key1") == "value-key1"
        assert call_count == 1

        # Test error case
        with pytest.raises(ValueError, match="Test error"):
            await fetch("error")
        assert call_count == 2

        # Verify error wasn't cached
        with pytest.raises(ValueError, match="Test error"):
            await fetch("error")
        assert call_count == 3

    @pytest.mark.asyncio
    async def test_concurrent_access(self):
        """Test concurrent access to cached function."""
        call_count = 0
        DELAY = 0.1

        @remote_object_cache(max_size=2)
        async def slow_fetch(key: str):
            nonlocal call_count
            call_count += 1
            await asyncio.sleep(DELAY)  # Simulate slow operation
            return f"value-{key}"

        # Launch multiple concurrent calls
        tasks = [slow_fetch("key1") for _ in range(5)]
        results = await asyncio.gather(*tasks)

        # All results should be the same
        assert all(r == "value-key1" for r in results)
        # Should only call once despite multiple concurrent requests
        assert call_count == 1


class TestCloudFileSystem:
    """Tests for the CloudFileSystem class."""

    @patch("pyarrow.fs.S3FileSystem")
    def test_get_fs_and_path_s3(self, mock_s3fs):
        """Test getting S3 filesystem and path."""
        mock_fs = MagicMock()
        mock_s3fs.return_value = mock_fs

        fs, path = CloudFileSystem.get_fs_and_path("s3://bucket/key")

        assert fs == mock_fs
        assert path == "bucket/key"
        mock_s3fs.assert_called_once()

    @patch("pyarrow.fs.GcsFileSystem")
    def test_get_fs_and_path_gcs(self, mock_gcsfs):
        """Test getting GCS filesystem and path."""
        mock_fs = MagicMock()
        mock_gcsfs.return_value = mock_fs

        fs, path = CloudFileSystem.get_fs_and_path("gs://bucket/key")

        assert fs == mock_fs
        assert path == "bucket/key"
        mock_gcsfs.assert_called_once()

    def test_get_fs_and_path_unsupported(self):
        """Test unsupported URI scheme."""
        with raises(ValueError, match="Unsupported URI scheme"):
            CloudFileSystem.get_fs_and_path("file:///tmp/file")

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
        content = CloudFileSystem.get_file("s3://bucket/test.txt")
        assert content == "test file content"

        # Test getting file as bytes
        content_bytes = CloudFileSystem.get_file(
            "s3://bucket/test.txt", decode_as_utf_8=False
        )
        assert content_bytes == b"test file content"

        # Test non-existent file
        mock_fs.get_file_info.return_value.type = pa_fs.FileType.NotFound
        assert CloudFileSystem.get_file("s3://bucket/nonexistent.txt") is None

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
        folders = CloudFileSystem.list_subfolders("gs://bucket/parent")
        assert sorted(folders) == ["dir1", "dir2"]

    @patch("ray.llm._internal.common.utils.cloud_utils.CloudFileSystem.get_fs_and_path")
    def test_list_subfolders_exception_handling(self, mock_get_fs_and_path):
        """Test that list_subfolders returns empty list when get_fs_and_path raises exception."""
        # Make get_fs_and_path raise an exception
        mock_get_fs_and_path.side_effect = ValueError("Example exception")

        # Test that list_subfolders handles the exception gracefully
        folders = CloudFileSystem.list_subfolders("gs://bucket/parent")
        assert folders == []

        # Verify get_fs_and_path was called
        mock_get_fs_and_path.assert_called_once_with("gs://bucket/parent/")

    @patch("pyarrow.fs.S3FileSystem")
    def test_download_files(self, mock_s3fs):
        """Test downloading files from cloud storage."""
        # Setup mock filesystem
        mock_fs = MagicMock()
        mock_s3fs.return_value = mock_fs

        # Create mock file infos for listing
        file_info1 = MagicMock()
        file_info1.type = pa_fs.FileType.File
        file_info1.path = "bucket/dir/file1.txt"

        file_info2 = MagicMock()
        file_info2.type = pa_fs.FileType.File
        file_info2.path = "bucket/dir/subdir/file2.txt"

        dir_info = MagicMock()
        dir_info.type = pa_fs.FileType.Directory
        dir_info.path = "bucket/dir/subdir"

        mock_fs.get_file_info.return_value = [file_info1, file_info2, dir_info]

        # Mock file content
        mock_file = MagicMock()
        mock_file.read.return_value = b"test content"
        mock_fs.open_input_file.return_value.__enter__.return_value = mock_file

        # Create temp directory for testing
        with tempfile.TemporaryDirectory() as tempdir:
            # Test downloading files
            CloudFileSystem.download_files(tempdir, "s3://bucket/dir")

            # Check that files were downloaded correctly
            assert os.path.exists(os.path.join(tempdir, "file1.txt"))
            assert os.path.exists(os.path.join(tempdir, "subdir", "file2.txt"))

            # Check content of downloaded files
            with open(os.path.join(tempdir, "file1.txt"), "rb") as f:
                assert f.read() == b"test content"

    @patch("pyarrow.fs.GcsFileSystem")
    def test_download_model(self, mock_gcsfs):
        """Test downloading a model from cloud storage."""
        # Setup mock filesystem
        mock_fs = MagicMock()
        mock_gcsfs.return_value = mock_fs

        # Mock hash file
        mock_hash_file = MagicMock()
        mock_hash_file.read.return_value = b"abcdef1234567890"
        mock_fs.open_input_file.return_value.__enter__.return_value = mock_hash_file

        # Mock file info for hash file
        mock_fs.get_file_info.return_value.type = pa_fs.FileType.File

        # Create temp directory for testing
        with tempfile.TemporaryDirectory() as tempdir:
            # Test downloading model
            with patch.object(
                CloudFileSystem, "download_files_parallel"
            ) as mock_download:
                CloudFileSystem.download_model(tempdir, "gs://bucket/model", False)

                # Check that hash file was processed
                assert os.path.exists(os.path.join(tempdir, "refs", "main"))
                with open(os.path.join(tempdir, "refs", "main"), "r") as f:
                    assert f.read() == "abcdef1234567890"

                # Check that download_files_parallel was called correctly
                mock_download.assert_called_once()
                call_args = mock_download.call_args[1]
                assert call_args["path"] == os.path.join(
                    tempdir, "snapshots", "abcdef1234567890"
                )
                assert call_args["bucket_uri"] == "gs://bucket/model"
                assert call_args["substrings_to_include"] == []
                assert call_args["suffixes_to_exclude"] is None
                assert call_args["chunk_size"] == 64 * 1024 * 1024

    @patch("pyarrow.fs.copy_files")
    def test_upload_files(self, mock_copy_files):
        """Test uploading files to cloud storage."""
        # Create temp directory for testing
        with tempfile.TemporaryDirectory() as tempdir:
            # Test uploading files
            CloudFileSystem.upload_files(tempdir, "s3://bucket/dir")

            # Check that the files are copied
            mock_copy_files.assert_called_once_with(
                source=tempdir,
                destination="bucket/dir",
                source_filesystem=ANY,
                destination_filesystem=ANY,
            )

    @patch("pyarrow.fs.copy_files")
    def test_upload_model(self, mock_copy_files):
        """Test uploading a model to cloud storage."""
        # Create temp directory for testing
        with tempfile.TemporaryDirectory() as tempdir:
            hash = "abcdef1234567890"
            # Create refs/main file
            os.makedirs(os.path.join(tempdir, "refs"), exist_ok=True)
            model_rev_path = os.path.join(tempdir, "refs", "main")
            with open(model_rev_path, "w") as f:
                f.write(hash)

            # Create snapshots/<hash> folder
            model_asset_path = os.path.join(tempdir, "snapshots", hash)
            os.makedirs(model_asset_path)

            # Test uploading model
            CloudFileSystem.upload_model(tempdir, "gs://bucket/model")

            # Check that the files are copied
            mock_copy_files.assert_has_calls(
                [
                    call(
                        source=model_rev_path,
                        destination="bucket/model/hash",
                        source_filesystem=ANY,
                        destination_filesystem=ANY,
                    ),
                    call(
                        source=model_asset_path,
                        destination="bucket/model",
                        source_filesystem=ANY,
                        destination_filesystem=ANY,
                    ),
                ],
                any_order=True,
            )


class TestIsRemotePath:
    """Tests for the is_remote_path utility function."""

    def test_s3_paths(self):
        """Test S3 path detection."""
        assert is_remote_path("s3://bucket/path") is True
        assert is_remote_path("s3://bucket") is True
        assert is_remote_path("s3://anonymous@bucket/path") is True

    def test_gcs_paths(self):
        """Test GCS path detection."""
        assert is_remote_path("gs://bucket/path") is True
        assert is_remote_path("gs://bucket") is True
        assert is_remote_path("gs://anonymous@bucket/path") is True

    def test_abfss_paths(self):
        """Test ABFSS path detection."""
        assert (
            is_remote_path("abfss://container@account.dfs.core.windows.net/path")
            is True
        )
        assert is_remote_path("abfss://container@account.dfs.core.windows.net") is True

    def test_azure_paths(self):
        """Test Azure path detection."""
        assert (
            is_remote_path("azure://container@account.blob.core.windows.net/path")
            is True
        )
        assert (
            is_remote_path("azure://container@account.dfs.core.windows.net/path")
            is True
        )

    def test_local_paths(self):
        """Test local path detection."""
        assert is_remote_path("/local/path") is False
        assert is_remote_path("./relative/path") is False
        assert is_remote_path("file:///local/path") is False
        assert is_remote_path("http://example.com") is False


class TestCloudMirrorConfig:
    """Tests for the CloudMirrorConfig class."""

    def test_valid_s3_uri(self):
        """Test valid S3 URI."""
        config = CloudMirrorConfig(bucket_uri="s3://my-bucket/path")
        assert config.bucket_uri == "s3://my-bucket/path"
        assert config.storage_type == "s3"

    def test_valid_gcs_uri(self):
        """Test valid GCS URI."""
        config = CloudMirrorConfig(bucket_uri="gs://my-bucket/path")
        assert config.bucket_uri == "gs://my-bucket/path"
        assert config.storage_type == "gcs"

    def test_valid_abfss_uri(self):
        """Test valid ABFSS URI."""
        config = CloudMirrorConfig(
            bucket_uri="abfss://container@account.dfs.core.windows.net/path"
        )
        assert (
            config.bucket_uri == "abfss://container@account.dfs.core.windows.net/path"
        )
        assert config.storage_type == "abfss"

    def test_valid_azure_uri(self):
        """Test valid Azure URI."""
        config = CloudMirrorConfig(
            bucket_uri="azure://container@account.blob.core.windows.net/path"
        )
        assert (
            config.bucket_uri == "azure://container@account.blob.core.windows.net/path"
        )
        assert config.storage_type == "azure"

    def test_none_uri(self):
        """Test None URI."""
        config = CloudMirrorConfig(bucket_uri=None)
        assert config.bucket_uri is None
        assert config.storage_type is None

    def test_invalid_uri(self):
        """Test invalid URI."""
        with pytest.raises(
            ValueError, match='Got invalid value "file:///tmp" for bucket_uri'
        ):
            CloudMirrorConfig(bucket_uri="file:///tmp")

    def test_extra_files(self):
        """Test extra files configuration."""
        config = CloudMirrorConfig(
            bucket_uri="s3://bucket/path",
            extra_files=[
                {"bucket_uri": "s3://bucket/file1", "destination_path": "/dest1"},
                {"bucket_uri": "s3://bucket/file2", "destination_path": "/dest2"},
            ],
        )
        assert len(config.extra_files) == 2
        assert config.extra_files[0].bucket_uri == "s3://bucket/file1"
        assert config.extra_files[0].destination_path == "/dest1"


class TestLoraMirrorConfig:
    """Tests for the LoraMirrorConfig class."""

    def test_valid_s3_config(self):
        """Test valid S3 LoRA config."""
        config = LoraMirrorConfig(
            lora_model_id="test-model",
            bucket_uri="s3://my-bucket/lora-models",
            max_total_tokens=1000,
        )
        assert config.lora_model_id == "test-model"
        assert config.bucket_uri == "s3://my-bucket/lora-models"
        assert config.bucket_name == "my-bucket"
        assert config.bucket_path == "lora-models"

    def test_valid_abfss_config(self):
        """Test valid ABFSS LoRA config."""
        config = LoraMirrorConfig(
            lora_model_id="test-model",
            bucket_uri="abfss://container@account.dfs.core.windows.net/lora/models",
            max_total_tokens=1000,
        )
        assert config.lora_model_id == "test-model"
        assert (
            config.bucket_uri
            == "abfss://container@account.dfs.core.windows.net/lora/models"
        )
        assert config.bucket_name == "container"
        assert config.bucket_path == "lora/models"

    def test_valid_azure_config(self):
        """Test valid Azure LoRA config."""
        config = LoraMirrorConfig(
            lora_model_id="test-model",
            bucket_uri="azure://container@account.blob.core.windows.net/lora/models",
            max_total_tokens=1000,
        )
        assert config.lora_model_id == "test-model"
        assert (
            config.bucket_uri
            == "azure://container@account.blob.core.windows.net/lora/models"
        )
        assert config.bucket_name == "container"
        assert config.bucket_path == "lora/models"

    def test_bucket_path_parsing(self):
        """Test bucket path parsing for different URI formats."""
        # S3 with multiple path segments
        config = LoraMirrorConfig(
            lora_model_id="test",
            bucket_uri="s3://bucket/path/to/model",
            max_total_tokens=1000,
        )
        assert config.bucket_name == "bucket"
        assert config.bucket_path == "path/to/model"

        # ABFSS with multiple path segments
        config = LoraMirrorConfig(
            lora_model_id="test",
            bucket_uri="abfss://container@account.dfs.core.windows.net/deep/nested/path",
            max_total_tokens=1000,
        )
        assert config.bucket_name == "container"
        assert config.bucket_path == "deep/nested/path"

    def test_invalid_uri(self):
        """Test invalid URI in LoRA config."""
        with pytest.raises(
            ValueError, match='Got invalid value "file:///tmp" for bucket_uri'
        ):
            LoraMirrorConfig(
                lora_model_id="test-model",
                bucket_uri="file:///tmp",
                max_total_tokens=1000,
            )

    def test_optional_fields(self):
        """Test optional fields in LoRA config."""
        config = LoraMirrorConfig(
            lora_model_id="test-model",
            bucket_uri="s3://bucket/path",
            max_total_tokens=1000,
            sync_args=["--exclude", "*.tmp"],
        )
        assert config.max_total_tokens == 1000
        assert config.sync_args == ["--exclude", "*.tmp"]


class TestCloudFileSystemFilterFiles:
    """Tests for the _filter_files method."""

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
        result = CloudFileSystem._filter_files(
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
        result = CloudFileSystem._filter_files(
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
        result = CloudFileSystem._filter_files(
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
        result = CloudFileSystem._filter_files(
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


class TestCloudFileSystemAzureSupport:
    """Tests for Azure/ABFSS support in CloudFileSystem."""

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

        fs, path = CloudFileSystem.get_fs_and_path(
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

        fs, path = CloudFileSystem.get_fs_and_path(
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
                CloudFileSystem._create_abfss_filesystem(uri)

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
                CloudFileSystem._create_abfss_filesystem(uri)

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
                CloudFileSystem._create_azure_filesystem(uri)

        # Test invalid URIs
        invalid_uris = [
            "azure://container",  # Missing @account
            "azure://@account.blob.core.windows.net/path",  # Empty container
            "azure://container@account.wrong.domain/path",  # Wrong domain
            "azure://container@.blob.core.windows.net/path",  # Empty account
        ]

        for uri in invalid_uris:
            with pytest.raises(ValueError):
                CloudFileSystem._create_azure_filesystem(uri)

    def test_abfss_import_error(self):
        """Test ImportError when adlfs is not available."""
        with patch(
            "builtins.__import__", side_effect=ImportError("No module named 'adlfs'")
        ):
            with pytest.raises(
                ImportError, match="You must `pip install adlfs azure-identity`"
            ):
                CloudFileSystem._create_abfss_filesystem(
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
                CloudFileSystem._create_azure_filesystem(
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
        fs, path = CloudFileSystem.get_fs_and_path(
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
        content = CloudFileSystem.get_file(
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
        folders = CloudFileSystem.list_subfolders(
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
