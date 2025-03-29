import sys
import pytest
import asyncio
from ray.llm._internal.common.utils.cloud_utils import (
    CloudFileSystem,
    CloudObjectCache,
    remote_object_cache,
)


import tempfile
import os
from unittest.mock import MagicMock, patch

import pyarrow.fs as pa_fs
from pytest import raises


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
            with patch.object(CloudFileSystem, "download_files") as mock_download:
                CloudFileSystem.download_model(tempdir, "gs://bucket/model", False)

                # Check that hash file was processed
                assert os.path.exists(os.path.join(tempdir, "refs", "main"))
                with open(os.path.join(tempdir, "refs", "main"), "r") as f:
                    assert f.read() == "abcdef1234567890"

                # Check that download_files was called correctly
                mock_download.assert_called_once()
                call_args = mock_download.call_args[1]
                assert call_args["path"] == os.path.join(
                    tempdir, "snapshots", "abcdef1234567890"
                )
                assert call_args["bucket_uri"] == "gs://bucket/model"
                assert call_args["substrings_to_include"] == []


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
