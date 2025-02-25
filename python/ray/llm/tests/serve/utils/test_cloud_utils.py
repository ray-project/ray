import sys
import pytest
import asyncio
from ray.llm._internal.serve.deployments.utils.cloud_utils import (
    CloudObjectCache,
    remote_object_cache,
    check_s3_path_exists_and_can_be_accessed,
    get_aws_credentials,
    get_file_from_gcs,
    get_file_from_s3,
    get_gcs_bucket_name_and_prefix,
)


from pathlib import Path
from typing import Optional, Tuple, Union
from unittest.mock import MagicMock, patch

import ray
from google.cloud import storage
from pytest import fixture, mark, raises

from ray.llm._internal.serve.configs.server_models import S3AWSCredentials


def patched_get_gcs_client():
    """In CI that runs on AWS we need to use a client with anonymous credentials."""

    return storage.Client.create_anonymous_client()


@fixture
def initialize_ray():
    if not ray.is_initialized():
        ray.init()
    yield
    if ray.is_initialized():
        ray.shutdown()


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


class Testremote_object_cacheDecorator:
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


class TestAWSCredentials:
    @patch("os.getenv")
    @patch("requests.post")
    def test_get_aws_credentials_with_auth_token(self, mock_post, mock_getenv):
        mock_getenv.return_value = "dummy_token"
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "AWS_ACCESS_KEY_ID": "dummy_access_key",
            "AWS_SECRET_ACCESS_KEY": "dummy_secret_key",
        }
        mock_response.ok = True
        mock_post.return_value = mock_response

        credentials_config = S3AWSCredentials(
            auth_token_env_variable="TOKEN_ENV_VAR",
            create_aws_credentials_url="http://dummy-url.com",
        )
        result = get_aws_credentials(credentials_config)

        assert result == {
            "AWS_ACCESS_KEY_ID": "dummy_access_key",
            "AWS_SECRET_ACCESS_KEY": "dummy_secret_key",
        }
        mock_getenv.assert_called_once_with("TOKEN_ENV_VAR")
        mock_post.assert_called_once_with(
            "http://dummy-url.com", headers={"Authorization": "Bearer dummy_token"}
        )

    @patch("requests.post")
    def test_get_aws_credentials_without_auth_token(self, mock_post):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "AWS_ACCESS_KEY_ID": "dummy_access_key",
            "AWS_SECRET_ACCESS_KEY": "dummy_secret_key",
        }
        mock_response.ok = True
        mock_post.return_value = mock_response

        credentials_config = S3AWSCredentials(
            auth_token_env_variable=None,
            create_aws_credentials_url="http://dummy-url.com",
        )
        result = get_aws_credentials(credentials_config)

        assert result == {
            "AWS_ACCESS_KEY_ID": "dummy_access_key",
            "AWS_SECRET_ACCESS_KEY": "dummy_secret_key",
        }
        mock_post.assert_called_once_with("http://dummy-url.com", headers=None)

    @patch("requests.post")
    def test_get_aws_credentials_request_failure(self, mock_post):
        mock_response = MagicMock()
        mock_response.ok = False
        mock_response.reason = "Bad Request"
        mock_post.return_value = mock_response

        credentials_config = S3AWSCredentials(
            auth_token_env_variable=None,
            create_aws_credentials_url="http://dummy-url.com",
        )
        result = get_aws_credentials(credentials_config)

        assert result is None
        mock_post.assert_called_once_with(
            "http://dummy-url.com",
            headers=None,
        )


class TestCheckS3PathExists:
    @patch("subprocess.run")
    def test_check_s3_path_exists_found(self, mock_run):
        # Test when S3 path exists
        mock_result = MagicMock()
        mock_result.returncode = 0  # Simulate path found
        mock_result.stdout.strip.return_value = "some-file"
        mock_run.return_value = mock_result
        s3_folder_uri = Path("s3://dummy-bucket/dummy-path")
        result = check_s3_path_exists_and_can_be_accessed(
            s3_folder_uri, subprocess_run=mock_run
        )

        assert result

    @patch("subprocess.run")
    def test_check_s3_path_not_exists(self, mock_run):
        # Test when S3 path does not exist
        mock_result = MagicMock()
        mock_result.returncode = 1  # Simulate path not found
        mock_result.stdout.strip.return_value = ""
        mock_run.return_value = mock_result

        s3_folder_uri = Path("s3://dummy-bucket/nonexistent-path/")
        result = check_s3_path_exists_and_can_be_accessed(
            s3_folder_uri, subprocess_run=mock_run
        )

        assert not result

    @patch("subprocess.run")
    def test_check_s3_path_invalid_aws_executable(self, mock_run):
        # Test with Invalid AWS Executable
        mock_run.side_effect = OSError("Invalid AWS executable")

        s3_folder_uri = Path("s3://dummy-bucket/dummy-path/")
        with raises(OSError):
            check_s3_path_exists_and_can_be_accessed(
                s3_folder_uri, subprocess_run=mock_run
            )


class TestGetGcsBucketNameAndPrefix:
    def run_and_validate(
        self,
        gcs_uri: str,
        expected_bucket_name: str,
        expected_prefix: str,
        is_file: bool = False,
    ):
        bucket_name, prefix = get_gcs_bucket_name_and_prefix(gcs_uri, is_file=is_file)

        assert bucket_name == expected_bucket_name
        assert prefix == expected_prefix

    @mark.parametrize("trailing_slash", [True, False])
    def test_plain_bucket_name(self, trailing_slash: bool):
        gcs_uri = "gs://bucket_name"
        if trailing_slash:
            gcs_uri += "/"

        expected_bucket_name = "bucket_name"
        expected_prefix = ""

        self.run_and_validate(gcs_uri, expected_bucket_name, expected_prefix)

    @mark.parametrize("trailing_slash", [True, False])
    def test_bucket_name_with_prefix(self, trailing_slash: bool):
        gcs_uri = "gs://bucket_name/my/prefix"
        if trailing_slash:
            gcs_uri += "/"

        expected_bucket_name = "bucket_name"
        expected_prefix = "my/prefix/"

        self.run_and_validate(gcs_uri, expected_bucket_name, expected_prefix)

    def test_object_name_with_prefix(self):
        gcs_uri = "gs://bucket_name/my/prefix.txt"

        expected_bucket_name = "bucket_name"
        expected_prefix = "my/prefix.txt"

        self.run_and_validate(
            gcs_uri, expected_bucket_name, expected_prefix, is_file=True
        )

    def test_invalid_uri(self):
        gcs_uri = "s3://bucket/prefix"
        expected_bucket_name = None
        expected_prefix = None

        with raises(ValueError):
            self.run_and_validate(gcs_uri, expected_bucket_name, expected_prefix)


class TestGetFileFromRemoteStorage:
    """Tests behavior of helper methods to get a file from S3 or GCS."""

    def _get_file(self, storage: str) -> Tuple[str, str]:
        """Gets a storage type and returns the file path and expected body"""
        if storage == "s3":
            return (
                "s3://rayllm-ci/test_file.txt",
                "This is a test file to unittest downloading files from s3.\n",
            )
        elif storage == "gs":
            return (
                "gs://anyscale-public-access-bucket/test_file.txt",
                "This is a test file to unittest downloading files from s3.\n",
            )
        else:
            raise ValueError(f"storage {storage} is not supported.")

    @patch(
        "ray.llm._internal.serve.deployments.utils.cloud_utils.get_gcs_client",
        patched_get_gcs_client,
    )
    def _download_file(
        self, storage: str, file_uri: str, decode_as_utf_8: bool = False
    ) -> Optional[Union[str, bytes]]:
        if storage == "s3":
            return get_file_from_s3(file_uri, decode_as_utf_8=decode_as_utf_8)
        elif storage == "gs":
            return get_file_from_gcs(file_uri, decode_as_utf_8=decode_as_utf_8)
        else:
            raise ValueError(f"storage {storage} is not supported.")

    @mark.parametrize("storage", ["s3", "gs"])
    @mark.parametrize("decode_as_utf_8", [False, True])
    def test_get_file(self, storage: str, decode_as_utf_8: bool):
        """Tests if we can successfully get files from s3."""

        file_uri, expected_body = self._get_file(storage)
        str_body = self._download_file(
            storage, file_uri, decode_as_utf_8=decode_as_utf_8
        )

        str_expected = (
            expected_body.encode("utf-8") if not decode_as_utf_8 else expected_body
        )
        assert str_body == str_expected

    @mark.parametrize("storage", ["s3", "gs"])
    def test_get_nonexistent_bucket(self, storage: str):
        """Tests if bucket doesn't exist, do we return None"""

        uri = f"{storage}://fake_bucket/foo.txt"
        body = self._download_file(storage, uri, decode_as_utf_8=True)
        assert body is None

    @mark.parametrize("storage", ["s3", "gs"])
    def test_get_nonexistent_file(self, storage: str):
        """Tests if file doesn't exit, do we return None"""

        cur_uri, *_ = self._get_file(storage)
        # Note: foo.txt does not exist hopefully
        parts = cur_uri.split("/")
        parts[-1] = "foo.txt"
        new_uri = "/".join(parts)
        body = self._download_file(storage, new_uri, decode_as_utf_8=True)
        assert body is None


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
