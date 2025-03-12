from typing import (
    List,
    Optional,
    Tuple,
    Union,
    Dict,
    Any,
    Callable,
    Awaitable,
    TypeVar,
    NamedTuple,
)
from pydantic import Field, field_validator
import os
import time
import inspect
import asyncio

# Use pyarrow for cloud storage access
import pyarrow.fs as pa_fs

from ray.llm._internal.serve.observability.logging import get_logger
from ray.llm._internal.common.base_pydantic import BaseModelExtended


T = TypeVar("T")

logger = get_logger(__name__)


class ExtraFiles(BaseModelExtended):
    bucket_uri: str
    destination_path: str


class CloudMirrorConfig(BaseModelExtended):
    """Unified mirror config for cloud storage (S3 or GCS).

    Args:
        bucket_uri: URI of the bucket (s3:// or gs://)
        extra_files: Additional files to download
    """

    bucket_uri: Optional[str] = None
    extra_files: List[ExtraFiles] = Field(default_factory=list)

    @field_validator("bucket_uri")
    @classmethod
    def check_uri_format(cls, value):
        if value is None:
            return value

        if not value.startswith("s3://") and not value.startswith("gs://"):
            raise ValueError(
                f'Got invalid value "{value}" for bucket_uri. '
                'Expected a URI that starts with "s3://" or "gs://".'
            )
        return value

    @property
    def storage_type(self) -> str:
        """Returns the storage type ('s3' or 'gcs') based on the URI prefix."""
        if self.bucket_uri is None:
            return None
        elif self.bucket_uri.startswith("s3://"):
            return "s3"
        elif self.bucket_uri.startswith("gs://"):
            return "gcs"
        return None


class LoraMirrorConfig(BaseModelExtended):
    lora_model_id: str
    bucket_uri: str
    max_total_tokens: Optional[int]
    sync_args: Optional[List[str]] = None

    @field_validator("bucket_uri")
    @classmethod
    def check_uri_format(cls, value):
        if value is None:
            return value

        if not value.startswith("s3://") and not value.startswith("gs://"):
            raise ValueError(
                f'Got invalid value "{value}" for bucket_uri. '
                'Expected a URI that starts with "s3://" or "gs://".'
            )
        return value

    @property
    def _bucket_name_and_path(self) -> str:
        for prefix in ["s3://", "gs://"]:
            if self.bucket_uri.startswith(prefix):
                return self.bucket_uri[len(prefix) :]
        return self.bucket_uri

    @property
    def bucket_name(self) -> str:
        return self._bucket_name_and_path.split("/")[0]

    @property
    def bucket_path(self) -> str:
        return "/".join(self._bucket_name_and_path.split("/")[1:])


class CloudFileSystem:
    """A unified interface for cloud file system operations using PyArrow.

    This class provides a simple interface for common operations on cloud storage
    systems (S3, GCS) using PyArrow's filesystem interface.
    """

    @staticmethod
    def get_fs_and_path(object_uri: str) -> Tuple[pa_fs.FileSystem, str]:
        """Get the appropriate filesystem and path from a URI.

        Args:
            object_uri: URI of the file (s3:// or gs://)
                If URI contains 'anonymous@', anonymous access is used.
                Example: s3://anonymous@bucket/path

        Returns:
            Tuple of (filesystem, path)
        """
        anonymous = False
        # Check for anonymous access pattern
        # e.g. s3://anonymous@bucket/path
        if "@" in object_uri:
            parts = object_uri.split("@", 1)
            # Check if the first part ends with "anonymous"
            if parts[0].endswith("anonymous"):
                anonymous = True
                # Remove the anonymous@ part, keeping the scheme
                scheme = parts[0].split("://")[0]
                object_uri = f"{scheme}://{parts[1]}"

        if object_uri.startswith("s3://"):
            fs = pa_fs.S3FileSystem(anonymous=anonymous)
            path = object_uri[5:]  # Remove "s3://"
        elif object_uri.startswith("gs://"):
            fs = pa_fs.GcsFileSystem(anonymous=anonymous)
            path = object_uri[5:]  # Remove "gs://"
        else:
            raise ValueError(f"Unsupported URI scheme: {object_uri}")

        return fs, path

    @staticmethod
    def get_file(
        object_uri: str, decode_as_utf_8: bool = True
    ) -> Optional[Union[str, bytes]]:
        """Download a file from cloud storage into memory.

        Args:
            object_uri: URI of the file (s3:// or gs://)
            decode_as_utf_8: If True, decode the file as UTF-8

        Returns:
            File contents as string or bytes, or None if file doesn't exist
        """
        try:
            fs, path = CloudFileSystem.get_fs_and_path(object_uri)

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
            logger.info(f"Error reading {object_uri}: {e}")
            return None

    @staticmethod
    def list_subfolders(folder_uri: str) -> List[str]:
        """List the immediate subfolders in a cloud directory.

        Args:
            folder_uri: URI of the directory (s3:// or gs://)

        Returns:
            List of subfolder names (without trailing slashes)
        """
        # Ensure that the folder_uri has a trailing slash.
        folder_uri = f"{folder_uri.rstrip('/')}/"

        try:
            fs, path = CloudFileSystem.get_fs_and_path(folder_uri)

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
            logger.info(f"Error listing subfolders in {folder_uri}: {e}")
            return []

    @staticmethod
    def download_files(
        path: str,
        bucket_uri: str,
        substrings_to_include: Optional[List[str]] = None,
    ) -> None:
        """Download files from cloud storage to a local directory.

        Args:
            path: Local directory where files will be downloaded
            bucket_uri: URI of cloud directory
            substrings_to_include: Only include files containing these substrings
        """
        try:
            fs, source_path = CloudFileSystem.get_fs_and_path(bucket_uri)

            # Ensure the destination directory exists
            os.makedirs(path, exist_ok=True)

            # List all files in the bucket
            file_selector = pa_fs.FileSelector(source_path, recursive=True)
            file_infos = fs.get_file_info(file_selector)

            # Download each file
            for file_info in file_infos:
                if file_info.type != pa_fs.FileType.File:
                    continue

                # Get relative path from source prefix
                rel_path = file_info.path[len(source_path) :].lstrip("/")

                # Check if file matches substring filters
                if substrings_to_include:
                    if not any(
                        substring in rel_path for substring in substrings_to_include
                    ):
                        continue

                # Create destination directory if needed
                if "/" in rel_path:
                    dest_dir = os.path.join(path, os.path.dirname(rel_path))
                    os.makedirs(dest_dir, exist_ok=True)

                # Download the file
                dest_path = os.path.join(path, rel_path)
                with fs.open_input_file(file_info.path) as source_file:
                    with open(dest_path, "wb") as dest_file:
                        dest_file.write(source_file.read())

        except Exception as e:
            logger.exception(f"Error downloading files from {bucket_uri}: {e}")
            raise

    @staticmethod
    def download_model(
        destination_path: str, bucket_uri: str, tokenizer_only: bool
    ) -> None:
        """Download a model from cloud storage.

        This downloads a model in the format expected by the HuggingFace transformers
        library.

        Args:
            destination_path: Path where the model will be stored
            bucket_uri: URI of the cloud directory containing the model
            tokenizer_only: If True, only download tokenizer-related files
        """
        try:
            fs, source_path = CloudFileSystem.get_fs_and_path(bucket_uri)

            # Check for hash file
            hash_path = os.path.join(source_path, "hash")
            hash_info = fs.get_file_info(hash_path)

            if hash_info.type == pa_fs.FileType.File:
                # Download and read hash file
                with fs.open_input_file(hash_path) as f:
                    f_hash = f.read().decode("utf-8").strip()
                logger.info(
                    f"Detected hash file in bucket {bucket_uri}. "
                    f"Using {f_hash} as the hash."
                )
            else:
                f_hash = "0000000000000000000000000000000000000000"
                logger.warning(
                    f"Hash file does not exist in bucket {bucket_uri}. "
                    f"Using {f_hash} as the hash."
                )

            # Write hash to refs/main
            main_dir = os.path.join(destination_path, "refs")
            os.makedirs(main_dir, exist_ok=True)
            with open(os.path.join(main_dir, "main"), "w") as f:
                f.write(f_hash)

            # Create destination directory
            destination_dir = os.path.join(destination_path, "snapshots", f_hash)
            os.makedirs(destination_dir, exist_ok=True)

            logger.info(f'Downloading model files to directory "{destination_dir}".')

            # Download files
            tokenizer_file_substrings = (
                ["tokenizer", "config.json"] if tokenizer_only else []
            )
            CloudFileSystem.download_files(
                path=destination_dir,
                bucket_uri=bucket_uri,
                substrings_to_include=tokenizer_file_substrings,
            )

        except Exception as e:
            logger.exception(f"Error downloading model from {bucket_uri}: {e}")
            raise


class _CacheEntry(NamedTuple):
    value: Any
    expire_time: Optional[float]


class CloudObjectCache:
    """A cache that works with both sync and async fetch functions.

    The purpose of this data structure is to cache the result of a function call
    usually used to fetch a value from a cloud object store.

    The idea is this:
    - Cloud operations are expensive
    - In LoRA specifically, we would fetch remote storage to download the model weights
    at each request.
    - If the same model is requested many times, we don't want to inflate the time to first token.
    - We control the cache via not only the least recently used eviction policy, but also
    by expiring cache entries after a certain time.
    - If the object is missing, we cache the missing status for a small duration while if
    the object exists, we cache the object for a longer duration.
    """

    def __init__(
        self,
        max_size: int,
        fetch_fn: Union[Callable[[str], Any], Callable[[str], Awaitable[Any]]],
        missing_expire_seconds: Optional[int] = None,
        exists_expire_seconds: Optional[int] = None,
        missing_object_value: Any = object(),
    ):
        """Initialize the cache.

        Args:
            max_size: Maximum number of items to store in cache
            fetch_fn: Function to fetch values (can be sync or async)
            missing_expire_seconds: How long to cache missing objects (None for no expiration)
            exists_expire_seconds: How long to cache existing objects (None for no expiration)
        """
        self._cache: Dict[str, _CacheEntry] = {}
        self._max_size = max_size
        self._fetch_fn = fetch_fn
        self._missing_expire_seconds = missing_expire_seconds
        self._exists_expire_seconds = exists_expire_seconds
        self._is_async = inspect.iscoroutinefunction(fetch_fn) or (
            callable(fetch_fn) and inspect.iscoroutinefunction(fetch_fn.__call__)
        )
        self._missing_object_value = missing_object_value
        # Lock for thread-safe cache access
        self._lock = asyncio.Lock()

    async def aget(self, key: str) -> Any:
        """Async get value from cache or fetch it if needed."""

        if not self._is_async:
            raise ValueError("Cannot use async get() with sync fetch function")

        async with self._lock:
            value, should_fetch = self._check_cache(key)
            if not should_fetch:
                return value

            # Fetch new value
            value = await self._fetch_fn(key)
            self._update_cache(key, value)
            return value

    def get(self, key: str) -> Any:
        """Sync get value from cache or fetch it if needed."""
        if self._is_async:
            raise ValueError("Cannot use sync get() with async fetch function")

        # For sync access, we use a simple check-then-act pattern
        # This is safe because sync functions are not used in async context
        value, should_fetch = self._check_cache(key)
        if not should_fetch:
            return value

        # Fetch new value
        value = self._fetch_fn(key)
        self._update_cache(key, value)
        return value

    def _check_cache(self, key: str) -> tuple[Any, bool]:
        """Check if key exists in cache and is valid.

        Returns:
            Tuple of (value, should_fetch)
            where should_fetch is True if we need to fetch a new value
        """
        now = time.monotonic()

        if key in self._cache:
            value, expire_time = self._cache[key]
            if expire_time is None or now < expire_time:
                return value, False

        return None, True

    def _update_cache(self, key: str, value: Any) -> None:
        """Update cache with new value."""
        now = time.monotonic()

        # Calculate expiration
        expire_time = None
        if (
            self._missing_expire_seconds is not None
            or self._exists_expire_seconds is not None
        ):
            if value is self._missing_object_value:
                expire_time = (
                    now + self._missing_expire_seconds
                    if self._missing_expire_seconds
                    else None
                )
            else:
                expire_time = (
                    now + self._exists_expire_seconds
                    if self._exists_expire_seconds
                    else None
                )

        # Enforce size limit by removing oldest entry if needed
        # This is an O(n) operation but it's fine since the cache size is usually small.
        if len(self._cache) >= self._max_size:
            oldest_key = min(
                self._cache, key=lambda k: self._cache[k].expire_time or float("inf")
            )
            del self._cache[oldest_key]

        self._cache[key] = _CacheEntry(value, expire_time)

    def __len__(self) -> int:
        return len(self._cache)


def remote_object_cache(
    max_size: int,
    missing_expire_seconds: Optional[int] = None,
    exists_expire_seconds: Optional[int] = None,
    missing_object_value: Any = None,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """A decorator that provides async caching using CloudObjectCache.

    This is a direct replacement for the remote_object_cache/cachetools combination,
    using CloudObjectCache internally to maintain cache state.

    Args:
        max_size: Maximum number of items to store in cache
        missing_expire_seconds: How long to cache missing objects
        exists_expire_seconds: How long to cache existing objects
        missing_object_value: Value to use for missing objects
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        # Create a single cache instance for this function
        cache = CloudObjectCache(
            max_size=max_size,
            fetch_fn=func,
            missing_expire_seconds=missing_expire_seconds,
            exists_expire_seconds=exists_expire_seconds,
            missing_object_value=missing_object_value,
        )

        async def wrapper(*args, **kwargs):
            # Extract the key from either first positional arg or object_uri kwarg
            key = args[0] if args else kwargs.get("object_uri")
            return await cache.aget(key)

        return wrapper

    return decorator
