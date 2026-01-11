import asyncio
import inspect
import os
import time
from pathlib import Path
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    NamedTuple,
    Optional,
    TypeVar,
    Union,
)

from pydantic import Field, field_validator

from ray.llm._internal.common.base_pydantic import BaseModelExtended
from ray.llm._internal.common.observability.logging import get_logger
from ray.llm._internal.common.utils.cloud_filesystem import (
    AzureFileSystem,
    GCSFileSystem,
    PyArrowFileSystem,
    S3FileSystem,
)

T = TypeVar("T")

logger = get_logger(__name__)


def is_remote_path(path: str) -> bool:
    """Check if the path is a remote path.

    Args:
        path: The path to check.

    Returns:
        True if the path is a remote path, False otherwise.
    """
    return (
        path.startswith("s3://")
        or path.startswith("gs://")
        or path.startswith("abfss://")
        or path.startswith("azure://")
        or path.startswith("pyarrow-")
    )


class ExtraFiles(BaseModelExtended):
    bucket_uri: str
    destination_path: str


class CloudMirrorConfig(BaseModelExtended):
    """Unified mirror config for cloud storage (S3, GCS, or Azure).

    Args:
        bucket_uri: URI of the bucket (s3://, gs://, abfss://, or azure://)
        extra_files: Additional files to download
    """

    bucket_uri: Optional[str] = None
    extra_files: List[ExtraFiles] = Field(default_factory=list)

    @field_validator("bucket_uri")
    @classmethod
    def check_uri_format(cls, value):
        if value is None:
            return value

        if not is_remote_path(value):
            raise ValueError(
                f'Got invalid value "{value}" for bucket_uri. '
                'Expected a URI that starts with "s3://", "gs://", "abfss://", or "azure://".'
            )
        return value

    @property
    def storage_type(self) -> str:
        """Returns the storage type ('s3', 'gcs', 'abfss', or 'azure') based on the URI prefix."""
        if self.bucket_uri is None:
            return None
        elif self.bucket_uri.startswith("s3://"):
            return "s3"
        elif self.bucket_uri.startswith("gs://"):
            return "gcs"
        elif self.bucket_uri.startswith("abfss://"):
            return "abfss"
        elif self.bucket_uri.startswith("azure://"):
            return "azure"
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

        if not is_remote_path(value):
            raise ValueError(
                f'Got invalid value "{value}" for bucket_uri. '
                'Expected a URI that starts with "s3://", "gs://", "abfss://", or "azure://".'
            )
        return value

    @property
    def _bucket_name_and_path(self) -> str:
        for prefix in ["s3://", "gs://", "abfss://", "azure://"]:
            if self.bucket_uri.startswith(prefix):
                return self.bucket_uri[len(prefix) :]
        return self.bucket_uri

    @property
    def bucket_name(self) -> str:
        bucket_part = self._bucket_name_and_path.split("/")[0]

        # For ABFSS and Azure URIs, extract container name from container@account format
        if self.bucket_uri.startswith(("abfss://", "azure://")) and "@" in bucket_part:
            return bucket_part.split("@")[0]

        return bucket_part

    @property
    def bucket_path(self) -> str:
        return "/".join(self._bucket_name_and_path.split("/")[1:])


class CloudFileSystem:
    """A unified interface for cloud file system operations.

    This class provides a simple interface for common operations on cloud storage
    systems (S3, GCS, Azure) by delegating to provider-specific implementations
    for optimal performance.
    """

    @staticmethod
    def _get_provider_fs(bucket_uri: str):
        """Get the appropriate provider-specific filesystem class based on URI.

        Args:
            bucket_uri: URI of the cloud storage (s3://, gs://, abfss://, or azure://)

        Returns:
            The appropriate filesystem class (S3FileSystem, GCSFileSystem, or AzureFileSystem)

        Raises:
            ValueError: If the URI scheme is not supported
        """
        if bucket_uri.startswith("pyarrow-"):
            return PyArrowFileSystem
        elif bucket_uri.startswith("s3://"):
            return S3FileSystem
        elif bucket_uri.startswith("gs://"):
            return GCSFileSystem
        elif bucket_uri.startswith(("abfss://", "azure://")):
            return AzureFileSystem
        else:
            raise ValueError(f"Unsupported URI scheme: {bucket_uri}")

    @staticmethod
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
        fs_class = CloudFileSystem._get_provider_fs(object_uri)
        return fs_class.get_file(object_uri, decode_as_utf_8)

    @staticmethod
    def list_subfolders(folder_uri: str) -> List[str]:
        """List the immediate subfolders in a cloud directory.

        Args:
            folder_uri: URI of the directory (s3://, gs://, abfss://, or azure://)

        Returns:
            List of subfolder names (without trailing slashes)
        """
        fs_class = CloudFileSystem._get_provider_fs(folder_uri)
        return fs_class.list_subfolders(folder_uri)

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
        fs_class = CloudFileSystem._get_provider_fs(bucket_uri)
        fs_class.download_files(
            path, bucket_uri, substrings_to_include, suffixes_to_exclude
        )

    @staticmethod
    def download_model(
        destination_path: str,
        bucket_uri: str,
        tokenizer_only: bool,
        exclude_safetensors: bool = False,
    ) -> None:
        """Download a model from cloud storage.

        This downloads a model in the format expected by the HuggingFace transformers
        library.

        Args:
            destination_path: Path where the model will be stored
            bucket_uri: URI of the cloud directory containing the model
            tokenizer_only: If True, only download tokenizer-related files
            exclude_safetensors: If True, skip download of safetensor files
        """
        try:
            # Get the provider-specific filesystem
            fs_class = CloudFileSystem._get_provider_fs(bucket_uri)

            # Construct hash file URI
            hash_uri = bucket_uri.rstrip("/") + "/hash"

            # Try to download and read hash file
            hash_content = fs_class.get_file(hash_uri, decode_as_utf_8=True)

            if hash_content is not None:
                f_hash = hash_content.strip()
                logger.info(
                    f"Detected hash file in bucket {bucket_uri}. "
                    f"Using {f_hash} as the hash."
                )
            else:
                f_hash = "0000000000000000000000000000000000000000"
                logger.info(
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

            safetensors_to_exclude = [".safetensors"] if exclude_safetensors else None

            CloudFileSystem.download_files(
                path=destination_dir,
                bucket_uri=bucket_uri,
                substrings_to_include=tokenizer_file_substrings,
                suffixes_to_exclude=safetensors_to_exclude,
            )

        except Exception as e:
            logger.exception(f"Error downloading model from {bucket_uri}: {e}")
            raise

    @staticmethod
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
        fs_class = CloudFileSystem._get_provider_fs(bucket_uri)
        fs_class.upload_files(local_path, bucket_uri)

    @staticmethod
    def upload_model(
        local_path: str,
        bucket_uri: str,
    ) -> None:
        """Upload a model to cloud storage.

        Args:
            local_path: The local path of the model.
            bucket_uri: The bucket uri to upload the model to, must start with `s3://` or `gs://`.
        """
        try:
            # If refs/main exists, upload as hash, and treat snapshots/<hash> as the model.
            # Otherwise, this is a custom model, we do not assume folder hierarchy.
            refs_main = Path(local_path, "refs", "main")
            if refs_main.exists():
                model_path = os.path.join(
                    local_path, "snapshots", refs_main.read_text().strip()
                )
                CloudFileSystem.upload_files(
                    local_path=model_path, bucket_uri=bucket_uri
                )
                CloudFileSystem.upload_files(
                    local_path=str(refs_main),
                    bucket_uri=os.path.join(bucket_uri, "hash"),
                )
            else:
                CloudFileSystem.upload_files(
                    local_path=local_path, bucket_uri=bucket_uri
                )
            logger.info(f"Uploaded model files to {bucket_uri}.")
        except Exception as e:
            logger.exception(f"Error uploading model to {bucket_uri}: {e}")
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


class CloudModelAccessor:
    """Unified accessor for models stored in cloud storage (S3 or GCS).

    Args:
        model_id: The model id to download or upload.
        mirror_config: The mirror config for the model.
    """

    def __init__(self, model_id: str, mirror_config: CloudMirrorConfig):
        self.model_id = model_id
        self.mirror_config = mirror_config

    def _get_lock_path(self, suffix: str = "") -> Path:
        return Path(
            "~", f"{self.model_id.replace('/', '--')}{suffix}.lock"
        ).expanduser()

    def _get_model_path(self) -> Path:
        if Path(self.model_id).exists():
            return Path(self.model_id)
        # Delayed import to avoid circular dependencies
        from transformers.utils.hub import TRANSFORMERS_CACHE

        return Path(
            TRANSFORMERS_CACHE, f"models--{self.model_id.replace('/', '--')}"
        ).expanduser()


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
