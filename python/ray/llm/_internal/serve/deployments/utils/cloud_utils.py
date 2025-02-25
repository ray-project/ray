# TODO (genesu): clean up these utils.
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
import os
import re
import requests
import subprocess
import time
import inspect
import asyncio

# Use pyarrow for cloud storage access
import pyarrow.fs as pa_fs

from ray.llm._internal.serve.observability.logging import get_logger
from ray.llm._internal.serve.configs.server_models import S3AWSCredentials


T = TypeVar("T")

logger = get_logger(__name__)

AWS_EXECUTABLE = "aws"
GCP_EXECUTABLE = "gcloud"


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
            
        Returns:
            Tuple of (filesystem, path)
        """
        if object_uri.startswith("s3://"):
            fs = pa_fs.S3FileSystem()
            path = object_uri[5:]  # Remove "s3://"
        elif object_uri.startswith("gs://"):
            fs = pa_fs.GcsFileSystem()
            path = object_uri[5:]  # Remove "gs://"
        else:
            raise ValueError(f"Unsupported URI scheme: {object_uri}")
        
        return fs, path
    
    @staticmethod
    def get_file(object_uri: str, decode_as_utf_8: bool = True) -> Optional[Union[str, bytes]]:
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
                    subfolder = os.path.basename(file_info.path.rstrip('/'))
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
                rel_path = file_info.path[len(source_path):].lstrip('/')
                
                # Check if file matches substring filters
                if substrings_to_include:
                    if not any(substring in rel_path for substring in substrings_to_include):
                        continue
                
                # Create destination directory if needed
                if "/" in rel_path:
                    dest_dir = os.path.join(path, os.path.dirname(rel_path))
                    os.makedirs(dest_dir, exist_ok=True)
                    
                # Download the file
                dest_path = os.path.join(path, rel_path)
                with fs.open_input_file(file_info.path) as source_file:
                    with open(dest_path, 'wb') as dest_file:
                        dest_file.write(source_file.read())
                        
        except Exception as e:
            logger.exception(f"Error downloading files from {bucket_uri}: {e}")
            raise

    @staticmethod
    def download_model(
        destination_path: str, 
        bucket_uri: str, 
        tokenizer_only: bool
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
                    f_hash = f.read().decode('utf-8').strip()
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
            tokenizer_file_substrings = ["tokenizer", "config.json"] if tokenizer_only else []
            CloudFileSystem.download_files(
                path=destination_dir,
                bucket_uri=bucket_uri,
                substrings_to_include=tokenizer_file_substrings,
            )
            
        except Exception as e:
            logger.exception(f"Error downloading model from {bucket_uri}: {e}")
            raise


# Maintain backward compatibility with existing function names
def _get_fs_and_path(object_uri: str) -> Tuple[pa_fs.FileSystem, str]:
    """Legacy wrapper for CloudFileSystem.get_fs_and_path"""
    return CloudFileSystem.get_fs_and_path(object_uri)


def get_file_from_s3(
    object_uri: str, decode_as_utf_8: bool = True
) -> Optional[Union[str, bytes]]:
    """Legacy wrapper for CloudFileSystem.get_file"""
    return CloudFileSystem.get_file(object_uri, decode_as_utf_8)


def get_gcs_bucket_name_and_prefix(
    bucket_uri: str, is_file: bool = False
) -> Tuple[str, str]:
    """Gets the GCS bucket name and prefix from the bucket_uri.

    The bucket name never includes a trailing slash.
    If is_file is False, the prefix always includes a trailing slash.

    Args:
        bucket_uri: The URI to the directory path or the file path on remote
            storage.
        is_file: If bucket_uri is a file path and not a directory path.

    Returns:
        Tuple containing a bucket name and the object / directory prefix.
    """

    if not bucket_uri.startswith("gs://"):
        raise ValueError(
            f'Got invalid bucket_uri "{bucket_uri}". Expected a value that '
            'starts with "gs://".'
        )

    stripped_uri = bucket_uri[len("gs://") :]
    split_uri = stripped_uri.split("/", maxsplit=1)

    bucket_name = split_uri[0]

    if len(split_uri) > 1:
        bucket_prefix = split_uri[1]
    else:
        bucket_prefix = ""

    # Ensure non-empty bucket_prefixes have a trailing slash.
    if not is_file and bucket_prefix != "" and not bucket_prefix.endswith("/"):
        bucket_prefix += "/"

    return bucket_name, bucket_prefix


def get_file_from_gcs(
    object_uri: str,
    decode_as_utf_8: bool = True,
) -> Optional[Union[str, bytes]]:
    """Legacy wrapper for CloudFileSystem.get_file"""
    return CloudFileSystem.get_file(object_uri, decode_as_utf_8)


def list_subfolders_s3(folder_uri: str) -> List[str]:
    """Legacy wrapper for CloudFileSystem.list_subfolders"""
    return CloudFileSystem.list_subfolders(folder_uri)


def list_subfolders_gcs(folder_uri: str) -> List[str]:
    """Legacy wrapper for CloudFileSystem.list_subfolders"""
    return CloudFileSystem.list_subfolders(folder_uri)


def download_files_from_gcs(
    path: str,
    bucket_uri: str,
    substrings_to_include: Optional[List[str]] = None,
) -> None:
    """Legacy wrapper for CloudFileSystem.download_files"""
    CloudFileSystem.download_files(path, bucket_uri, substrings_to_include)


def download_model_from_gcs(
    destination_path: str, bucket_uri: str, tokenizer_only: bool
) -> None:
    """Legacy wrapper for CloudFileSystem.download_model"""
    CloudFileSystem.download_model(destination_path, bucket_uri, tokenizer_only)


def check_s3_path_exists_and_can_be_accessed(
    s3_folder_uri: str,
    aws_executable: str = AWS_EXECUTABLE,
    subprocess_run=subprocess.run,
    env: Optional[Dict[str, str]] = None,
) -> bool:
    """
    Check if a given path exists and can be accessed in an S3 bucket.

    :param s3_folder_uri: The Path object pointing to the desired folder in S3.
    :param aws_executable: Path to the AWS CLI executable.
    :param env: Environment variables to be passed to the subprocess.
    :param subprocess_run: the subprocess run method, added for testing.
    :return: True if the path exists, False otherwise.
    """
    try:
        fs, path = CloudFileSystem.get_fs_and_path(s3_folder_uri)
        file_info = fs.get_file_info(path)
        return file_info.type in (pa_fs.FileType.Directory, pa_fs.FileType.File)
    except Exception:
        # Fall back to CLI method if PyArrow fails
        # Use AWS CLI to list objects in the specified folder
        result = subprocess_run(
            [aws_executable, "s3", "ls", s3_folder_uri],
            capture_output=True,
            env=env,
        )
        # If the command executed successfully and the output is not empty, the folder exists
        return result.returncode == 0 and bool(result.stdout.strip())


def download_files_from_s3(
    path: str,
    bucket_uri: str,
    s3_sync_args: Optional[List[str]] = None,
    aws_executable: str = AWS_EXECUTABLE,
    env: Optional[Dict[str, str]] = None,
) -> None:
    """Download files from an S3 bucket to disk.

    Args:
        path: The path to download to.
        bucket_uri: The s3 URI to download from.
        s3_sync_args: Args to pass to s3.
        aws_executable: Name of the AWS executable.
        env: Passed to subprocess.check_output().
    """
    path = str(path)
    os.makedirs(path, exist_ok=True)
    
    # Check if we can use PyArrow directly
    try:
        # If s3_sync_args includes complex filtering that PyArrow can't handle,
        # we should fall back to the CLI method
        if s3_sync_args and any("--exclude" in arg or "--include" in arg for arg in s3_sync_args):
            raise ValueError("Complex filtering requires AWS CLI")
        
        # Check that URI exists
        if not check_s3_path_exists_and_can_be_accessed(bucket_uri, aws_executable, env=env):
            if not bucket_uri.endswith("/"):
                bucket_uri_with_slash = bucket_uri + "/"
                if not check_s3_path_exists_and_can_be_accessed(
                    bucket_uri_with_slash, aws_executable, env=env
                ):
                    raise FileNotFoundError(f"URI {bucket_uri} does not exist.")
                bucket_uri = bucket_uri_with_slash
            else:
                raise FileNotFoundError(f"URI {bucket_uri} does not exist.")
        
        logger.info("Downloading files from %s to %s using PyArrow", bucket_uri, path)
        CloudFileSystem.download_files(path, bucket_uri)
        return
    except Exception as e:
        logger.info(f"PyArrow download failed, falling back to AWS CLI: {e}")
    
    # Fall back to CLI method
    s3_sync_args = s3_sync_args or []
    
    # Check that URI exists
    exists = check_s3_path_exists_and_can_be_accessed(
        bucket_uri, aws_executable, env=env
    )
    if not exists and not bucket_uri.endswith("/"):
        bucket_uri += "/"
        exists = check_s3_path_exists_and_can_be_accessed(
            bucket_uri, aws_executable, env=env
        )
    if not exists:
        raise FileNotFoundError(f"URI {bucket_uri} does not exist.")
    
    logger.info("Downloading files from %s to %s using AWS CLI", bucket_uri, path)
    try:
        subprocess.check_output(
            [aws_executable, "s3", "sync", "--quiet"]
            + s3_sync_args
            + [bucket_uri, path],
            env=env,
        )
    except subprocess.CalledProcessError:
        logger.exception("Encountered an error while downloading files.")


def download_model_from_s3(
    path: str,
    bucket_uri: str,
    s3_sync_args: Optional[List[str]] = None,
    tokenizer_only: bool = False,
    aws_executable: str = AWS_EXECUTABLE,
    env: Optional[Dict[str, str]] = None,
) -> None:
    """
    Download a model from an S3 bucket and save it in TRANSFORMERS_CACHE for
    seamless interoperability with Hugging Face's Transformers library.

    The downloaded model may have a 'hash' file containing the commit hash
    corresponding to the commit on Hugging Face Hub.
    """
    path = str(path)
    extended_env = None
    if env:
        extended_env = {**os.environ.copy(), **env}
    
    # Try using PyArrow first
    try:
        if s3_sync_args and (
            "--exclude" in s3_sync_args or "--include" in s3_sync_args
        ):
            raise ValueError("Complex filtering requires AWS CLI")
        
        # Make sure the hash file is not present in the local directory
        if os.path.exists(os.path.join("..", "hash")):
            os.remove(os.path.join("..", "hash"))
        
        fs, source_path = CloudFileSystem.get_fs_and_path(bucket_uri)
        
        # Get hash file first
        hash_path = os.path.join(source_path, "hash")
        try:
            with fs.open_input_file(hash_path) as f:
                f_hash = f.read().decode("utf-8").strip()
                with open(os.path.join("..", "hash"), "w") as local_hash:
                    local_hash.write(f_hash)
        except:
            f_hash = "0000000000000000000000000000000000000000"
            logger.warning(
                f"hash file does not exist in {bucket_uri}. Using {f_hash} as the hash."
            )
        
        # Create necessary directories
        target_path = os.path.join(path, "snapshots", f_hash)
        os.makedirs(target_path, exist_ok=True)
        os.makedirs(os.path.join(path, "refs"), exist_ok=True)
        
        # Download model files
        if tokenizer_only:
            substrings_to_include = ["token", "config.json"]
        else:
            substrings_to_include = None
        
        CloudFileSystem.download_files(
            path=target_path,
            bucket_uri=bucket_uri,
            substrings_to_include=substrings_to_include,
        )
        
        # Write hash to refs/main
        with open(os.path.join(path, "refs", "main"), "w") as f:
            f.write(f_hash)
        
        return
    except Exception as e:
        logger.info(f"PyArrow download failed, falling back to AWS CLI: {e}")
    
    # Fall back to CLI method
    s3_sync_args = s3_sync_args or []

    # Make sure the hash file is not present in the local directory
    if os.path.exists(os.path.join("..", "hash")):
        os.remove(os.path.join("..", "hash"))

    s3_hash_file_path = os.path.join(bucket_uri, "hash")
    try:
        subprocess.check_output(
            [aws_executable, "s3", "cp", "--quiet"]
            + s3_sync_args
            + [s3_hash_file_path, "."],
            env=extended_env,
        )
    except subprocess.CalledProcessError:
        logger.exception(
            "Encountered an error while copying the hash file at "
            f"{s3_hash_file_path} to the working directory ({os.getcwd()})."
        )

    if not os.path.exists(os.path.join("..", "hash")):
        f_hash = "0000000000000000000000000000000000000000"
        logger.warning(
            f"hash file does not exist in {bucket_uri}. Using {f_hash} as the hash."
        )
    else:
        with open(os.path.join("..", "hash"), "r") as f:
            f_hash = f.read().strip()

    target_path = os.path.join(path, "snapshots", f_hash)
    subprocess.check_output(["mkdir", "-p", target_path])
    subprocess.check_output(["mkdir", "-p", os.path.join(path, "refs")])

    download_files_from_s3(
        target_path,
        bucket_uri,
        s3_sync_args=s3_sync_args
        + (
            ["--exclude", "*", "--include", "*token*", "--include", "config.json"]
            if tokenizer_only
            else []
        ),
        aws_executable=aws_executable,
        env=extended_env,
    )
    with open(os.path.join(path, "refs", "main"), "w") as f:
        f.write(f_hash)


def get_aws_credentials(
    s3_aws_credentials_config: S3AWSCredentials,
) -> Optional[Dict[str, str]]:
    """
    This function creates temporary AWS credentials from a configured rayllm by issuing a POST request to the configured API.
    The function optionally uses an env variable for authorization and the returned result is a set of env variables that should
    be injected to the process issuing the S3 sync.
    """
    token = (
        os.getenv(s3_aws_credentials_config.auth_token_env_variable)
        if s3_aws_credentials_config.auth_token_env_variable
        else None
    )
    headers = {"Authorization": f"Bearer {token}"} if token else None
    resp = requests.post(
        s3_aws_credentials_config.create_aws_credentials_url, headers=headers
    )
    if not resp.ok:
        logger.error(f"Request to create AWS credentials had failed with {resp.reason}")
        return None

    env = resp.json()
    return env


class CacheEntry(NamedTuple):
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
        self._cache: Dict[str, CacheEntry] = {}
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

        self._cache[key] = CacheEntry(value, expire_time)

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
