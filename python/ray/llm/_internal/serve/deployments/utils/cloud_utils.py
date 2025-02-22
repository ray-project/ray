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

# TODO (genesu): remove dependency on boto3. Lazy import in the functions.
import boto3
import requests
import subprocess
import time
import inspect
import asyncio

from ray.llm._internal.serve.observability.logging import get_logger
from ray.llm._internal.serve.configs.server_models import S3AWSCredentials


T = TypeVar("T")

logger = get_logger(__name__)

AWS_EXECUTABLE = "aws"
GCP_EXECUTABLE = "gcloud"


def get_file_from_s3(
    object_uri: str, decode_as_utf_8: bool = True
) -> Optional[Union[str, bytes]]:
    """Download a file from an S3 bucket into memory.

    Args:
        object_uri: URI of the file we want to download
        decode_as_utf_8: If True, decode the body of the retrieved file as utf-8.

    Return: contents of file as string or bytes depending on value of
        decode_as_utf_8. If the file does not exist, returns None.
    """
    # Parse the S3 path string to extract bucket name and object key
    path_parts = object_uri.replace("s3://", "").split("/", 1)
    bucket_name = path_parts[0]
    object_key = path_parts[1]
    s3_client = boto3.client("s3")
    try:
        obj = s3_client.get_object(Bucket=bucket_name, Key=object_key)
    except (s3_client.exceptions.NoSuchBucket, s3_client.exceptions.NoSuchKey):
        logger.info(f"URI {object_uri} does not exist.")
        return None
    body = obj["Body"].read()
    if decode_as_utf_8:
        body = body.decode("utf-8")
    return body


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


def get_gcs_client():
    """Returns the default gcs client"""

    try:
        from google.cloud import storage
    except ImportError as e:
        raise ImportError(
            "You must `pip install google-cloud-storage` "
            "to download from Google Cloud Storage."
        ) from e

    return storage.Client()


def get_file_from_gcs(
    object_uri: str,
    decode_as_utf_8: bool = True,
) -> Optional[Union[str, bytes]]:
    """Download a file from a Google Cloud Storage bucket into memory.

    Args:
        object_uri: URI of the file we want to download
        decode_as_utf_8: If True, decode the body of the retrieved file as utf-8.

    Return: contents of file as string or bytes depending on value of
        decode_as_utf_8. If the file does not exist, returns None.
    """

    try:
        from google.api_core.exceptions import Forbidden, NotFound
    except ImportError as e:
        raise ImportError(
            "You must `pip install google-cloud-storage` "
            "to download from Google Cloud Storage."
        ) from e

    bucket_name, prefix = get_gcs_bucket_name_and_prefix(object_uri, is_file=True)

    storage_client = get_gcs_client()
    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(prefix)
        body = blob.download_as_string()
    except NotFound:
        logger.info(f"URI {object_uri} does not exist.")
        return None
    except Forbidden:
        logger.info(f"URI {object_uri} is throwing forbidden access.")
        return None

    if decode_as_utf_8:
        body = body.decode(encoding="utf-8")
    return body


def list_subfolders_s3(folder_uri: str) -> List[str]:
    """List the subfolders of an S3 folder.

    Not recursive. Lists only the immediate children of the folder.

    Args:
        folder_uri: the folder to read.

    Return: list of subfolder names in an S3 folder. None of the subfolders
        have a trailing slash.
    """

    # Ensure that the folder_uri has a trailing slash.
    folder_uri = f"{folder_uri.rstrip('/')}/"

    # Parse the S3 path string to extract bucket name and object key
    path_parts = folder_uri.replace("s3://", "").split("/", 1)
    bucket_name = path_parts[0]
    folder_prefix = path_parts[1]
    s3_client = boto3.client("s3")
    try:
        objs = s3_client.list_objects_v2(
            Bucket=bucket_name, Prefix=folder_prefix, Delimiter="/"
        )
    except (s3_client.exceptions.NoSuchBucket, s3_client.exceptions.NoSuchKey):
        logger.info(f"Folder URI {folder_uri} does not exist. No LoRA adapters found.")
        return []

    subfolders = []
    if "CommonPrefixes" in objs:
        for common_prefix_dict in objs.get("CommonPrefixes", {}):
            if "Prefix" in common_prefix_dict:
                common_prefix = common_prefix_dict["Prefix"]
                subfolder = common_prefix[len(folder_prefix) :].rstrip("/")
                subfolders.append(subfolder)

    return subfolders


def list_subfolders_gcs(folder_uri: str) -> List[str]:
    """List the subfolders of a Google Cloud Storage folder.

    Not recursive. Lists only the immediate children of the folder.

    Args:
        folder_uri: the folder to read.

    Return: list of subfolder names in a GCS folder. None of the subfolders
        have a trailing slash.
    """

    # Ensure that the folder_uri has a trailing slash.
    folder_uri = f"{folder_uri.rstrip('/')}/"

    # Parse the GCS path string to extract bucket name and object key
    path_parts = folder_uri.replace("gs://", "").split("/", 1)
    bucket_name = path_parts[0]
    folder_prefix = path_parts[1]

    gcs_client = get_gcs_client()

    try:
        from google.api_core.exceptions import Forbidden, NotFound
    except ImportError:
        logger.exception(
            "You must `pip install google-cloud-storage` "
            "to check models in Google Cloud Storage."
        )
        return []

    try:
        # NOTE (shrekris): list_blobs() has a delimiter argument that should
        # allows us to list just the children of the folder. However, that
        # argument doesn't seem to work currently. So instead, we list all
        # the children recursively and keep only the common prefixes.
        blobs = gcs_client.list_blobs(bucket_name, prefix=folder_prefix)
    except NotFound:
        logger.info(f"GCS folder URI {folder_uri} does not exist.")
        return []
    except Forbidden:
        logger.info(f"GCS folder URI {folder_uri} is throwing forbidden access.")
        return []

    subfolders = set()
    for blob in blobs:
        full_name: str = blob.name
        child_name = full_name[len(folder_prefix) :]

        if "/" in child_name:
            # This object is not a file stored directly in the folder_uri. It
            # may be a subfolder or a file in a subfolder. We extract just the
            # subfolder value.
            subfolder = child_name.split("/", 1)[0]
            subfolders.add(subfolder)

    return list(subfolders)


def download_files_from_gcs(
    path: str,
    bucket_uri: str,
    substrings_to_include: Optional[List[str]] = None,
) -> None:
    """Download files from a GCS bucket.

    Args:
        path: The local path that the files should be downloaded to.
        bucket_uri: URI of the path we want to download files from.
        substrings_to_include: List of file sub-strings that should
            be included in the downloaded files. If this is provided the files
            that do not match the provided sub-strings will be skipped by
            default.
    """

    try:
        from google.cloud import storage
    except ImportError as e:
        raise ImportError(
            "You must `pip install google-cloud-storage` "
            "to download models from Google Cloud Storage."
        ) from e

    bucket_name, prefix = get_gcs_bucket_name_and_prefix(bucket_uri)

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # Download all files in bucket to the path/snapshots/<f_hash>/ directory.
    # Blob names can contain slashes (/). However, GCS doesn't actually contain
    # true directories. We create the directories manually before downloading
    # blobs to mirror the directory structure in the bucket.
    for blob in bucket.list_blobs(prefix=prefix):
        # Remove the prefix from each blob's name
        blob_base_name = blob.name[len(prefix) :]

        if substrings_to_include:
            for substring in substrings_to_include:
                if substring not in blob_base_name:
                    continue

        if "/" in blob_base_name:
            blob_source_dir = blob_base_name[: blob_base_name.rfind("/")]
            blob_destination_dir = os.path.join(path, blob_source_dir)
            os.makedirs(blob_destination_dir, exist_ok=True)

        blob_destination_path = os.path.join(path, blob_base_name)

        # If the blob is a file (not a directory), we download it.
        blob_is_file = not blob_destination_path.endswith("/")
        if blob_is_file:
            blob.download_to_filename(blob_destination_path)


def download_model_from_gcs(
    destination_path: str, bucket_uri: str, tokenizer_only: bool
) -> None:
    """
    Download a model from a GCS bucket and save it in TRANSFORMERS_CACHE for
    seamless interoperability with Hugging Face's Transformers library.

    The downloaded model may have a 'hash' file containing the commit hash corresponding
    to the commit on Hugging Face Hub. If not, we set the hash to a default
    value.

    The files are downloaded to the destination_path/snapshots/HASH/ directory.
    This function also writes a destination_path/refs/main file that contains
    the hash.

    Args:
        destination_path: The file path of the directory where all the files
            will be downloaded.
        bucket_uri: The URI of the GCS bucket to download files from.
        tokenizer_only: If True, only the files needed for the model's
            tokenizer will be downloaded.
    """

    try:
        from google.cloud import storage
    except ImportError as e:
        raise ImportError(
            "You must `pip install google-cloud-storage` "
            "to download models from Google Cloud Storage."
        ) from e

    bucket_name, prefix = get_gcs_bucket_name_and_prefix(bucket_uri)

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    logger.info(
        f'Downloading files from GCS bucket "{bucket_name}" at prefix ' f'"{prefix}".'
    )

    # Download hash file if it exists and get the hash. Otherwise, set
    # hash to a default.
    f_hash = "0000000000000000000000000000000000000000"
    for blob in bucket.list_blobs(prefix=prefix):
        if blob.name == f"{prefix}hash":
            blob.download_to_filename("./hash")
            with open(os.path.join("..", "hash"), "r") as f:
                f_hash = f.read().strip()
            logger.info(
                f"Detected hash file in GCS bucket {bucket_uri}. "
                f"Using {f_hash} as the hash."
            )
            break
    else:
        logger.warning(
            f"Hash file does not exist in GCS bucket {bucket_uri}. "
            f"Using {f_hash} as the hash."
        )

    # Write hash name to path/refs/main file.
    main_dir = os.path.join(destination_path, "refs")
    os.makedirs(main_dir, exist_ok=True)
    with open(os.path.join(main_dir, "main"), "w") as f:
        f.write(f_hash)

    destination_dir = os.path.join(destination_path, "snapshots", f_hash)
    os.makedirs(destination_dir, exist_ok=True)

    logger.info(f'Downloading model files to directory "{destination_dir}".')

    # Download all files in bucket to the path/snapshots/<f_hash>/ directory.
    # Blob names can contain slashes (/). However, GCS doesn't actually contain
    # true directories. We create the directories manually before downloading
    # blobs to mirror the directory structure in the bucket.
    tokenizer_file_substrings = ["tokenizer", "config.json"] if tokenizer_only else []
    download_files_from_gcs(
        path=destination_dir,
        bucket_uri=bucket_uri,
        substrings_to_include=tokenizer_file_substrings,
    )


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

    This spawns a subprocess running an AWS s3 sync command.
    We run the subprocess as follows:
    `<aws_exacutable> s3 sync --quiet <s3_sync_args> <bucket_uri> <path>`

    Args:
        path: The path to download to.
        bucket_uri: The s3 URI to download from.
        s3_sync_args: Args to pass to s3.
        aws_executable: Name of the AWS executable.
        env: Passed to subprocess.check_output().
    """
    path = str(path)
    os.makedirs(path, exist_ok=True)
    s3_sync_args = s3_sync_args or []
    # check that URI exists
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
    logger.info("Downloading files from %s to %s", bucket_uri, path)
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
