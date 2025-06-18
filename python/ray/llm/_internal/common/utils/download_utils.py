import asyncio
import enum
import os
import time
from functools import wraps
from pathlib import Path
from typing import Dict, List, Optional

from filelock import FileLock

from ray.llm._internal.common.models import DiskMultiplexConfig
from ray.llm._internal.common.observability.logging import get_logger
from ray.llm._internal.common.utils.cloud_utils import (
    CloudFileSystem,
    CloudMirrorConfig,
    CloudModelAccessor,
    LoraMirrorConfig,
    is_remote_path,
)
from ray.llm._internal.common.utils.import_utils import try_import

torch = try_import("torch")

logger = get_logger(__name__)


class NodeModelDownloadable(enum.Enum):
    """Defines which files to download from cloud storage."""

    MODEL_AND_TOKENIZER = enum.auto()
    TOKENIZER_ONLY = enum.auto()
    NONE = enum.auto()

    def __bool__(self):
        return self != NodeModelDownloadable.NONE

    def union(self, other: "NodeModelDownloadable") -> "NodeModelDownloadable":
        """Return a NodeModelDownloadable that is a union of this and the other."""
        if (
            self == NodeModelDownloadable.MODEL_AND_TOKENIZER
            or other == NodeModelDownloadable.MODEL_AND_TOKENIZER
        ):
            return NodeModelDownloadable.MODEL_AND_TOKENIZER

        if (
            self == NodeModelDownloadable.TOKENIZER_ONLY
            or other == NodeModelDownloadable.TOKENIZER_ONLY
        ):
            return NodeModelDownloadable.TOKENIZER_ONLY

        return NodeModelDownloadable.NONE


def get_model_entrypoint(model_id: str) -> str:
    """Get the path to entrypoint of the model on disk if it exists, otherwise return the model id as is.

    Entrypoint is typically <TRANSFORMERS_CACHE>/models--<model_id>/

    Args:
        model_id: Hugging Face model ID.

    Returns:
        The path to the entrypoint of the model on disk if it exists, otherwise the model id as is.
    """
    from transformers.utils.hub import TRANSFORMERS_CACHE

    model_dir = Path(
        TRANSFORMERS_CACHE, f"models--{model_id.replace('/', '--')}"
    ).expanduser()
    if not model_dir.exists():
        return model_id
    return str(model_dir.absolute())


def get_model_location_on_disk(model_id: str) -> str:
    """Get the location of the model on disk if exists, otherwise return the model id as is.

    Args:
        model_id: Hugging Face model ID.
    """
    model_dir = Path(get_model_entrypoint(model_id))
    model_id_or_path = model_id

    model_dir_refs_main = Path(model_dir, "refs", "main")

    if model_dir.exists():
        if model_dir_refs_main.exists():
            # If refs/main exists, use the snapshot hash to find the model
            # and check if *config.json (could be config.json for general models
            # or adapter_config.json for LoRA adapters) exists to make sure it
            # follows HF model repo structure.
            with open(model_dir_refs_main, "r") as f:
                snapshot_hash = f.read().strip()

            snapshot_hash_path = Path(model_dir, "snapshots", snapshot_hash)
            if snapshot_hash_path.exists() and list(
                Path(snapshot_hash_path).glob("*config.json")
            ):
                model_id_or_path = str(snapshot_hash_path.absolute())
        else:
            # If it doesn't have refs/main, it is a custom model repo
            # and we can just return the model_dir.
            model_id_or_path = str(model_dir.absolute())

    return model_id_or_path


class CloudModelDownloader(CloudModelAccessor):
    """Unified downloader for models stored in cloud storage (S3 or GCS).

    Args:
        model_id: The model id to download.
        mirror_config: The mirror config for the model.
    """

    def get_model(
        self,
        tokenizer_only: bool,
    ) -> str:
        """Gets a model from cloud storage and stores it locally.

        Args:
            tokenizer_only: whether to download only the tokenizer files.

        Returns: file path of model if downloaded, else the model id.
        """
        bucket_uri = self.mirror_config.bucket_uri

        if bucket_uri is None:
            return self.model_id

        lock_path = self._get_lock_path()
        path = self._get_model_path()
        storage_type = self.mirror_config.storage_type

        try:
            # Timeout 0 means there will be only one attempt to acquire
            # the file lock. If it cannot be acquired, a TimeoutError
            # will be thrown.
            # This ensures that subsequent processes don't duplicate work.
            with FileLock(lock_path, timeout=0):
                try:
                    CloudFileSystem.download_model(
                        destination_path=path,
                        bucket_uri=bucket_uri,
                        tokenizer_only=tokenizer_only,
                    )
                    logger.info(
                        "Finished downloading %s for %s from %s storage",
                        "tokenizer" if tokenizer_only else "model and tokenizer",
                        self.model_id,
                        storage_type.upper() if storage_type else "cloud",
                    )
                except RuntimeError:
                    logger.exception(
                        "Failed to download files for model %s from %s storage",
                        self.model_id,
                        storage_type.upper() if storage_type else "cloud",
                    )
        except TimeoutError:
            # If the directory is already locked, then wait but do not do anything.
            with FileLock(lock_path, timeout=-1):
                pass
        return get_model_location_on_disk(self.model_id)

    def get_extra_files(self) -> List[str]:
        """Gets user-specified extra files from cloud storage and stores them in
        provided paths.

        Returns: list of file paths of extra files if downloaded.
        """
        paths = []
        extra_files = self.mirror_config.extra_files or []
        if not extra_files:
            return paths

        lock_path = self._get_lock_path(suffix="-extra_files")
        storage_type = self.mirror_config.storage_type

        logger.info(
            f"Downloading extra files for {self.model_id} from {storage_type} storage"
        )
        try:
            # Timeout 0 means there will be only one attempt to acquire
            # the file lock. If it cannot be acquired, a TimeoutError
            # will be thrown.
            # This ensures that subsequent processes don't duplicate work.
            with FileLock(lock_path, timeout=0):
                for extra_file in extra_files:
                    path = Path(
                        os.path.expandvars(extra_file.destination_path)
                    ).expanduser()
                    paths.append(path)
                    CloudFileSystem.download_files(
                        path=path,
                        bucket_uri=extra_file.bucket_uri,
                    )
        except TimeoutError:
            # If the directory is already locked, then wait but do not do anything.
            with FileLock(lock_path, timeout=-1):
                pass
        return paths


def _log_download_info(
    *, source: str, download_model: NodeModelDownloadable, download_extra_files: bool
):
    if download_model == NodeModelDownloadable.NONE:
        if download_extra_files:
            logger.info("Downloading extra files from %s", source)
        else:
            logger.info("Not downloading anything from %s", source)
    elif download_model == NodeModelDownloadable.TOKENIZER_ONLY:
        if download_extra_files:
            logger.info("Downloading tokenizer and extra files from %s", source)
        else:
            logger.info("Downloading tokenizer from %s", source)
    elif download_model == NodeModelDownloadable.MODEL_AND_TOKENIZER:
        if download_extra_files:
            logger.info("Downloading model, tokenizer, and extra files from %s", source)
        else:
            logger.info("Downloading model and tokenizer from %s", source)


def download_model_files(
    model_id: Optional[str] = None,
    mirror_config: Optional[CloudMirrorConfig] = None,
    download_model: NodeModelDownloadable = NodeModelDownloadable.MODEL_AND_TOKENIZER,
    download_extra_files: bool = True,
) -> Optional[str]:
    """
    Download the model files from the cloud storage. We support two ways to specify
    the remote model path in the cloud storage:
    Approach 1:
    - model_id: The vanilla model id such as "meta-llama/Llama-3.1-8B-Instruct".
    - mirror_config: Config for downloading model from cloud storage.

    Approach 2:
    - model_id: The remote path (s3:// or gs://) in the cloud storage.
    - mirror_config: None.
    In this approach, we will create a CloudMirrorConfig from the model_id and use that
    to download the model.

    Args:
        model_id: The model id.
        mirror_config: Config for downloading model from cloud storage.
        download_model: What parts of the model to download.
        download_extra_files: Whether to download extra files specified in the mirror config.

    Returns:
        The local path to the downloaded model, or the original model ID
        if no cloud storage mirror is configured or if the model is not downloaded.
    """

    # Create the torch cache kernels directory if it doesn't exist.
    # This is a workaround for a torch issue, where the kernels directory
    # cannot be created by torch if the parent directory doesn't exist.
    torch_cache_home = torch.hub._get_torch_home()
    os.makedirs(os.path.join(torch_cache_home, "kernels"), exist_ok=True)
    model_path_or_id = None

    if model_id is None:
        return None

    if mirror_config is None:
        if is_remote_path(model_id):
            logger.info(
                "Creating a CloudMirrorConfig from remote model path %s", model_id
            )
            mirror_config = CloudMirrorConfig(bucket_uri=model_id)
        else:
            logger.info("No cloud storage mirror configured")
            return model_id

    storage_type = mirror_config.storage_type
    source = (
        f"{storage_type.upper()} mirror" if storage_type else "Cloud storage mirror"
    )

    _log_download_info(
        source=source,
        download_model=download_model,
        download_extra_files=download_extra_files,
    )

    downloader = CloudModelDownloader(model_id, mirror_config)

    if download_model != NodeModelDownloadable.NONE:
        model_path_or_id = downloader.get_model(
            tokenizer_only=download_model == NodeModelDownloadable.TOKENIZER_ONLY
        )

    if download_extra_files:
        downloader.get_extra_files()

    return model_path_or_id


def download_lora_adapter(
    lora_name: str,
    remote_path: Optional[str] = None,
    lora_root: Optional[str] = None,
    download_timeout_s: Optional[float] = None,
    max_tries: int = 1,
) -> str:
    """Download a LoRA adapter from remote storage to local directory.

    This function supports both simple downloading (for backward compatibility)
    and advanced downloading with caching and retry functionality.

    Args:
        lora_name: The lora name.
        remote_path: The remote path to the lora. If specified, the remote_path will be
            used as the base path to load the lora.
        lora_root: Path to directory where LoRA weights will be cached (for advanced mode).
        download_timeout_s: Download timeout in seconds (for advanced mode).
        max_tries: Number of retry attempts (for advanced mode).

    Returns:
        The local path to the lora if remote_path is specified, otherwise the lora name.
    """
    assert not is_remote_path(
        lora_name
    ), "lora_name cannot be a remote path (s3:// or gs://)"

    if remote_path is None:
        return lora_name

    # Use the new unified LoRA downloading functionality
    lora_path = os.path.join(remote_path, lora_name)

    # If advanced parameters are provided, use the new LoraModelLoader
    if lora_root is not None or download_timeout_s is not None or max_tries != 1:
        loader = _LoraModelLoader(
            lora_root=lora_root,
            download_timeout_s=download_timeout_s,
            max_tries=max_tries,
        )

        # Create a simple LoraMirrorConfig for the loader
        lora_mirror_config = LoraMirrorConfig(
            lora_model_id=lora_name,
            bucket_uri=lora_path,
            max_total_tokens=None,
        )

        # Use the sync version directly
        disk_config = loader._load_model_sync(lora_mirror_config)
        return disk_config.local_path

    # Fallback to original simple implementation for backward compatibility
    mirror_config = CloudMirrorConfig(bucket_uri=lora_path)
    downloader = CloudModelDownloader(lora_name, mirror_config)
    return downloader.get_model(tokenizer_only=False)


# Utility functions moved from multiplex/utils.py
def get_base_model_id(model_id: str) -> str:
    """Get base model id for a given model id.

    A LoRA fine-tuned model_id is expected to be in the format of
        base_model_id:lora_id
        e.g. meta-llama/Llama-2-7b-chat-hf:my_suffix:aBc1234

    The returned base model id is in the format of
        base_model_id
        e.g. meta-llama/Llama-2-7b-chat-hf

    This function can safely take any string.
    """
    return model_id.split(":")[0]


def get_lora_id(lora_model_id: str) -> str:
    """Get lora id for a given lora model id.

    A LoRA fine-tuned model_id is expected to be in the format of
        base_model_id:lora_id
        e.g. meta-llama/Llama-2-7b-chat-hf:my_suffix:aBc1234

    The returned lora id is in the format of
        lora_id
        e.g. my_suffix:aBc1234

    This function can safely take any string.
    """
    return ":".join(lora_model_id.split(":")[1:])


def clean_model_id(model_id: str):
    return model_id.replace("/", "--")


def clear_directory(dir: str):
    import subprocess

    try:
        subprocess.run(f"rm -r {dir}", shell=True, check=False)
    except FileNotFoundError:
        pass


def retry_with_exponential_backoff(
    max_tries: int,
    exception_to_check: type[Exception],
    base_delay: float = 1,
    max_delay: float = 32,
    exponential_base: float = 2,
):
    """Retry decorator with exponential backoff.

    Args:
        max_tries: Maximum number of retry attempts
        exception_to_check: Exception type to catch and retry on
        base_delay: Initial delay between retries in seconds
        max_delay: Maximum delay between retries in seconds
        exponential_base: Base for exponential calculation

    Returns:
        A decorator function that applies retry logic with exponential backoff
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            delay = base_delay
            last_exception = None

            for attempt in range(max_tries):
                try:
                    return func(*args, **kwargs)
                except exception_to_check as e:
                    last_exception = e
                    if attempt == max_tries - 1:  # Last attempt
                        raise last_exception

                    # Log the failure and retry
                    logger.warning(
                        f"Attempt {attempt + 1}/{max_tries} failed: {str(e)}. "
                        f"Retrying in {delay} seconds..."
                    )
                    time.sleep(delay)
                    # Calculate next delay with exponential backoff
                    delay = min(delay * exponential_base, max_delay)

            # This should never be reached due to the raise in the loop
            raise last_exception if last_exception else RuntimeError(
                "Unexpected error in retry logic"
            )

        return wrapper

    return decorator


class GlobalCounter:
    """Manage a global counter

    This counter should be a singleton global to the process.
    """

    def __init__(self):
        # Initialize to 0, but we never return 0
        self.global_id = 0

    def next(self):
        # The id starts at 1
        self.global_id += 1
        return self.global_id


global_id_manager = GlobalCounter()


def make_async(func):
    """Take a blocking function, and run it on in an executor thread.

    This function prevents the blocking function from blocking the asyncio event loop.
    The code in this function needs to be thread safe.
    """
    from functools import partial

    def _async_wrapper(*args, **kwargs):
        loop = asyncio.get_event_loop()
        func_partial = partial(func, *args, **kwargs)
        return loop.run_in_executor(executor=None, func=func_partial)

    return _async_wrapper


class _LoraModelLoader:
    """Download Lora weights from remote, and manage a CPU memory cache.

    This entire downloader is sync.

    Args:
        lora_root: Path to directory where LoRA weights will be cached.
        download_timeout_s: How much time the download subprocess has to download
            a single LoRA before a timeout. None means no timeout.
        max_tries: Number of times to try downloading a LoRA model if
            the download subprocess fails.
    """

    def __init__(
        self,
        lora_root: Optional[str] = None,
        download_timeout_s: Optional[float] = None,
        max_tries: int = 1,
    ):
        self.lora_root = lora_root or "/tmp/ray/llm/lora/cache"
        self.disk_cache: Dict[str, DiskMultiplexConfig] = {}
        self.active_syncing_tasks: Dict[str, asyncio.Task[DiskMultiplexConfig]] = {}
        if download_timeout_s is not None and download_timeout_s <= 0:
            raise ValueError(
                f"download_timeout_s must be None or >0, got {download_timeout_s}"
            )
        self.download_timeout_s = download_timeout_s
        if max_tries < 1:
            raise ValueError(f"max_tries must be >=1, got {max_tries}")
        self.max_tries = max_tries

    async def load_model(
        self, lora_model_id: str, lora_mirror_config: "LoraMirrorConfig"
    ) -> DiskMultiplexConfig:
        """Load a model.

        This function will load a Lora model from s3 and cache it on disk and in memory.
        This function runs in a separate thread because it does synchronous disk operations.
        """
        if lora_model_id in self.disk_cache:
            return self.disk_cache[lora_model_id]

        if lora_model_id not in self.active_syncing_tasks:
            # Cannot use _load_model directly in create_task
            # due to TypeError: a coroutine was expected, got <Future...
            task = asyncio.create_task(self._load_model_async(lora_mirror_config))
            task.add_done_callback(
                lambda result: self.active_syncing_tasks.pop(lora_model_id, None)
            )
            self.active_syncing_tasks[lora_model_id] = task
        else:
            task = self.active_syncing_tasks[lora_model_id]

        # Ensure that cancellation of the current request doesn't
        # affect other requests
        disk_config = await asyncio.shield(task)

        # If we are successful, add the result to the disk cache
        # This will not be reached if the task raises an exception
        self.disk_cache[lora_model_id] = disk_config

        return disk_config

    async def _load_model_async(
        self, lora_mirror_config: "LoraMirrorConfig"
    ) -> DiskMultiplexConfig:
        return await self._load_model(lora_mirror_config)

    @make_async
    def _load_model(
        self, lora_mirror_config: "LoraMirrorConfig"
    ) -> DiskMultiplexConfig:
        return self._load_model_sync(lora_mirror_config)

    @make_async
    def clear_cache(self):
        """Clear the disk cache

        Note: clear_disk_cache currently blindly clears the disk cache and is not
         thread / process safe because another process
         may be reading the cache as it is being cleared.

         TODO(tchordia): come up with a way to clear the Lora Disk cache.
        """
        clear_directory(self.lora_root)

    def _model_dir_path(self, model_id: str) -> str:
        """Construct the path for the lora weight.

        Given a lora model id is expected to be in the format of
            base_model_id:lora_id
        This function will return the path to the directory where the lora weights
            lora_root/lora_id
        """
        lora_id = get_lora_id(clean_model_id(model_id))
        path = os.path.join(self.lora_root, lora_id)
        os.makedirs(path, exist_ok=True)
        return path

    def _download_lora(self, lora_mirror_config: "LoraMirrorConfig") -> str:
        # Note (genesu): `model_local_path` affects where the lora weights are stored
        # on local disk.
        model_local_path = self._model_dir_path(lora_mirror_config.lora_model_id)
        self._sync_model(
            lora_mirror_config.bucket_uri,
            model_local_path,
            timeout=self.download_timeout_s,
            sync_args=lora_mirror_config.sync_args,
        )
        return model_local_path

    def _sync_model(
        self,
        bucket_uri: str,
        local_path: str,
        timeout: Optional[float] = None,
        sync_args: Optional[List[str]] = None,
    ):
        """Sync from bucket_uri to local_path.

        This method isn't re-entrant and will block (up to timeout) if already syncing
        at a given path.
        """

        logger.info("Downloading %s to %s", bucket_uri, local_path)

        with FileLock(local_path + ".lock", timeout=timeout or -1):
            try:
                # Use CloudFileSystem.download_files for the sync operation
                CloudFileSystem.download_files(
                    path=local_path,
                    bucket_uri=bucket_uri,
                )
            except Exception as e:
                logger.error(
                    "Failed to sync model (%s) from %s to %s",
                    str(e),
                    bucket_uri,
                    local_path,
                )
                raise

    def _load_model_sync(
        self, lora_mirror_config: "LoraMirrorConfig"
    ) -> DiskMultiplexConfig:
        """Load a model from the given mirror configuration."""

        # Apply retry decorator to _download_lora at runtime with instance parameters
        download_with_retries = retry_with_exponential_backoff(
            max_tries=self.max_tries,
            exception_to_check=Exception,  # Catch any exception from CloudFileSystem
        )(lambda config: self._download_lora(config))

        local_path = download_with_retries(lora_mirror_config)
        # the lora_assigned_id is consistent for the lifetime of the disk cache entry
        # If the disk cache is cleared, a new id will be generated.
        return DiskMultiplexConfig.model_validate(
            {
                "model_id": lora_mirror_config.lora_model_id,
                "max_total_tokens": lora_mirror_config.max_total_tokens,
                "local_path": local_path,
                "lora_assigned_int_id": global_id_manager.next(),
            }
        )
