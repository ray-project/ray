import enum
import os
from pathlib import Path
from typing import List, Optional

from filelock import FileLock

from ray.llm._internal.utils import try_import
from ray.llm._internal.common.observability.logging import get_logger
from ray.llm._internal.common.utils.cloud_utils import (
    CloudFileSystem,
    CloudMirrorConfig,
    is_remote_path,
)

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


def get_model_location_on_disk(model_id: str) -> str:
    """Get the location of the model on disk.

    Args:
        model_id: Hugging Face model ID.
    """
    from transformers.utils.hub import TRANSFORMERS_CACHE

    model_dir = Path(
        TRANSFORMERS_CACHE, f"models--{model_id.replace('/', '--')}"
    ).expanduser()
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


class CloudModelDownloader:
    """Unified downloader for models stored in cloud storage (S3 or GCS).

    Args:
        model_id: The model id to download.
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
        # Delayed import to avoid circular dependencies
        from transformers.utils.hub import TRANSFORMERS_CACHE

        return Path(
            TRANSFORMERS_CACHE, f"models--{self.model_id.replace('/', '--')}"
        ).expanduser()

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
) -> str:
    """If remote_path is specified, pull the lora to the local
    directory and return the local path.

    TODO: Refactor lora_model_loader in llm/_intenral/serve/deployments/llm/multiplex
    and move them here to unify with this function.

    Args:
        lora_name: The lora name.
        remote_path: The remote path to the lora. If specified, the remote_path will be
            used as the base path to load the lora.

    Returns:
        The local path to the lora if remote_path is specified, otherwise the lora name.
    """
    assert not is_remote_path(
        lora_name
    ), "lora_name cannot be a remote path (s3:// or gs://)"

    if remote_path is None:
        return lora_name

    lora_path = os.path.join(remote_path, lora_name)
    mirror_config = CloudMirrorConfig(bucket_uri=lora_path)
    downloader = CloudModelDownloader(lora_name, mirror_config)
    return downloader.get_model(tokenizer_only=False)
