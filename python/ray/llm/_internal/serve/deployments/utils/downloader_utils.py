import os
from pathlib import Path
from typing import List

from filelock import FileLock

from ray.llm._internal.serve.observability.logging import get_logger
from ray.llm._internal.serve.deployments.utils.cloud_utils import CloudFileSystem
from ray.llm._internal.serve.configs.server_models import CloudMirrorConfig

logger = get_logger(__name__)


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

    if model_dir.exists() and model_dir_refs_main.exists():
        with open(model_dir_refs_main, "r") as f:
            snapshot_hash = f.read().strip()

        snapshot_hash_path = Path(model_dir, "snapshots", snapshot_hash)
        if (
            snapshot_hash_path.exists()
            and Path(snapshot_hash_path, "config.json").exists()
        ):
            model_id_or_path = str(snapshot_hash_path.absolute())

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
