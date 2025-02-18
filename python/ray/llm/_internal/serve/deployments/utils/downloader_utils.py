import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List

from filelock import FileLock

from ray.llm._internal.serve.observability.logging import get_logger
from ray.llm._internal.serve.deployments.utils.cloud_utils import (
    download_files_from_gcs,
    download_files_from_s3,
    download_model_from_gcs,
    download_model_from_s3,
    get_aws_credentials,
)
from ray.llm._internal.serve.configs.server_models import (
    GCSMirrorConfig,
    MirrorConfig,
    S3MirrorConfig,
)

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


class BaseDownloader(ABC):
    """Base class for downloading models from cloud storage."""

    def __init__(self, model_id: str, mirror_config: "MirrorConfig"):
        self.model_id = model_id
        self.mirror_config = mirror_config

    def _get_lock_path(self, suffix: str = "") -> Path:
        return Path(
            "~", f"{self.model_id.replace('/', '--')}{suffix}.lock"
        ).expanduser()

    def _get_model_path(self) -> Path:
        # TODO (shrekris-anyscale): add comment for why this is a delayed import.
        from transformers.utils.hub import TRANSFORMERS_CACHE

        return Path(
            TRANSFORMERS_CACHE, f"models--{self.model_id.replace('/', '--')}"
        ).expanduser()

    @abstractmethod
    def get_model(
        self,
        tokenizer_only: bool,
    ) -> str:
        """Gets a model from cloud and stores it locally.

        Args:
            tokenizer_only: whether to download only the tokenizer files.

        Returns: file path of model if downloaded, else the model id.
        """
        ...

    @abstractmethod
    def get_extra_files(self) -> List[str]:
        """Gets user-specified extra files from cloud and stores them in
        provided paths.

        Returns: list of file paths of extra files if downloaded.
        """
        ...


class S3Downloader(BaseDownloader):
    """Downloader for models stored in S3.

    Args:
        model_id: the model id.
        mirror_config: the mirror config for the model.
    """

    def __init__(self, model_id: str, mirror_config: "S3MirrorConfig"):
        super().__init__(model_id, mirror_config)

    def _get_env_vars(self):
        env_vars = None
        if self.mirror_config.s3_aws_credentials is not None:
            env_vars = get_aws_credentials(self.mirror_config.s3_aws_credentials)
        return env_vars

    def get_model(
        self,
        tokenizer_only: bool,
    ) -> str:
        bucket_uri = self.mirror_config.bucket_uri

        if bucket_uri is None:
            return self.model_id

        env_vars = self._get_env_vars()
        lock_path = self._get_lock_path()
        path = self._get_model_path()

        try:
            # Timeout 0 means there will be only one attempt to acquire
            # the file lock. If it cannot be aquired, a TimeoutError
            # will be thrown.
            # This ensures that subsequent processes don't duplicate work.
            with FileLock(lock_path, timeout=0):
                s3_sync_args = (
                    self.mirror_config.s3_sync_args if self.mirror_config else None
                )
                try:
                    download_model_from_s3(
                        path,
                        bucket_uri,
                        s3_sync_args=s3_sync_args,
                        tokenizer_only=tokenizer_only,
                        env=env_vars,
                    )
                    logger.info(
                        "Finished downloading %s for %s from S3 bucket",
                        "tokenizer" if tokenizer_only else "model and tokenizer",
                        self.model_id,
                    )
                except RuntimeError:
                    logger.exception(
                        "Failed to download files for model %s from S3 bucket",
                        self.model_id,
                    )
        except TimeoutError:
            # If the directory is already locked, then wait but do not do anything.
            with FileLock(lock_path, timeout=-1):
                pass
        return get_model_location_on_disk(self.model_id)

    def get_extra_files(self) -> List[str]:
        paths = []
        extra_files = self.mirror_config.extra_files or []
        if not extra_files:
            return paths

        env_vars = self._get_env_vars()
        lock_path = self._get_lock_path(suffix="-extra_files")

        try:
            # Timeout 0 means there will be only one attempt to acquire
            # the file lock. If it cannot be aquired, a TimeoutError
            # will be thrown.
            # This ensures that subsequent processes don't duplicate work.
            with FileLock(lock_path, timeout=0):
                s3_sync_args = (
                    self.mirror_config.s3_sync_args if self.mirror_config else None
                )
                for extra_file in extra_files:
                    path = Path(
                        os.path.expandvars(extra_file.destination_path)
                    ).expanduser()
                    paths.append(path)
                    download_files_from_s3(
                        path=path,
                        bucket_uri=extra_file.bucket_uri,
                        s3_sync_args=s3_sync_args,
                        env=env_vars,
                    )
                    logger.info(
                        "Finished downloading extra files for %s from S3 bucket",
                        self.model_id,
                    )
        except TimeoutError:
            # If the directory is already locked, then wait but do not do anything.
            with FileLock(lock_path, timeout=-1):
                pass
        return paths


class GCSDownloader(BaseDownloader):
    """Downloader for models stored in GCS.

    Args:
        model_id: the model id.
        mirror_config: the mirror config for the model.
    """

    def __init__(self, model_id: str, mirror_config: "GCSMirrorConfig"):
        super().__init__(model_id, mirror_config)

    def get_model(
        self,
        tokenizer_only: bool,
    ) -> str:
        bucket_uri = self.mirror_config.bucket_uri

        if bucket_uri is None:
            return self.model_id

        lock_path = self._get_lock_path()
        path = self._get_model_path()

        try:
            # Timeout 0 means there will be only one attempt to acquire
            # the file lock. If it cannot be aquired, a TimeoutError
            # will be thrown.
            # This ensures that subsequent processes don't duplicate work.
            with FileLock(lock_path, timeout=0):
                try:
                    download_model_from_gcs(
                        destination_path=path,
                        bucket_uri=bucket_uri,
                        tokenizer_only=tokenizer_only,
                    )
                    logger.info(
                        "Finished downloading %s for %s from GCS bucket",
                        "tokenizer" if tokenizer_only else "model and tokenizer",
                        self.model_id,
                    )
                except RuntimeError:
                    logger.exception(
                        "Failed to download files for model %s from GCS bucket",
                        self.model_id,
                    )
        except TimeoutError:
            # If the directory is already locked, then wait but do not do anything.
            with FileLock(lock_path, timeout=-1):
                pass
        return get_model_location_on_disk(self.model_id)

    def get_extra_files(self) -> List[str]:
        paths = []
        extra_files = self.mirror_config.extra_files or []
        if not extra_files:
            return paths

        lock_path = self._get_lock_path(suffix="-extra_files")

        try:
            # Timeout 0 means there will be only one attempt to acquire
            # the file lock. If it cannot be aquired, a TimeoutError
            # will be thrown.
            # This ensures that subsequent processes don't duplicate work.
            with FileLock(lock_path, timeout=0):
                for extra_file in extra_files:
                    path = Path(
                        os.path.expandvars(extra_file.destination_path)
                    ).expanduser()
                    paths.append(path)
                    download_files_from_gcs(
                        path=path,
                        bucket_uri=extra_file.bucket_uri,
                    )
        except TimeoutError:
            # If the directory is already locked, then wait but do not do anything.
            with FileLock(lock_path, timeout=-1):
                pass
        return paths
