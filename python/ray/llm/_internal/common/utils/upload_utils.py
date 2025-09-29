from pathlib import Path

import typer
from filelock import FileLock
from typing_extensions import Annotated

from ray.llm._internal.common.observability.logging import get_logger
from ray.llm._internal.common.utils.cloud_utils import (
    CloudFileSystem,
    CloudMirrorConfig,
    CloudModelAccessor,
    is_remote_path,
)
from ray.llm._internal.common.utils.download_utils import (
    get_model_entrypoint,
)

logger = get_logger(__name__)


class CloudModelUploader(CloudModelAccessor):
    """Unified uploader to upload models to cloud storage (S3 or GCS).

    Args:
        model_id: The model id to upload.
        mirror_config: The mirror config for the model.
    """

    def upload_model(self) -> str:
        """Upload the model to cloud storage (s3 or gcs).

        Returns:
            The remote path of the uploaded model.
        """
        bucket_uri = self.mirror_config.bucket_uri

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
                    CloudFileSystem.upload_model(
                        local_path=path,
                        bucket_uri=bucket_uri,
                    )
                    logger.info(
                        "Finished uploading %s to %s storage",
                        self.model_id,
                        storage_type.upper() if storage_type else "cloud",
                    )
                except RuntimeError:
                    logger.exception(
                        "Failed to upload model %s to %s storage",
                        self.model_id,
                        storage_type.upper() if storage_type else "cloud",
                    )
        except TimeoutError:
            # If the directory is already locked, then wait but do not do anything.
            with FileLock(lock_path, timeout=-1):
                pass
        return bucket_uri


def upload_model_files(model_id: str, bucket_uri: str) -> str:
    """Upload the model files to cloud storage (s3 or gcs).

    If `model_id` is a local path, the files will be uploaded to the cloud storage.
    If `model_id` is a huggingface model id, the model will be downloaded from huggingface
    and then uploaded to the cloud storage.

    Args:
        model_id: The huggingface model id, or local model path to upload.
        bucket_uri: The bucket uri to upload the model to, must start with `s3://` or `gs://`.

    Returns:
        The remote path of the uploaded model.
    """
    assert not is_remote_path(
        model_id
    ), f"model_id must NOT be a remote path: {model_id}"
    assert is_remote_path(bucket_uri), f"bucket_uri must be a remote path: {bucket_uri}"

    if not Path(model_id).exists():
        maybe_downloaded_model_path = get_model_entrypoint(model_id)
        if not Path(maybe_downloaded_model_path).exists():
            logger.info(
                "Assuming %s is huggingface model id, and downloading it.", model_id
            )
            import huggingface_hub

            huggingface_hub.snapshot_download(repo_id=model_id)
            # Try to get the model path again after downloading.
            maybe_downloaded_model_path = get_model_entrypoint(model_id)
            assert Path(
                maybe_downloaded_model_path
            ).exists(), f"Failed to download the model {model_id} to {maybe_downloaded_model_path}"
            return upload_model_files(maybe_downloaded_model_path, bucket_uri)
        else:
            return upload_model_files(maybe_downloaded_model_path, bucket_uri)

    uploader = CloudModelUploader(model_id, CloudMirrorConfig(bucket_uri=bucket_uri))
    return uploader.upload_model()


def upload_model_cli(
    model_source: Annotated[
        str,
        typer.Option(
            help="HuggingFace model ID to download, or local model path to upload",
        ),
    ],
    bucket_uri: Annotated[
        str,
        typer.Option(
            help="The bucket uri to upload the model to, must start with `s3://` or `gs://`",
        ),
    ],
):
    """Upload the model files to cloud storage (s3 or gcs)."""
    upload_model_files(model_source, bucket_uri)
