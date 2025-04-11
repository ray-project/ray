from pathlib import Path

from filelock import FileLock

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
    ), "model_id must NOT be a remote path: {}".format(model_id)
    assert is_remote_path(bucket_uri), "bucket_uri must be a remote path: {}".format(
        bucket_uri
    )

    if not Path(model_id).exists():
        maybe_downloaded_model_path = get_model_entrypoint(model_id)
        if not Path(maybe_downloaded_model_path).exists():
            logger.info(
                "Assuming %s is huggingface model id, and downloading it.", model_id
            )
            try:
                # It's non-trivial to use transformer library to load the model, as it requires
                # knowing the model architecture ahead to use the proper model class. It's
                # also concerning to bring in a new dependency for this, so we try and provide
                # workarounds.
                from curated_transformers.models import FromHF as model_FromHF
                from curated_transformers.tokenizers import FromHF as tokenizer_FromHF

                model_FromHF.from_hf_hub_to_cache(name=model_id)
                tokenizer_FromHF.from_hf_hub_to_cache(name=model_id)
            except ImportError:
                raise ImportError(
                    "curated_transformers is missing, which can be helpful for downloading "
                    "models + tokenizer from huggingface. Try to: \n\t1. install it with "
                    "`pip install curated-transformers`, or \n\t2. use transformer library "
                    "to load the model + tokenizer such that the assets will be downloaded, "
                    "or \n\t3. mannually download the model + tokenizer and then provide the "
                    "local path to `model_id`."
                )
            maybe_downloaded_model_path = get_model_entrypoint(model_id)
            assert Path(
                maybe_downloaded_model_path
            ).exists(), "Failed to download the model {} to {}".format(
                model_id, maybe_downloaded_model_path
            )
            return upload_model_files(maybe_downloaded_model_path, bucket_uri)
        else:
            return upload_model_files(maybe_downloaded_model_path, bucket_uri)

    uploader = CloudModelUploader(model_id, CloudMirrorConfig(bucket_uri=bucket_uri))
    return uploader.upload_model()
