from ray.util.annotations import PublicAPI
from ray.llm._internal.common.utils.upload_utils import (
    upload_model_files as _upload_model_files,
)


@PublicAPI(stability="alpha")
def upload_model_files(model_id: str, bucket_uri: str) -> str:
    """Upload the model files to cloud storage (s3 or gcs).

    If `model_id` is a local path, the files will be uploaded to the cloud storage.
    If `model_id` is a huggingface model id, the model will be downloaded from huggingface
    and then uploaded to the cloud storage.

    Args:
        model_id: The huggingface model id, or local model path to upload.
        bucket_uri: The bucket uri to upload the model to, must start with `s3://` or `gs://`.

    Returns:
        The bucket uri of the uploaded model.

    Examples:

        .. testcode::
            :skipif: True

            from ray.llm.utils import upload_model_files
            # Download (if needed) and upload the model files to s3
            upload_model_files("facebook/opt-350m", "s3://my-bucket/path/to/facebook-opt-350m")
            # Or, specify exact local path to the model files to upload
            upload_model_files("~/.cache/huggingface/hub/models--facebook--opt-350m/", "s3://my-bucket/path/to/facebook-opt-350m")
    """
    return _upload_model_files(model_id, bucket_uri)
