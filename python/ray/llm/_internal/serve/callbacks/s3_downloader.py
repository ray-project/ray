import logging

from .custom_initialization import Callback

logger = logging.getLogger(__name__)


class S3Downloader(Callback):
    """Callback that downloads files from S3 before model files are downloaded.

    This callback expects self.kwargs to contain a 'paths' field which should be
    a list of tuples, where each tuple contains (s3_uri, local_path) strings.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # Type checking for paths field
        if "paths" not in self.kwargs:
            raise ValueError("S3Downloader requires 'paths' field in kwargs")

        paths = self.kwargs["paths"]
        if not isinstance(paths, list):
            raise TypeError("'paths' must be a list")

        for i, path_tuple in enumerate(paths):
            if not isinstance(path_tuple, tuple):
                raise TypeError(f"paths[{i}] must be a tuple, got {type(path_tuple)}")

            if len(path_tuple) != 2:
                raise ValueError(
                    f"paths[{i}] must be a tuple of length 2, got length {len(path_tuple)}"
                )

            s3_uri, local_path = path_tuple
            if not isinstance(s3_uri, str):
                raise TypeError(
                    f"paths[{i}][0] (s3_uri) must be a string, got {type(s3_uri)}"
                )
            if not isinstance(local_path, str):
                raise TypeError(
                    f"paths[{i}][1] (local_path) must be a string, got {type(local_path)}"
                )

            if not s3_uri.startswith("s3://"):
                raise ValueError(
                    f"paths[{i}][0] (s3_uri) must start with 's3://', got '{s3_uri}'"
                )

    def on_before_download_model_files_distributed(self) -> None:
        """Download files from S3 to local paths before model files are downloaded."""
        from ray.llm._internal.common.utils.cloud_utils import CloudFileSystem

        paths = self.kwargs["paths"]
        logger.info(f"S3Downloader: Starting download of {len(paths)} files from S3")

        for s3_uri, local_path in paths:
            try:
                logger.info(f"S3Downloader: Downloading {s3_uri} to {local_path}")
                CloudFileSystem.download_files(path=local_path, bucket_uri=s3_uri)
                logger.info(
                    f"S3Downloader: Successfully downloaded {s3_uri} to {local_path}"
                )
            except Exception as e:
                logger.error(
                    f"S3Downloader: Failed to download {s3_uri} to {local_path}: {e}"
                )
                if self.raise_error_on_callback:
                    raise
