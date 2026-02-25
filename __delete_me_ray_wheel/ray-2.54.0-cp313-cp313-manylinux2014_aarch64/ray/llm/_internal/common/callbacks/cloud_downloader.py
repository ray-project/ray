import logging
import time
from typing import Any, List, Tuple

from pydantic import BaseModel, field_validator

from .base import CallbackBase

logger = logging.getLogger(__name__)


class CloudDownloaderConfig(BaseModel):
    """Model for validating CloudDownloader configuration."""

    paths: List[Tuple[str, str]]

    @field_validator("paths")
    @classmethod
    def validate_paths(cls, v: List[Tuple[str, str]]) -> List[Tuple[str, str]]:
        # Supported cloud storage URI schemes
        valid_schemes = ("s3://", "gs://", "abfss://", "azure://")

        for i, (cloud_uri, _) in enumerate(v):
            if not any(cloud_uri.startswith(scheme) for scheme in valid_schemes):
                raise ValueError(
                    f"paths[{i}][0] (cloud_uri) must start with one of {valid_schemes}, "
                    f"got '{cloud_uri}'"
                )
        return v


class CloudDownloader(CallbackBase):
    """Callback that downloads files from cloud storage before model files are downloaded.

    This callback expects self.kwargs to contain a 'paths' field which should be
    a list of tuples, where each tuple contains (cloud_uri, local_path) strings.

    Supported cloud storage URIs: s3://, gs://, abfss://, azure://

    Example:
        ```
        from ray.llm._internal.common.callbacks.cloud_downloader import CloudDownloader
        from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
        config = LLMConfig(
            ...
            callback_config={
                "callback_class": CloudDownloader,
                "callback_kwargs": {
                    "paths": [
                        ("s3://bucket/path/to/file.txt", "/local/path/to/file.txt"),
                        ("gs://bucket/path/to/file.txt", "/local/path/to/file.txt"),
                    ]
                }
            }
            ...
        )
        ```
    """

    def __init__(self, **kwargs: Any) -> None:
        """Initialize the CloudDownloader callback.

        Args:
            **kwargs: Keyword arguments passed to the callback as a dictionary.
                Must contain a 'paths' field with a list of (cloud_uri, local_path) tuples.
        """
        super().__init__(**kwargs)

        # Validate configuration using Pydantic
        if "paths" not in self.kwargs:
            raise ValueError("CloudDownloader requires 'paths' field in kwargs")

        CloudDownloaderConfig.model_validate(self.kwargs)

    def on_before_download_model_files_distributed(self) -> None:
        """Download files from cloud storage to local paths before model files are downloaded."""
        from ray.llm._internal.common.utils.cloud_utils import CloudFileSystem

        paths = self.kwargs["paths"]
        start_time = time.monotonic()
        for cloud_uri, local_path in paths:
            CloudFileSystem.download_files(path=local_path, bucket_uri=cloud_uri)
        end_time = time.monotonic()
        logger.info(
            f"CloudDownloader: Files downloaded in {end_time - start_time} seconds"
        )
