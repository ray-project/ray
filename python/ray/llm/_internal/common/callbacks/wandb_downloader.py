import os
import time
from logging import getLogger
from typing import Any, List, Optional, Tuple

import wandb
from pydantic import BaseModel, ConfigDict, field_validator

from ray.llm._internal.common.callbacks.base import CallbackBase

logger = getLogger(__name__)


class WandBArtifactHandler:
    """Handler for WandB artifacts.

    This class provides a simple interface for downloading artifacts from WandB.
    It uses the WandB API to download artifacts and supports both public and private artifacts.

    """

    def __init__(
        self,
        wandb_base_url: Optional[str] = None,
        wandb_api_key: Optional[str] = None,
        artifact_root: Optional[str] = None,
    ):
        self.wandb_base_url = wandb_base_url
        self.wandb_api_key = wandb_api_key
        self.artifact_root = artifact_root
        self.settings = wandb.Settings(
            base_url=self.wandb_base_url, api_key=self.wandb_api_key
        )

    def download_artifact(
        self, path: Optional[str], artifact_uri: str, artifact_type: str = "model"
    ) -> str:
        """Download an artifact from WandB."""

        with wandb.init(settings=self.settings) as run:
            artifact_path = artifact_uri.replace("wandb://", "")
            downloaded_path = run.use_artifact(
                artifact_path, type=artifact_type
            ).download(root=self.artifact_root)

        if path is not None:
            target_is_directory = os.path.isdir(downloaded_path)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            os.symlink(
                downloaded_path,
                path,
                target_is_directory=target_is_directory,
            )


class WandBDownloaderConfig(BaseModel):
    """Model for validating WandBDownloader configuration."""

    model_config = ConfigDict(
        protected_namespaces=tuple(),
        extra="ignore",
    )

    paths: List[Tuple[str, str]]
    wandb_base_url: Optional[str] = None
    wandb_api_key: Optional[str] = None

    @field_validator("paths")
    @classmethod
    def validate_paths(cls, v: List[Tuple[str, str]]) -> List[Tuple[str, str]]:
        """Validate the paths."""

        # Supported WandB URI schemes
        valid_schemes = ("wandb://",)

        for i, (wandb_uri, _) in enumerate(v):
            if not any(wandb_uri.startswith(scheme) for scheme in valid_schemes):
                raise ValueError(
                    f"paths[{i}][0] (wandb_uri) must start with one of {valid_schemes}, "
                    f"got '{wandb_uri}'"
                )
        return v


class WandBDownloader(CallbackBase):
    """Callback that downloads files from WandB before model files are downloaded.

    This callback expects self.kwargs to contain a 'paths' field which should be
    a list of tuples, where each tuple contains (wandb_uri, local_path) strings.

    Supported WandB URI schemes: wandb://

    Example:
        ```
        from ray.llm._internal.common.callbacks.wandb_downloader import WandBDownloader
        from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
        config = LLMConfig(
            ...
            callback_config={
                "callback_class": WandBDownloader,
                "callback_kwargs": {
                    "paths": [
                        ("wandb://user/model/123", "/local/path/to/model"),
                    ],
                    "wandb_base_url": "https://api.wandb.ai", # Your WandB base URL
                    "wandb_api_key": "sk-1234567890", # Your WandB API key
                }
            }
            ...
        )
        ```
    """

    def __init__(self, **kwargs: Any) -> None:
        """Initialize the WandBDownloader callback.

        Args:
            **kwargs: Keyword arguments passed to the callback as a dictionary.
                Must contain a 'paths' field with a list of (wandb_uri, local_path) tuples.
        """
        super().__init__(**kwargs)

        self.callback_config = WandBDownloaderConfig(**kwargs)

        WandBDownloaderConfig.model_validate(self.kwargs)

    async def on_before_node_init(self) -> None:
        """Download files from WandB before model files are downloaded."""

        paths = self.kwargs["paths"]
        wandb_base_url = self.kwargs["wandb_base_url"]
        wandb_api_key = self.kwargs["wandb_api_key"]

        handler = WandBArtifactHandler(
            wandb_base_url=wandb_base_url,
            wandb_api_key=wandb_api_key,
        )

        start_time = time.monotonic()
        for wandb_uri, local_path in paths:
            handler.download_artifact(path=local_path, artifact_uri=wandb_uri)

        end_time = time.monotonic()
        logger.info(
            f"WandBDownloader: Artifacts downloaded in {end_time - start_time} seconds"
        )
