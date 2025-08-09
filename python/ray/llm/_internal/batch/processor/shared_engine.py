"""Shared engine registry for managing shared LLM engine deployments and their configurations."""

import logging
from typing import Dict, Optional

from pydantic import BaseModel

from ray.llm._internal.batch.processor.base import ProcessorConfig

logger = logging.getLogger(__name__)


class SharedEngineMetadata(BaseModel):
    """
    Tracks a shared LLM engine configuration and its associated Ray Serve deployment.

    When multiple processors share the same LLM engine configuration, they can reuse
    the same LLM engine instance through a Ray Serve deployment. This class maps each
    shared engine configuration to its corresponding deployment name.

    Note: Only the LLM engine is shared, not other processor stages like tokenization,
    chat template, etc.
    """

    processor_config: ProcessorConfig
    deployment_name: Optional[str] = None


class _SharedEngineRegistry:
    """Registry for tracking shared LLM engine configurations and their deployments."""

    def __init__(self):
        self._shared_configs: Dict[int, SharedEngineMetadata] = {}

    def register_config(self, config: ProcessorConfig) -> None:
        """Register a processor configuration for shared LLM engine usage."""
        config_id = id(config)
        if config_id not in self._shared_configs:
            self._shared_configs[config_id] = SharedEngineMetadata(
                processor_config=config
            )

    def get_shared_metadata(
        self, config: ProcessorConfig
    ) -> Optional[SharedEngineMetadata]:
        """Get the shared engine info for a processor configuration."""
        return self._shared_configs.get(id(config))

    def set_serve_deployment(self, config: ProcessorConfig, deployment: str) -> None:
        """Set the Ray Serve deployment name for a shared engine configuration."""
        config_id = id(config)
        if config_id in self._shared_configs:
            self._shared_configs[config_id].deployment_name = deployment


# Global shared engine registry
_shared_engine_registry = _SharedEngineRegistry()
