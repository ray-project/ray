from abc import ABC, abstractmethod
from typing import Any

from ray.llm._internal.serve.configs.server_models import LLMConfig
from ray.llm._internal.serve.deployments.utils.node_initialization_utils import (
    InitializeNodeOutput,
)


class CustomInitialization(ABC):
    """Abstract base class for custom initialization implementations.

    This class provides a framework for implementing custom initialization logic
    for LLMEngine. Subclasses must implement the initialize method to define
    their specific initialization behavior.
    """

    def __init__(self, **kwargs: Any) -> None:
        """Initialize the custom initialization class with keyword arguments.

        Args:
            **kwargs: Arbitrary keyword arguments that can be used by subclasses
                     for configuration and setup.
        """
        self.kwargs = kwargs

    @abstractmethod
    async def initialize(self, llm_config: LLMConfig) -> InitializeNodeOutput:
        """Initialize the node with custom logic.

        This method must be implemented by all subclasses to define their
        specific initialization behavior.

        Args:
            llm_config: The LLM configuration object containing all necessary
                       settings for model initialization.

        Returns:
            InitializeNodeOutput: An object containing the placement group,
                                 runtime environment, and any extra initialization
                                 keyword arguments.
        """
        pass
