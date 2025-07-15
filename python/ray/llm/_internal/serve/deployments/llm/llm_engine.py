import abc
from typing import AsyncGenerator, Optional

from ray.llm._internal.serve.configs.server_models import (
    DiskMultiplexConfig,
    GenerationRequest,
    LLMConfig,
    LLMRawResponse,
    Prompt,
)


class LLMEngine(abc.ABC):
    """Base class for all LLM engines"""

    @abc.abstractmethod
    def __init__(self, llm_config: LLMConfig):
        """Initialize the engine with the llm config"""
        pass

    @abc.abstractmethod
    async def start(self):
        """Start the engine"""
        pass

    @abc.abstractmethod
    async def prepare_request(
        self,
        request_id: str,
        prompt: Prompt,
        stream: bool,
        disk_lora_model: Optional[DiskMultiplexConfig] = None,
        **kwargs,
    ) -> GenerationRequest:
        """Prepare a GenerationRequest for the engine"""
        pass

    @abc.abstractmethod
    async def generate(
        self, request: GenerationRequest
    ) -> AsyncGenerator[LLMRawResponse, None]:
        """Generate an LLMRawResponse stream based on the GenerationRequest"""
        pass

    async def check_health(self) -> None:
        """Check the health of the replica. Does not return anything. Raise error when
        the engine is dead and needs to be restarted.
        """
        return

    ##############################################################
    # Optional methods
    # These methods will be implemented in the future to allow
    # more granular life-cycle management of the engine.
    # e.g. in usecases like RL training, we need to put the engine
    # to sleep during training and wake up during rollouts.
    ##############################################################

    async def sleep(self):
        """Puts the engine to sleep"""
        pass

    async def wakeup(self):
        """Wakes up the engine"""
        pass

    def shutdown(self):
        """Shuts down the engine"""
        pass
