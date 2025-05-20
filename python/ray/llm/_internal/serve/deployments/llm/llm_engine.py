from typing import AsyncGenerator, Optional

from ray.llm._internal.serve.configs.server_models import (
    Prompt,
    LLMRawResponse,
    LLMConfig,
    GenerationRequest,
    DiskMultiplexConfig,
)


import abc


class LLMEngine(abc.ABC):
    """Base class for all LLM engines"""

    def __init__(self, llm_config: LLMConfig):
        self._llm_config = llm_config

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

    async def check_health(self) -> bool:
        """Check the health of the engine"""
        return True

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
