import abc
from typing import AsyncGenerator, Any

from ray.llm._internal.serve.configs.server_models import (
    DiskMultiplexConfig,
    LLMConfig,
)


class LLMEngine(abc.ABC):
    """Base protocal class for all LLM engines"""

    @abc.abstractmethod
    def __init__(self, llm_config: LLMConfig):
        """Initialize the engine with the llm config"""
        pass

    @abc.abstractmethod
    async def start(self):
        """Start the engine"""
        pass
    
    @abc.abstractmethod
    async def resolve_lora(self, lora_model: DiskMultiplexConfig):
        """Resolve the lora model"""
        pass
    
    @abc.abstractmethod
    async def chat(self, request) ->  AsyncGenerator[Any, None]:
        """Chat with the engine"""
        pass
    
    @abc.abstractmethod
    async def completions(self, request) ->  AsyncGenerator[Any, None]:
        """Completion with the engine"""
        pass
    
    @abc.abstractmethod
    async def embeddings(self, request) ->  AsyncGenerator[Any, None]:
        """Embed with the engine"""
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
