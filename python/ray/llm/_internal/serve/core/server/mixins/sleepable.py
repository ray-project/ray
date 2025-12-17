"""Sleepable server mixin.

Server mixin that delegates sleep/wakeup operations to the underlying engine.
"""

from typing import Any

from ray.llm._internal.serve.observability.logging import get_logger

logger = get_logger(__name__)


class SleepableServerMixin:
    """Server mixin that delegates sleep/wakeup to engine.

    Checks engine capability before delegating. This allows LLMServer
    to compose all mixins regardless of which engine is used.
    """

    async def sleep(self, **kwargs: Any) -> None:
        """Put the engine to sleep.

        Args:
            **kwargs: Engine-specific sleep options. Passed through to the engine.
        """
        if self.engine is None:
            return
        if not hasattr(self.engine, "sleep"):
            logger.warning("Engine does not support sleep")
            return
        try:
            await self.engine.sleep(**kwargs)
        except Exception as e:
            logger.error("Engine sleep failed in LLMServer.sleep: %s", e)
            raise e

    async def wakeup(self, **kwargs: Any) -> None:
        """Wake up the engine from sleep mode.

        Args:
            **kwargs: Engine-specific wakeup options. Passed through to the engine.
        """
        if self.engine is None:
            return
        if not hasattr(self.engine, "wakeup"):
            logger.warning("Engine does not support wakeup")
            return
        try:
            await self.engine.wakeup(**kwargs)
        except Exception as e:
            logger.error("Engine wakeup failed in LLMServer.wakeup: %s", e)
            raise e

    async def is_sleeping(self) -> bool:
        """Check whether the engine is currently sleeping.

        Returns:
            True if the engine is sleeping, False otherwise.
        """
        logger.info("[DEBUG] Checking if engine is sleeping from server")
        if self.engine is None:
            return False
        if not hasattr(self.engine, "is_sleeping"):
            return False
        try:
            result = await self.engine.is_sleeping()
            logger.info("[DEBUG] Engine is sleeping from server: %s", result)
            return result
        except Exception as e:
            logger.error("Engine is_sleeping failed in LLMServer.is_sleeping: %s", e)
            raise e
