"""Cache manager server mixin.

Server mixin that delegates cache management operations to the underlying engine.
"""

from ray.llm._internal.serve.observability.logging import get_logger

logger = get_logger(__name__)


class CacheManagerServerMixin:
    """Server mixin that delegates cache management to engine.

    Checks engine capability before delegating. This allows LLMServer
    to compose all mixins regardless of which engine is used.
    """

    async def reset_prefix_cache(self) -> None:
        """Reset the KV prefix cache on the engine.

        Clears cached key-value pairs from previous requests.
        """
        if self.engine is None:
            return
        if not hasattr(self.engine, "reset_prefix_cache"):
            logger.warning("Engine does not support reset_prefix_cache")
            return
        try:
            await self.engine.reset_prefix_cache()
        except Exception as e:
            logger.error(
                "Engine reset_prefix_cache failed in LLMServer.reset_prefix_cache: %s",
                e,
            )
            raise e
