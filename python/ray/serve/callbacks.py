import asyncio
import logging
from typing import List

from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.serve.config import CallbackConfig

logger = logging.getLogger(SERVE_LOGGER_NAME)


class CallbackManager:
    """Manages callbacks for deployment lifecycle events."""

    def __init__(self, callbacks: List[CallbackConfig]):
        self.callbacks = callbacks

    async def trigger_request_start(self, request_metadata):
        """Trigger on_request_start callbacks."""
        for callback_config in self.callbacks:
            if callback_config.on_request_start:
                try:
                    await self._execute_callback(
                        callback_config.on_request_start, request_metadata
                    )
                except Exception as e:
                    # Log error but don't fail the request
                    logger.error(f"Callback error in on_request_start: {e}")

    async def trigger_request_end(self, request_metadata, response):
        """Trigger on_request_end callbacks."""
        for callback_config in self.callbacks:
            if callback_config.on_request_end:
                try:
                    await self._execute_callback(
                        callback_config.on_request_end, request_metadata, response
                    )
                except Exception as e:
                    logger.error(f"Callback error in on_request_end: {e}")

    async def _execute_callback(self, callback, *args):
        """Execute callback safely, handling both sync and async functions."""
        if asyncio.iscoroutinefunction(callback):
            await callback(*args)
        else:
            callback(*args)
