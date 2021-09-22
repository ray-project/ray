import asyncio
import logging
from typing import Callable

from starlette.requests import Request
from uvicorn.config import Config
from uvicorn.lifespan.on import LifespanOn

from ray.serve.http_util import ASGIHTTPSender
from ray.serve.utils import logger, LoggingContext


class ASGIWrapper:
    def __init__(self, app: Callable):
        self._app = app

        # Use uvicorn's lifespan handling code to properly deal with
        # startup and shutdown event.
        self._serve_asgi_lifespan = LifespanOn(
            Config(self._app, lifespan="on"))

        # Replace uvicorn logger with our own.
        self._serve_asgi_lifespan.logger = logger

    async def _async_setup(self):
        """Runs FastAPI startup hooks, will be run at replica startup."""
        # LifespanOn's logger logs in INFO level thus becomes spammy
        # Within this block we temporarily uplevel for cleaner logging
        with LoggingContext(
                self._serve_asgi_lifespan.logger, level=logging.WARNING):
            await self._serve_asgi_lifespan.startup()

    async def __call__(self, request: Request):
        sender = ASGIHTTPSender()
        await self._app(
            request.scope,
            request._receive,
            sender,
        )
        return sender.build_starlette_response()

    def __del__(self):
        # LifespanOn's logger logs in INFO level thus becomes spammy
        # Within this block we temporarily uplevel for cleaner logging
        with LoggingContext(
                self._serve_asgi_lifespan.logger, level=logging.WARNING):
            asyncio.get_event_loop().run_until_complete(
                self._serve_asgi_lifespan.shutdown())
