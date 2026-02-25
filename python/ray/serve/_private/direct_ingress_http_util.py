import asyncio
import logging

from starlette.types import Message, Receive, Scope

from ray.serve._private.constants import (
    SERVE_LOGGER_NAME,
)

logger = logging.getLogger(SERVE_LOGGER_NAME)


class ASGIDIReceiveProxy:
    """Proxies ASGI receive from an actor.

    The `receive_asgi_messages` callback will be called repeatedly to fetch messages
    until a disconnect message is received.
    """

    def __init__(
        self,
        scope: Scope,
        receive: Receive,
        user_event_loop: asyncio.AbstractEventLoop,
    ):
        self._type = scope["type"]  # Either 'http' or 'websocket'.
        # Lazy init the queue to ensure it is created in the user code event loop.
        self._queue = None
        self._receive = receive
        self._user_event_loop = user_event_loop
        self._disconnect_message = None

    def _get_default_disconnect_message(self) -> Message:
        """Return the appropriate disconnect message based on the connection type.

        HTTP ASGI spec:
            https://asgi.readthedocs.io/en/latest/specs/www.html#disconnect-receive-event

        WS ASGI spec:
            https://asgi.readthedocs.io/en/latest/specs/www.html#disconnect-receive-event-ws
        """
        if self._type == "websocket":
            return {
                "type": "websocket.disconnect",
                # 1005 is the default disconnect code according to the ASGI spec.
                "code": 1005,
            }
        else:
            return {"type": "http.disconnect"}

    @property
    def queue(self) -> asyncio.Queue:
        if self._queue is None:
            self._queue = asyncio.Queue()

        return self._queue

    def put_message(self, msg: Message):
        self.queue.put_nowait(msg)

    def close_queue(self):
        self.queue.close()

    def fetch_until_disconnect_task(self) -> asyncio.Task:
        return asyncio.create_task(self._fetch_until_disconnect())

    async def _fetch_until_disconnect(self):
        """Fetch messages repeatedly until a disconnect message is received.

        If a disconnect message is received, this function exits and returns it.

        If an exception occurs, it will be raised on the next __call__ and no more
        messages will be received.

        Note that this is meant to be called in the system event loop.
        """
        while True:
            msg = await self._receive()
            if asyncio.get_running_loop() == self._user_event_loop:
                await self.queue.put(msg)
            else:
                self._user_event_loop.call_soon_threadsafe(self.put_message, msg)

            if msg["type"] == "http.disconnect":
                self._disconnect_message = msg
                return None

            if msg["type"] == "websocket.disconnect":
                self._disconnect_message = msg
                return msg["code"]

    async def __call__(self) -> Message:
        """Return the next message once available.

        This will repeatedly return a disconnect message once it's been received.
        """
        if self.queue.empty() and self._disconnect_message is not None:
            return self._disconnect_message

        message = await self.queue.get()
        if isinstance(message, Exception):
            raise message

        return message
