import asyncio
import ctypes
import socket
import struct

import pyarrow.plasma as plasma

import ray
from ray.services import logger

from ray.core.generated.ray.protocol.MessageType import MessageType
from ray.core.generated.ray.protocol.ObjectLocalEvent import ObjectLocalEvent

HEADER_SIZE = ctypes.sizeof(ctypes.c_int64) * 3


def _release_waiter(waiter, *_):
    if not waiter.done():
        waiter.set_result(None)


class RayletEventProtocol(asyncio.Protocol):
    """Protocol control for the asyncio connection."""

    def __init__(self, plasma_client, plasma_event_handler):
        self.plasma_client = plasma_client
        self.plasma_event_handler = plasma_event_handler
        self.transport = None
        self._buffer = bytearray()

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        # Header := version (int64_t), type (int64_t), size (int64_t)
        self._buffer.extend(data)
        messages = []
        i = 0
        while i + HEADER_SIZE <= len(self._buffer):
            version, msg_type, msg_len = struct.unpack(
                "=qqq", self._buffer[i:i + HEADER_SIZE])
            # TODO: check version
            if i + HEADER_SIZE + msg_len > len(self._buffer):
                break
            i += HEADER_SIZE
            message_body = self._buffer[i:i + msg_len]
            i += msg_len
            messages.append((msg_type, message_body))

        del self._buffer[:i]
        self.plasma_event_handler.process_notifications(messages)

    def connection_lost(self, exc):
        # The socket has been closed
        logger.debug("RayletEventProtocol - connection lost.")

    def eof_received(self):
        logger.debug("RayletEventProtocol - EOF received.")


class RayletEventTransport:
    """Event transport for raylet client.

    This event transport only borrows socket file descriptor. When it is
    closed, the socket file descriptor will be detached instead of being
    destroyed.
    """
    max_size = 1024 * 1024  # Buffer size passed to recv().

    # Attribute used in the destructor: it must be set even if the constructor
    # is not called (see _SelectorSslTransport which may start by raising an
    # exception)
    _sock = None

    def __init__(self, loop, socket_fd, protocol):
        self._loop = loop
        self._sock = socket.socket(
            fileno=socket_fd, family=socket.AF_UNIX, type=socket.SOCK_STREAM)
        self._sock_fd = socket_fd
        self._protocol = protocol
        self._closing = False  # Set when close() called.
        loop._transports[self._sock_fd] = self
        self._loop.call_soon(self._protocol.connection_made, self)
        # only start reading when connection_made() has been called
        self._loop.call_soon(self._loop._add_reader, self._sock_fd,
                             self._read_ready)

    def is_closing(self):
        return self._closing

    def close(self, exc=None):
        if self._closing:
            return
        self._closing = True
        self._loop._remove_reader(self._sock_fd)
        self._loop.call_soon(self._call_connection_lost, exc)

    # On Python 3.3 and older, objects with a destructor part of a reference
    # cycle are never destroyed. It"s not more the case on Python 3.4 thanks
    # to the PEP 442.
    if asyncio.compat.PY34:

        def __del__(self):
            if self._sock is not None:
                # NOTE: we only detach the socket handle because it is borrowed
                # from the C++ client.
                self._sock.detach()

    def _call_connection_lost(self, exc):
        try:
            self._protocol.connection_lost(exc)
        finally:
            # NOTE: we only detach the socket handle because it is borrowed
            # from the C++ client.
            self._sock.detach()
            self._sock = None
            self._protocol = None
            self._loop = None

    def _read_ready(self):
        if self._closing:
            return
        try:
            data = self._sock.recv(self.max_size)
        except (BlockingIOError, InterruptedError):
            pass
        except Exception as exc:
            self._loop.call_exception_handler({
                "message": "Fatal read error on socket transport",
                "exception": exc,
                "transport": self,
                "protocol": self._protocol,
            })
            self.close(exc)
        else:
            if data:
                self._protocol.data_received(data)
            else:
                self._protocol.eof_received()
                self.close()


class SubscriptionFuture(asyncio.Future):
    def __init__(self, event_handler):
        super().__init__(loop=event_handler.loop)
        self.event_handler = event_handler
        # We use C++ random ID because generating from numpy is extremely slow.
        self.subscription_id = ray.raylet.random_id()

    def cancel(self):
        super().cancel()
        self.event_handler.unsubscribe(self.subscription_id)


class ObjectLocalFuture(SubscriptionFuture):
    def __init__(self, event_handler, worker, wait_only):
        super().__init__(event_handler)
        self._worker = worker
        self._wait_only = wait_only

    def set_result(self, result):
        assert isinstance(result, ray.ObjectID)
        if self._wait_only:
            super().set_result(result)
        else:
            object_id = plasma.ObjectID(result.id())
            obj = self._worker.retrieve_and_deserialize([object_id], 0)[0]
            super().set_result(obj)


class RayletEventHandler:
    """This class is an event handler for Plasma."""

    def __init__(self, loop, worker):
        super().__init__()
        self.loop = loop
        self._worker = worker
        self._subscription_dict = {}

    def process_notifications(self, messages):
        """Process notifications."""
        for msg_type, msg_body in messages:
            if msg_type == MessageType.ObjectLocalEvent:
                body = ObjectLocalEvent.GetRootAsObjectLocalEvent(msg_body, 0)
                self._process_object_local(body)

    def _process_object_local(self, object_local_event):
        subscription_id = ray.ObjectID(object_local_event.SubscriptionId())
        if subscription_id in self._subscription_dict:
            fut = self._subscription_dict.pop(subscription_id)
            fut.set_result(ray.ObjectID(object_local_event.ObjectId()))

    def close(self):
        """Clean up this handler."""
        for subscription_id in list(self._subscription_dict.keys()):
            self.unsubscribe(subscription_id)

    def unsubscribe(self, subscription_id):
        if subscription_id in self._subscription_dict:
            fut = self._subscription_dict.pop(subscription_id)
            self._worker.raylet_client.unsubscribe(subscription_id)
            if not (fut.cancelled() or fut.done()):
                fut.cancel()

    def as_future(self, object_id, wait_only=False):
        """Turn an object_id into a Future object.

        Args:
            object_id (ray.ObjectID): A Ray's object_id.
            wait_only (bool): If true, the future will not fetch the object,
                it will return the original ObjectID instead.

        Returns:
            PlasmaObjectFuture: A future object that waits the object_id.
        """
        if not isinstance(object_id, ray.ObjectID):
            raise TypeError("Input should be an ObjectID.")

        fut = ObjectLocalFuture(self, self._worker, wait_only)
        self._worker.raylet_client.subscribe_object_local(
            fut.subscription_id, object_id)

        self._subscription_dict[fut.subscription_id] = fut

        return fut
