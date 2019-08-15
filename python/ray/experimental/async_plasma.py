import asyncio
import ctypes
import sys

import pyarrow.plasma as plasma

import ray
from ray.services import logger

INT64_SIZE = ctypes.sizeof(ctypes.c_int64)


def _release_waiter(waiter, *_):
    if not waiter.done():
        waiter.set_result(None)


class PlasmaProtocol(asyncio.Protocol):
    """Protocol control for the asyncio connection."""

    def __init__(self, plasma_client, plasma_event_handler):
        self.plasma_client = plasma_client
        self.plasma_event_handler = plasma_event_handler
        self.transport = None
        self._buffer = b""

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        self._buffer += data
        messages = []
        i = 0
        while i + INT64_SIZE <= len(self._buffer):
            msg_len = int.from_bytes(self._buffer[i:i + INT64_SIZE],
                                     sys.byteorder)
            if i + INT64_SIZE + msg_len > len(self._buffer):
                break
            i += INT64_SIZE
            segment = self._buffer[i:i + msg_len]
            i += msg_len
            messages.append(self.plasma_client.decode_notification(segment))

        self._buffer = self._buffer[i:]
        self.plasma_event_handler.process_notifications(messages)

    def connection_lost(self, exc):
        # The socket has been closed
        logger.debug("PlasmaProtocol - connection lost.")

    def eof_received(self):
        logger.debug("PlasmaProtocol - EOF received.")
        self.transport.close()


class PlasmaObjectFuture(asyncio.Future):
    """This class manages the lifecycle of a Future contains an object_id.

    Note:
        This Future is an item in an linked list.

    Attributes:
        object_id: The object_id this Future contains.
    """

    def __init__(self, loop, object_id):
        super().__init__(loop=loop)
        self.object_id = object_id
        self.prev = None
        self.next = None

    @property
    def ray_object_id(self):
        return ray.ObjectID(self.object_id.binary())

    def __repr__(self):
        return super().__repr__() + "{object_id=%s}" % self.object_id


class PlasmaObjectLinkedList(asyncio.Future):
    """This class is a doubly-linked list.
    It holds a ObjectID and maintains futures assigned to the ObjectID.

    Args:
        loop: an event loop.
        plain_object_id (plasma.ObjectID):
            The plasma ObjectID this class holds.
    """

    def __init__(self, loop, plain_object_id):
        super().__init__(loop=loop)
        assert isinstance(plain_object_id, plasma.ObjectID)
        self.object_id = plain_object_id
        self.head = None
        self.tail = None

    def append(self, future):
        """Append an object to the linked list.

        Args:
            future (PlasmaObjectFuture): A PlasmaObjectFuture instance.
        """
        future.prev = self.tail
        if self.tail is None:
            assert self.head is None
            self.head = future
        else:
            self.tail.next = future
        self.tail = future
        # Once done, it will be removed from the list.
        future.add_done_callback(self.remove)

    def remove(self, future):
        """Remove an object from the linked list.

        Args:
            future (PlasmaObjectFuture): A PlasmaObjectFuture instance.
        """
        if self._loop.get_debug():
            logger.debug("Removing %s from the linked list.", future)
        if future.prev is None:
            assert future is self.head
            self.head = future.next
            if self.head is None:
                self.tail = None
                if not self.cancelled():
                    self.set_result(None)
            else:
                self.head.prev = None
        elif future.next is None:
            assert future is self.tail
            self.tail = future.prev
            if self.tail is None:
                self.head = None
                if not self.cancelled():
                    self.set_result(None)
            else:
                self.tail.prev = None

    def cancel(self, *args, **kwargs):
        """Manually cancel all tasks assigned to this event loop."""
        # Because remove all futures will trigger `set_result`,
        # we cancel itself first.
        super().cancel()
        for future in self.traverse():
            # All cancelled futures should have callbacks to removed itself
            # from this linked list. However, these callbacks are scheduled in
            # an event loop, so we could still find them in our list.
            if not future.cancelled():
                future.cancel()

    def set_result(self, result):
        """Complete all tasks. """
        for future in self.traverse():
            # All cancelled futures should have callbacks to removed itself
            # from this linked list. However, these callbacks are scheduled in
            # an event loop, so we could still find them in our list.
            future.set_result(result)
        if not self.done():
            super().set_result(result)

    def traverse(self):
        """Traverse this linked list.

        Yields:
            PlasmaObjectFuture: PlasmaObjectFuture instances.
        """
        current = self.head
        while current is not None:
            yield current
            current = current.next


class PlasmaEventHandler:
    """This class is an event handler for Plasma."""

    def __init__(self, loop, worker):
        super().__init__()
        self._loop = loop
        self._worker = worker
        self._waiting_dict = {}

    def process_notifications(self, messages):
        """Process notifications."""
        for object_id, object_size, metadata_size in messages:
            if object_size > 0 and object_id in self._waiting_dict:
                linked_list = self._waiting_dict[object_id]
                self._complete_future(linked_list)

    def close(self):
        """Clean up this handler."""
        for linked_list in self._waiting_dict.values():
            linked_list.cancel()
        # All cancelled linked lists should have callbacks to removed itself
        # from the waiting dict. However, these callbacks are scheduled in
        # an event loop, so we don't check them now.

    def _unregister_callback(self, fut):
        del self._waiting_dict[fut.object_id]

    def _complete_future(self, fut):
        obj = self._worker.retrieve_and_deserialize([fut.object_id], 0)[0]
        fut.set_result(obj)

    def as_future(self, object_id, check_ready=True):
        """Turn an object_id into a Future object.

        Args:
            object_id: A Ray's object_id.
            check_ready (bool): If true, check if the object_id is ready.

        Returns:
            PlasmaObjectFuture: A future object that waits the object_id.
        """
        if not isinstance(object_id, ray.ObjectID):
            raise TypeError("Input should be an ObjectID.")

        plain_object_id = plasma.ObjectID(object_id.binary())
        fut = PlasmaObjectFuture(loop=self._loop, object_id=plain_object_id)

        if check_ready:
            ready, _ = ray.wait([object_id], timeout=0)
            if ready:
                if self._loop.get_debug():
                    logger.debug("%s has been ready.", plain_object_id)
                self._complete_future(fut)
                return fut

        if plain_object_id not in self._waiting_dict:
            linked_list = PlasmaObjectLinkedList(self._loop, plain_object_id)
            linked_list.add_done_callback(self._unregister_callback)
            self._waiting_dict[plain_object_id] = linked_list
        self._waiting_dict[plain_object_id].append(fut)
        if self._loop.get_debug():
            logger.debug("%s added to the waiting list.", fut)

        return fut
