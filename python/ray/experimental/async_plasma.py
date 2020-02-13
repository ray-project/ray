import asyncio
import ctypes
import sys

import ray
from ray.services import logger
from collections import defaultdict
from typing import List

INT64_SIZE = ctypes.sizeof(ctypes.c_int64)


class PlasmaObjectFuture(asyncio.Future):
    """This class is a wrapper for a Future on Plasma."""
    pass


class PlasmaEventHandler:
    """This class is an event handler for Plasma."""

    def __init__(self, loop):
        super().__init__()
        self._loop = loop
        self._waiting_dict = defaultdict(list)

    def process_notifications(self, messages):
        """Process notifications."""
        for object_id, object_size, metadata_size in messages:
            if object_size > 0 and object_id in self._waiting_dict:
                futures = self._waiting_dict[object_id]
                self._complete_future(futures, object_id)

    def close(self):
        """Clean up this handler."""
        for futures in self._waiting_dict.values():
            for fut in futures:
                fut.cancel()

    def _complete_future(self, futures: List[asyncio.Future], ray_object_id):
        obj = ray.get(ray_object_id)
        for fut in futures:
            try:
                fut.set_result(obj)
            except InvalidStateError as e:
                # Avoid issues where process_notifications and check_ready paths both get executed
                pass
        try:
            self._waiting_dict.pop(ray_object_id)
        except KeyError as e:
            # Same situation as above
            pass

    def as_future(self, object_id, check_ready=True):
        """Turn an object_id into a Future object.

        Args:
            object_id: A Ray's object_id.
            check_ready (bool): If true, check if the object_id is ready.

        Returns:
            PlasmaObjectFuture: A future object that waits the object_id.
        """
        if not isinstance(object_id, ray.ObjectID):
            raise TypeError("Input should be a Ray ObjectID.")
        fut = PlasmaObjectFuture(loop=self._loop)

        self._waiting_dict[object_id].append(fut)

        if check_ready:
            ready, _ = ray.wait([object_id], timeout=0)
            if ready:
                if self._loop.get_debug():
                    logger.debug("%s has been ready.", object_id)
                if not fut.done():
                    self._complete_future([fut], object_id)
                return fut
        if self._loop.get_debug():
            logger.debug("%s added to the waiting list.", fut)

        return fut
