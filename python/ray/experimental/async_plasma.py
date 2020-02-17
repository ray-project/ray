import asyncio

import ray
from ray.services import logger
from collections import defaultdict


class PlasmaObjectFuture(asyncio.Future):
    """This class is a wrapper for a Future on Plasma."""
    pass


class PlasmaEventHandler:
    """This class is an event handler for Plasma."""

    def __init__(self, loop, worker):
        super().__init__()
        self._loop = loop
        self._worker = worker
        self._waiting_dict = defaultdict(list)

    def process_notifications(self, messages):
        """Process notifications."""
        for object_id, object_size, metadata_size in messages:
            if object_size > 0 and object_id in self._waiting_dict:
                self._complete_future(object_id)

    def close(self):
        """Clean up this handler."""
        for futures in self._waiting_dict.values():
            for fut in futures:
                fut.cancel()

    def _complete_future(self, ray_object_id):
        # TODO(ilr): Consider race condition between popping from the
        # waiting_dict and as_future appending to the waiting_dict's list.
        logger.debug(
            "Completing plasma futures for object id {}".format(ray_object_id))

        obj = self._worker.get_objects([ray_object_id])[0]
        futures = self._waiting_dict.pop(ray_object_id)
        for fut in futures:
            loop = fut._loop

            def complete_closure():
                try:
                    fut.set_result(obj)
                except asyncio.InvalidStateError:
                    # Avoid issues where process_notifications
                    # and check_ready both get executed
                    logger.debug("Failed to set result for future {}."
                                 "Most likely already set.".format(fut))

            loop.call_soon_threadsafe(complete_closure)

    def check_immediately(self, object_id):
        ready, _ = ray.wait([object_id], timeout=0)
        if ready:
            self._complete_future(object_id)

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

        future = PlasmaObjectFuture(loop=self._loop)
        self._waiting_dict[object_id].append(future)
        self.check_immediately(object_id)

        return future
