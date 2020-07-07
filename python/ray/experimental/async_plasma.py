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

    def _complete_future(self, ray_object_id):
        # TODO(ilr): Consider race condition between popping from the
        # waiting_dict and as_future appending to the waiting_dict's list.
        logger.debug(
            "Completing plasma futures for object id {}".format(ray_object_id))
        if ray_object_id not in self._waiting_dict:
            return
        obj = self._worker.get_objects([ray_object_id], timeout=0)[0]
        futures = self._waiting_dict.pop(ray_object_id)
        for fut in futures:
            try:
                fut.set_result(obj)
            except asyncio.InvalidStateError:
                # Avoid issues where the future got set immediately but we also
                # received a notification about the object from plasma.
                logger.debug("Failed to set result for future {}."
                             "Most likely already set.".format(fut))

    def close(self):
        """Clean up this handler."""
        for futures in self._waiting_dict.values():
            for fut in futures:
                fut.cancel()

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

        # Check if the object is ready.
        ready, _ = ray.wait([object_id], timeout=0)
        if ready:
            # The object is ready. Set the result immediately.
            self._complete_future(object_id)
        elif len(self._waiting_dict[object_id]) == 1:
            # The object is not yet ready. Only subscribe to this object if we
            # haven't already.
            self._worker.core_worker.subscribe_to_plasma_object(object_id)
        return future
