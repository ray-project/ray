from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray.worker


class RuntimeContext(object):
    """A class used for getting runtime context."""

    def __init__(self, worker=None):
        self.worker = worker

    @property
    def current_driver_id(self):
        """Get current driver ID for this worker or driver.

        Returns:
            If called by a driver, this returns the driver ID. If called in
                a task, return the driver ID of the associated driver.
        """
        assert self.worker is not None
        return self.worker.task_driver_id


_runtime_context = None


def _get_runtime_context():
    global _runtime_context
    if _runtime_context is None:
        _runtime_context = RuntimeContext(ray.worker.get_global_worker())

    return _runtime_context
