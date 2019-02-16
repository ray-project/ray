from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


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
