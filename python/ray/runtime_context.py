from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


class RuntimeContext(object):
    """A class used for getting runtime context.
    """
    def __init__(self, worker=None):
        self.worker = worker

    @property
    def current_driver_id(self):
        """Get current driver id for this worker or driver.
        Returns:
            Return the driver id in driver, and return the driver id of
            current executing task in worker.
        """
        assert self.worker is not None
        return self.worker.task_driver_id
