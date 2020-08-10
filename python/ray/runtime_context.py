from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray.worker
import json
import logging

logger = logging.getLogger(__name__)


class RuntimeContext(object):
    """A class used for getting runtime context."""

    def __init__(self, worker=None):
        self.worker = worker

    @property
    def current_job_id(self):
        """Get current driver ID for this worker or driver.

        Returns:
            If called by a driver, this returns the driver ID. If called in
                a task, return the driver ID of the associated driver.
        """
        assert self.worker is not None
        return self.worker.current_job_id

    @property
    def get_job_configs(self):
        key = "_JOB_CONFIG_{}".format(self.current_driver_id.hex())
        result = ray.state.state._execute_command_on_primary("GET", key)
        assert result != None
        return json.loads(result, encoding="UTF-8")

    @property
    def current_actor_id(self):
        """Get the current actor ID in this worker.

        Returns:
            The current driver id in this worker.
        """
        assert self.worker is not None
        assert self.worker.mode == ray.worker.WORKER_MODE

        return self.worker.actor_id

    @property
    def raylet_socket_name(self):

        assert self.worker is not None
        return ray.worker._global_node.raylet_socket_name

    @property
    def object_store_socket_name(self):
        assert self.worker is not None
        return ray.worker._global_node.plasma_store_socket_name

    # Note that this method should not be called in a normal task.
    @property
    def was_current_actor_reconstructed(self):
        # None means call this in a normal task.
        assert self.worker is not None
        actor_info = ray.state.actors(self.current_actor_id.hex())
        if actor_info and actor_info["NumRestarts"] != 0:
            return True
        return False

    @property
    def redis_address(self):
        assert self.worker is not None
        return ray.state.state.get_redis_address()


_runtime_context = None


def get_runtime_context():
    global _runtime_context
    if _runtime_context is None:
        _runtime_context = RuntimeContext(ray.worker.global_worker)

    return _runtime_context
