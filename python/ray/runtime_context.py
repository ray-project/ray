import ray.worker
import logging

logger = logging.getLogger(__name__)


class RuntimeContext(object):
    """A class used for getting runtime context."""

    def __init__(self, worker):
        assert worker is not None
        self.worker = worker

    @property
    def current_job_id(self):
        """Get current job ID for this worker or driver.

        Returns:
            If called by a driver, this returns the job ID. If called in
                a task, return the job ID of the associated driver.
        """
        return self.worker.current_job_id

    @property
    def current_actor_id(self):
        """Get the current actor ID in this worker.

        Returns:
            The current driver id in this worker.
        """
        # only worker mode has actor_id
        assert self.worker.mode == ray.worker.WORKER_MODE, (
            f"This method is only available when the process is a\
                 worker. Current mode: {self.worker.mode}")
        return self.worker.actor_id

    @property
    def was_current_actor_reconstructed(self):
        """Check whether this actor has been restarted

        Returns:
            Whether this actor has been ever restarted.
        """
        # TODO: this method should not be called in a normal task.
        actor_info = ray.state.actors(self.current_actor_id.hex())
        return actor_info and actor_info["NumRestarts"] != 0


_runtime_context = None


def get_runtime_context():
    global _runtime_context
    if _runtime_context is None:
        _runtime_context = RuntimeContext(ray.worker.global_worker)

    return _runtime_context
