from ray.train.v2._internal.execution.worker_group import WorkerGroup


class Callback:
    pass


class SystemCallback(Callback):
    def after_worker_group_start(self, worker_group: WorkerGroup):
        """Called after the worker group is started.
        All workers should be ready to execute tasks."""
        pass

    def before_worker_group_shutdown(self, worker_group: WorkerGroup):
        """Called before the worker group is shut down.
        Workers may be dead at this point, so this method should catch
        and handle exceptions if attempting to execute tasks."""
        pass
