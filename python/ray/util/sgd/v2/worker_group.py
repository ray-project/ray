import logging
from typing import Callable, List, TypeVar

import ray
from ray.types import ObjectRef

T = TypeVar("T")

logger = logging.getLogger(__name__)


class BaseWorker:
    """A class to execute arbitrary functions. Does not hold any state."""

    def execute(self, func: Callable[..., T], *args, **kwargs) -> T:
        """Executes the input function and returns the output.
        Args:
            func(Callable): The function to execute.
            args, kwargs: The arguments to pass into func.
        """
        return func(*args, **kwargs)


class WorkerGroup:
    """Group of Ray Actors that can execute arbitrary functions.

    ``WorkerGroup`` launches Ray actors according to the given
    specification. It can then execute arbitrary Python functions in each of
    these workers.

    If not enough resources are available to launch the actors, the Ray
    cluster will automatically scale up if autoscaling is enabled.

    Args:
        num_workers (int): The number of workers (Ray actors) to launch.
            Defaults to 1.
        num_cpus_per_worker (float): The number of CPUs to reserve for each
            worker. Fractional values are allowed. Defaults to 1.
        num_gpus_per_worker (float): The number of GPUs to reserve for each
            worker. Fractional values are allowed. Defaults to 0.

    Example:

    .. code_block:: python

        worker_group = WorkerGroup(num_workers=2)
        output = worker_group.execute(lambda: 1)
        assert len(output) == 2
        assert all(o == 1 for o in output)
    """

    def __init__(self,
                 num_workers: int = 1,
                 num_cpus_per_worker: float = 1,
                 num_gpus_per_worker: float = 0):

        if num_workers <= 0:
            raise ValueError("The provided `num_workers` must be greater "
                             f"than 0. Received num_workers={num_workers} "
                             f"instead.")
        if num_cpus_per_worker < 0 or num_gpus_per_worker < 0:
            raise ValueError("The number of CPUs and GPUs per worker must "
                             "not be negative. Received "
                             f"num_cpus_per_worker={num_cpus_per_worker} and "
                             f"num_gpus_per_worker={num_gpus_per_worker}.")

        self.num_workers = num_workers
        self.num_cpus_per_worker = num_cpus_per_worker
        self.num_gpus_per_worker = num_gpus_per_worker
        self.workers = []
        self.start()

    def start(self):
        """Starts all the workers in this worker group."""
        if self.workers and len(self.workers) > 0:
            raise RuntimeError("The workers have already been started. "
                               "Please call `shutdown` first if you want to "
                               "restart them.")
        remote_cls = ray.remote(
            num_cpus=self.num_cpus_per_worker,
            num_gpus=self.num_gpus_per_worker)(BaseWorker)
        logger.debug(f"Starting {self.num_workers} workers.")
        self.workers = [remote_cls.remote() for _ in range(self.num_workers)]
        logger.debug(f"{len(self.workers)} workers have successfully started.")

    def shutdown(self, patience_s: float = 5):
        """Shutdown all the workers in this worker group.

        Args:
            patience_s (float): Attempt a graceful shutdown
                of the workers for this many seconds. Fallback to force kill
                if graceful shutdown is not complete after this time. If
                this is less than or equal to 0, immediately force kill all
                workers.
        """
        logger.debug(f"Shutting down {len(self.workers)} workers.")
        if patience_s <= 0:
            for worker in self.workers:
                ray.kill(worker)
        else:
            done_refs = [w.__ray_terminate__.remote() for w in self.workers]
            # Wait for actors to die gracefully.
            done, not_done = ray.wait(done_refs, timeout=patience_s)
            if not_done:
                logger.debug("Graceful termination failed. Falling back to "
                             "force kill.")
                # If all actors are not able to die gracefully, then kill them.
                for worker in self.workers:
                    ray.kill(worker)

        logger.debug("Shutdown successful.")
        self.workers = []

    def execute_async(self, func: Callable[..., T], *args,
                      **kwargs) -> List[ObjectRef]:
        """Execute ``func`` on each worker and return the futures.

        Args:
            func (Callable): A function to call on each worker.
            args, kwargs: Passed directly into func.

        Returns:
            (List[ObjectRef]) A list of ``ObjectRef`` representing the
                output of ``func`` from each worker. The order is the same
                as ``self.workers``.

        """
        if len(self.workers) <= 0:
            raise RuntimeError("There are no active workers. This worker "
                               "group has most likely been shut down. Please"
                               "create a new WorkerGroup or restart this one.")

        return [w.execute.remote(func, *args, **kwargs) for w in self.workers]

    def execute(self, func: Callable[..., T], *args, **kwargs) -> List[T]:
        """Execute ``func`` on each worker and return the outputs of ``func``.

        Args:
            func (Callable): A function to call on each worker.
            args, kwargs: Passed directly into func.

        Returns:
            (List[T]) A list containing the output of ``func`` from each
                worker. The order is the same as ``self.workers``.

        """
        return ray.get(self.execute_async(func, *args, **kwargs))

    def execute_single_async(self, worker_index: int, func: Callable[..., T],
                             *args, **kwargs) -> ObjectRef:
        """Execute ``func`` on worker ``worker_index`` and return futures.

        Args:
            worker_index (int): The index to execute func on.
            func (Callable): A function to call on the first worker.
            args, kwargs: Passed directly into func.

        Returns:
            (ObjectRef) An ObjectRef representing the output of func.

        """
        if worker_index >= len(self.workers):
            raise ValueError(f"The provided worker_index {worker_index} is "
                             f"not valid for {self.num_workers} workers.")
        return self.workers[worker_index].execute.remote(func, *args, **kwargs)

    def execute_single(self, worker_index: int, func: Callable[..., T], *args,
                       **kwargs) -> T:
        """Execute ``func`` on worker with index ``worker_index``.

        Args:
            worker_index (int): The index to execute func on.
            func (Callable): A function to call on the first worker.
            args, kwargs: Passed directly into func.

        Returns:
            (T) The output of func.

        """

        return ray.get(
            self.execute_single_async(worker_index, func, *args, **kwargs))

    def __len__(self):
        return len(self.workers)
