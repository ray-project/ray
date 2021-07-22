import logging
from typing import Callable, Dict

import ray
from ray.exceptions import RayActorError
from ray.util.sgd.v2.actor_group import ActorGroup
from ray.util.sgd.v2.backend_config import BackendConfig
from ray.util.sgd.v2.worker_config import WorkerConfig

logger = logging.getLogger(__name__)


class BackendExecutor:
    def __init__(self, worker_config: WorkerConfig,
                 backend_config: BackendConfig):
        self.actor_group = ActorGroup(worker_config.num_workers,
                                      worker_config.num_cpus_per_worker,
                                      worker_config.num_gpus_per_worker,
                                      worker_config.placement_strategy)

    def start(self):
        self.actor_group.start()

    def execute(self, train_func: Callable, config: Dict):
        def wrapped_train_func():
            return train_func(config)

        def fetch_next():
            # get next results from sgd.report from the queue of each worker
            pass

        # Run the training function asynchronously on all workers.
        # returns list of object references of train_func
        not_ready = self.actor_group.execute_async(wrapped_train_func)

        # While the training function has not finished on all the workers,
        # Get results and yield.
        while not_ready:
            try:
                # return actual results of sgd.report in queue
                results = self.actor_group.execute(fetch_next)
            except RayActorError:
                # An actor failed.
                # handle fault tolerance and elastic training here.
                raise RuntimeError("Training failed.")

            yield results  # one sgd report iteration
            ready, not_ready = ray.wait(not_ready, timeout=0)

    def shutdown(self):
        self.actor_group.shutdown()

    def run(self, train_func: Callable, config: Dict):
        self.start()
        # this will hang if user doesnt finish iterating
        yield from self.execute(train_func, config)
        self.shutdown()
