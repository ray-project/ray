import logging
from typing import Optional

from ray.exceptions import RayActorError
from ray.util.sgd.v2.distributed_actor import DistributedActor
from ray.util.sgd.v2.worker_placement_strategy import WorkerPlacementStrategy

from python.ray.util.client import ray

logger = logging.getLogger(__name__)


class ActorGroup:
    def __init__(
            self,
            num_actors: Optional[int] = None,
            cpus_per_actor: int = 1,
            gpus_per_actor: int = 0,  # Are 0 and None the same?
            placement_group_strategy: Optional[WorkerPlacementStrategy] = None
    ):
        self._num_actors = num_actors
        self._num_cpus_per_actor = cpus_per_actor
        self._num_gpus_per_actor = gpus_per_actor
        self._placement_group_strategy = placement_group_strategy
        self._params = {}
        self._dist_params = {}

        self.remote_actors = []

    def _start_remote_workers(self, num_workers):
        RemoteDistributedActor = ray.remote(
            num_cpus=self._num_cpus_per_actor,
            num_gpus=self._num_gpus_per_actor)(DistributedActor)

        self.remote_actors = [
            RemoteDistributedActor.remote(**{
                **self._params,
                **self._dist_params
            }) for _ in range(num_workers)
        ]

    def start(self):
        self._start_remote_workers(self._num_actors)

    def execute_async(self, func, *args):
        return [w.execute.remote(func) for w in self.remote_actors]

    def execute(self, func, *args):
        return ray.get(self.execute_async(func, *args))

    def _shutdown_remote_actors(self):
        shutdown_results = [
            worker.shutdown.remote() for worker in self.remote_actors
        ]
        try:
            ray.get(shutdown_results)
            for worker in self.remote_actors:
                worker.__ray_terminate__.remote()
        except RayActorError:
            logger.warning("Failed to shutdown")

    def shutdown(self):
        self._shutdown_remote_actors()
