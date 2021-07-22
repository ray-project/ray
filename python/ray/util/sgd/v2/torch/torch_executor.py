from typing import Callable, Dict

from ray.util.sgd.torch.worker_group import RemoteWorkerGroup
from ray.util.sgd.v2.backend_config import BackendConfig
from ray.util.sgd.v2.backend_executor import BackendExecutor
from ray.util.sgd.v2.worker_config import WorkerConfig


class TorchExecutor(BackendExecutor):
    def __init__(self, worker_config: WorkerConfig,
                 backend_config: BackendConfig):
        super().__init__(worker_config, backend_config)

        # To be replaced by ActorGroup.
        self._num_workers = worker_config.num_workers
        self.worker_group = RemoteWorkerGroup(
            worker_config.num_workers,
            {},
            {},
            None,
            worker_config.timeout_s,
            worker_config.num_cpus_per_worker,
            bool(worker_config.num_gpus_per_worker)  # True if >= 1
        )

    def start(self):
        # TODO start ActorGroup instead.
        # num_workers should not be needed in actor_group
        self.worker_group.start_workers(self._num_workers)

    def execute(self, train_func: Callable, config: Dict):
        def WrappedTrainFunc():
            return train_func(config)

        return self.worker_group.apply_all_workers(WrappedTrainFunc)

    def shutdown(self):
        # TODO shutdown ActorGroup instead.
        self.worker_group.shutdown()
