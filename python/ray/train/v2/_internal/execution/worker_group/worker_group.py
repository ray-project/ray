import abc
from dataclasses import dataclass
from typing import Dict, Optional

from ray.train.v2._internal.execution.worker_group import WorkerStatus


@dataclass
class WorkerGroupStatus:
    num_workers: int
    latest_restart_time: float
    worker_statuses: Dict[int, WorkerStatus]

    @property
    def errors(self) -> Dict[int, Exception]:
        return {
            world_rank: status.error
            for world_rank, status in self.worker_statuses.items()
            if status.error is not None
        }

    @property
    def finished(self) -> bool:
        return self.worker_statuses and all(
            not status.running for status in self.worker_statuses.values()
        )


class WorkerGroup(abc.ABC):
    """A group of workers that runs a training function.

    Each worker should be assigned a unique world rank
    which ranges from [0, num_workers).
    """

    @abc.abstractmethod
    def start(self, num_workers: int, resources_per_worker: dict):
        raise NotImplementedError

    @abc.abstractmethod
    def shutdown(self):
        raise NotImplementedError

    @abc.abstractmethod
    def run_train_fn(self, train_fn):
        raise NotImplementedError

    @abc.abstractmethod
    def poll_status(self, timeout: Optional[float] = None) -> WorkerGroupStatus:
        raise NotImplementedError
