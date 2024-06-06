import abc
from dataclasses import dataclass
from typing import Dict

from ray.train.v2.worker_group.worker import Worker, WorkerStatus


@dataclass
class WorkerGroupStatus:
    num_workers: int
    latest_restart_time: float
    worker_statuses: Dict[Worker, WorkerStatus]

    @property
    def errors(self) -> Dict[Worker, Exception]:
        return {
            worker: status.error
            for worker, status in self.worker_statuses.items()
            if status.error is not None
        }

    @property
    def finished(self) -> bool:
        return all(not status.running for status in self.worker_statuses.values())


class WorkerGroup(abc.ABC):
    def __init__(self):
        pass

    def get_status(self) -> WorkerGroupStatus:
        pass

    def run_train_fn(self, train_fn):
        pass

    def poll(self) -> WorkerGroupStatus:
        pass

    def start(self, num_workers: int, resources_per_worker: dict):
        pass

    def shutdown(self):
        pass
