import os
import socket
from dataclasses import dataclass
from typing import Callable, List, Optional, TypeVar

import ray
from ray.actor import ActorHandle

T = TypeVar("T")


@dataclass
class WorkerStatus:
    running: bool
    error: Optional[Exception] = None


@dataclass
class ActorMetadata:
    hostname: str
    node_id: str
    node_ip: str
    pid: int
    accelerator_ids: List[int]


@dataclass
class Worker:
    actor: ActorHandle
    metadata: ActorMetadata


class RayTrainWorker:
    def execute(self, fn: Callable[..., T], *fn_args, **fn_kwargs) -> T:
        return fn(*fn_args, **fn_kwargs)

    def run_train_fn(self, train_fn: Callable):
        train_fn()

    def get_metadata(self) -> ActorMetadata:
        return ActorMetadata(
            hostname=socket.gethostname(),
            node_id=ray.get_runtime_context().get_node_id(),
            node_ip=ray.util.get_node_ip_address(),
            pid=os.getpid(),
            accelerator_ids=ray.get_runtime_context().get_accelerator_ids(),
        )

    def poll_status(self):
        # TODO: Implement checkpoint polling logic.
        pass
