import os
import socket
from dataclasses import dataclass
from queue import Queue
from typing import Callable, Dict, List, Optional, TypeVar, Union

import ray
from ray.actor import ActorHandle
from ray.train import Checkpoint
from ray.train._internal.session import _TrainingResult
from ray.train.v2._internal.execution.checkpoint.sync_actor import SynchronizationActor
from ray.train.v2._internal.execution.context import (
    DistributedContext,
    ExecutionContext,
    TrainContext,
    get_train_context,
    set_train_context,
)
from ray.train.v2._internal.execution.storage import StorageContext
from ray.train.v2.api.config import RunConfig

T = TypeVar("T")


@dataclass
class WorkerStatus:
    running: bool
    error: Optional[Exception] = None
    training_result: Optional[_TrainingResult] = None


@dataclass
class ActorMetadata:
    hostname: str
    node_id: str
    node_ip: str
    pid: int
    accelerator_ids: Dict[str, List[Union[int, str]]]


@dataclass
class Worker:
    actor: ActorHandle
    metadata: ActorMetadata


class RayTrainWorker:
    def execute(self, fn: Callable[..., T], *fn_args, **fn_kwargs) -> T:
        return fn(*fn_args, **fn_kwargs)

    def run_train_fn(self, train_fn: Callable):
        try:
            train_fn()
        finally:
            # Make sure the result queue is fully consumed before the worker exits.
            # Training is not "finished" until all results are processed.
            self.flush_result_queue()

    def get_metadata(self) -> ActorMetadata:
        return ActorMetadata(
            hostname=socket.gethostname(),
            node_id=ray.get_runtime_context().get_node_id(),
            node_ip=ray.util.get_node_ip_address(),
            pid=os.getpid(),
            accelerator_ids=ray.get_runtime_context().get_accelerator_ids(),
        )

    def flush_result_queue(self):
        """Waits until all results are consumed and processed by the controller.

        Joining the queue will block until all items have been processed.
        This requires `poll_status` to be called repeatedly until the
        queue has been fully flushed.
        """
        result_queue = get_train_context().get_result_queue()
        result_queue.join()

    def poll_status(self) -> _TrainingResult:
        train_context = get_train_context()
        result_queue = train_context.get_result_queue()
        if result_queue.empty():
            return None
        # TODO: We can implement two phase commit here.
        # Only mark the task done when the result has been processed by the controller.
        training_result = result_queue.get()
        result_queue.task_done()
        return training_result

    def init_train_context(
        self,
        run_config: RunConfig,
        distributed_context: DistributedContext,
        synchronization_actor: SynchronizationActor,
        storage_context: StorageContext,
        checkpoint: Optional[Checkpoint] = None,
    ):
        context = TrainContext(
            run_config=run_config,
            distributed_context=distributed_context,
            execution_context=ExecutionContext(
                synchronization_actor=synchronization_actor,
                result_queue=Queue(),
            ),
            storage_context=storage_context,
            checkpoint=checkpoint,
        )
        set_train_context(context)
