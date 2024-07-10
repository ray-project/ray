import os
import queue
import socket
from dataclasses import dataclass
from functools import cached_property
from typing import Any, Callable, Dict, List, Optional, TypeVar, Union

import ray
from .thread_runner import ThreadRunner
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


@dataclass(frozen=True)
class ActorMetadata:
    hostname: str
    node_id: str
    node_ip: str
    pid: int
    accelerator_ids: Dict[str, List[Union[int, str]]]

    @cached_property
    def _repr(self) -> str:
        indent = "  "
        repr_lines = [
            "ActorMetadata(",
            f"{indent}hostname={repr(self.hostname)},",
            f"{indent}node_id={repr(self.node_id)},",
            f"{indent}node_ip={repr(self.node_ip)},",
            f"{indent}pid={repr(self.pid)},",
        ]
        non_empty_accelerator_ids = {k: v for k, v in self.accelerator_ids.items() if v}
        if non_empty_accelerator_ids:
            repr_lines.append(f"{indent}accelerator_ids={non_empty_accelerator_ids},")
        repr_lines.append(")")
        return "\n".join(repr_lines)

    def __repr__(self) -> str:
        return self._repr


@dataclass
class Worker:
    actor: ActorHandle
    metadata: ActorMetadata
    distributed_context: Optional[DistributedContext] = None

    @cached_property
    def _repr(self) -> str:
        indent = "  "
        metadata_repr = repr(self.metadata).replace("\n", f"\n{indent}")
        context_repr = repr(self.distributed_context).replace("\n", f"\n{indent}")

        repr_lines = [
            "Worker(",
            f"{indent}actor={repr(self.actor)},",
            f"{indent}metadata={metadata_repr},",
            f"{indent}distributed_context={context_repr},",
            ")",
        ]
        return "\n".join(repr_lines)

    def __repr__(self) -> str:
        return self._repr


class RayTrainWorker:
    def execute(self, fn: Callable[..., T], *fn_args, **fn_kwargs) -> T:
        return fn(*fn_args, **fn_kwargs)

    def run_train_fn(self, train_fn: Callable[[], Any]):
        """Run the training function in a separate thread.

        This function should return immediately, freeing up the main actor thread
        to perform other tasks such as polling the status.
        """
        # Create and start the training thread.
        get_train_context().execution_context.training_thread_runner.run(train_fn)

    def get_metadata(self) -> ActorMetadata:
        return ActorMetadata(
            hostname=socket.gethostname(),
            node_id=ray.get_runtime_context().get_node_id(),
            node_ip=ray.util.get_node_ip_address(),
            pid=os.getpid(),
            accelerator_ids=ray.get_runtime_context().get_accelerator_ids(),
        )

    def poll_status(self) -> WorkerStatus:
        execution_context = get_train_context().execution_context

        # TODO: We can implement two phase commit here.
        # Only mark the task done when the result has been processed by the controller.
        try:
            training_result = execution_context.result_queue.get_nowait()
            execution_context.result_queue.task_done()
        except queue.Empty:
            training_result = None

        error = execution_context.training_thread_runner.get_error()

        # TODO: The running state should not be conflated with queue flushing.
        # Running should only be true if the user code is still running.
        # This relies on `worker_group_status.finished` returning False
        # until all training results have been flushed.
        running = execution_context.training_thread_runner.is_running() or bool(
            training_result
        )

        return WorkerStatus(
            running=running, error=error, training_result=training_result
        )

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
                result_queue=queue.Queue(),
                training_thread_runner=ThreadRunner(),
            ),
            storage_context=storage_context,
            checkpoint=checkpoint,
        )
        set_train_context(context)
