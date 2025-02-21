import os
import queue
import socket
from dataclasses import dataclass
from functools import cached_property
from typing import Any, Callable, Dict, List, Optional, TypeVar, Union

import ray
from ray.types import ObjectRef
from .thread_runner import ThreadRunner
from ray.actor import ActorHandle
from ray.data.iterator import DataIterator
from ray.train import Checkpoint
from ray.train.v2._internal.execution.callback import (
    TrainContextCallback,
    WorkerCallback,
)
from ray.train.v2._internal.execution.checkpoint.sync_actor import SynchronizationActor
from ray.train.v2._internal.execution.context import (
    DistributedContext,
    ExecutionContext,
    TrainContext,
    TrainRunContext,
    get_train_context,
    set_train_context,
)
from ray.train.v2._internal.execution.storage import StorageContext
from ray.train.v2._internal.execution.worker_group.poll import WorkerStatus
from ray.train.v2._internal.logging.logging import configure_worker_logger
from ray.train.v2._internal.logging.patch_print import patch_print_function

T = TypeVar("T")


@dataclass(frozen=True)
class ActorMetadata:
    hostname: str
    node_id: str
    node_ip: str
    pid: int
    accelerator_ids: Dict[str, List[Union[int, str]]]

    @property
    def gpu_ids(self) -> List[Union[int, str]]:
        return self.accelerator_ids.get("GPU", [])

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
    resources: Dict[str, float]
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

    def execute_async(self, fn: Callable[..., T], *fn_args, **fn_kwargs) -> ObjectRef:
        """Execute ``func`` on worker.

        Returns:
            (ObjectRef) An ObjectRef representing the output of func.

        """
        return self.actor.execute.options(name=f"execute.{fn.__name__}").remote(
            fn, *fn_args, **fn_kwargs
        )


class RayTrainWorker:
    def __init__(self):
        self._callbacks: List[WorkerCallback] = []

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

    def shutdown(self):
        """Shutdown the worker.

        This method is not doing the real shutdown, but it is used by the worker
        group to signal the worker to stop running the training function.
        Any shutdown worker callbacks can hook on this method to implement the
        corresponding shutdown logic. Note that the shutdown logic needs to be
        thread-safe if it is running in a separate thread.
        """
        for callback in self._callbacks:
            callback.before_worker_shutdown()

    def init_train_context(
        self,
        train_run_context: TrainRunContext,
        distributed_context: DistributedContext,
        synchronization_actor: SynchronizationActor,
        storage_context: StorageContext,
        worker_callbacks: List[Union[WorkerCallback, TrainContextCallback]],
        dataset_shards: Dict[str, DataIterator] = None,
        checkpoint: Optional[Checkpoint] = None,
    ):
        self._callbacks = [c for c in worker_callbacks if isinstance(c, WorkerCallback)]
        context_callbacks_to_propagate = [
            c for c in worker_callbacks if isinstance(c, TrainContextCallback)
        ]
        context = TrainContext(
            run_config=train_run_context.run_config,
            distributed_context=distributed_context,
            execution_context=ExecutionContext(
                synchronization_actor=synchronization_actor,
                # Make the queue size 1 to avoid building up too
                # many unprocessed results.
                result_queue=queue.Queue(maxsize=1),
                training_thread_runner=ThreadRunner(),
                train_context_callbacks=context_callbacks_to_propagate,
            ),
            storage_context=storage_context,
            dataset_shards=dataset_shards or {},
            checkpoint=checkpoint,
        )
        # Configure the train and root logger for the worker processes.
        configure_worker_logger(context)
        patch_print_function()
        # Set the train context global variable for the worker.
        set_train_context(context)

        for callback in self._callbacks:
            callback.after_init_train_context()
