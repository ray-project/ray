from abc import ABC
import random
from typing import Any, Callable, List, Tuple, Union

import ray
from ray import cloudpickle, ObjectRef
from ray.actor import ActorHandle
from ray.remote_function import RemoteFunction

from ray.serve.pipeline.common import ExecutionMode, StepConfig


class Executor(ABC):
    def __init__(self, serialized_callable_factory: bytes, config: StepConfig):
        pass

    def call(self, input_arg: Tuple[Any]) -> Union[Any, ObjectRef]:
        pass

    async def call_async(self, input_arg: Tuple[Any]) -> Union[Any, ObjectRef]:
        pass


class LocalExecutor(Executor):
    """Executor that runs code in-process."""

    def __init__(self, serialized_callable_factory: bytes, _config: StepConfig):
        self._callable: Callable = cloudpickle.loads(serialized_callable_factory)()

    def call(self, *args: Tuple[Any]) -> Any:
        args = tuple(
            ray.get(arg) if isinstance(arg, ObjectRef) else arg for arg in args
        )
        return self._callable(*args)

    async def call_async(self, input_arg: Tuple[Any]) -> Any:
        raise NotImplementedError("No async support yet.")


class TasksExecutor(Executor):
    """Executor that wraps code in Ray tasks."""

    def __init__(self, serialized_callable_factory: bytes, config: StepConfig):
        @ray.remote
        def callable_wrapper(*args):
            return cloudpickle.loads(serialized_callable_factory)()(*args)

        self._remote_function: RemoteFunction = callable_wrapper

    def call(self, *args: Tuple[Any]) -> ObjectRef:
        return self._remote_function.remote(*args)

    async def call_async(self) -> ObjectRef:
        raise NotImplementedError("No async support yet.")


@ray.remote
class ExecutorActor:
    """Actor that executes the serialized code for a given step."""

    def __init__(self, serialize_callable_factory: bytes):
        self._callable = cloudpickle.loads(serialize_callable_factory)()

    def call(self, *args, **kwargs):
        """Call the underlying callable."""
        return self._callable(*args, **kwargs)

    def ready(self):
        """Used to wait for the actor to become available."""
        pass


class ActorsExecutor(Executor):
    """Executor that wraps a pool of Ray actors."""

    def __init__(self, serialized_callable_factory: bytes, config: StepConfig):
        self._actors: List[ActorHandle] = [
            ExecutorActor.remote(serialized_callable_factory)
            for _ in range(config.num_replicas)
        ]
        # Wait for the actors to be ready.
        ray.get([actor.ready.remote() for actor in self._actors])

    def call(self, *args: Tuple[Any]) -> ObjectRef:
        # TODO(simon): use ActorGroup to load balance.
        return random.choice(self._actors).call.remote(*args)

    async def call_async(self) -> ObjectRef:
        raise NotImplementedError("No async support yet.")


def create_executor_from_step_config(
    serialized_callable_factory: bytes, config: StepConfig
) -> Executor:
    if config.execution_mode == ExecutionMode.LOCAL:
        return LocalExecutor(serialized_callable_factory, config)
    elif config.execution_mode == ExecutionMode.TASKS:
        return TasksExecutor(serialized_callable_factory, config)
    elif config.execution_mode == ExecutionMode.ACTORS:
        return ActorsExecutor(serialized_callable_factory, config)
    else:
        assert False, f"Unknown execution mode: {config.execution_mode}"
