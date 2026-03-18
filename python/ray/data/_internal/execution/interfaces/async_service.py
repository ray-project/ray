import logging
import time
import uuid
from abc import ABC, abstractmethod
from concurrent import futures
from enum import Enum
from typing import (
    Any,
    Dict,
    Generic,
    Optional,
    Protocol,
    TypeVar,
    runtime_checkable,
)

import ray
from ray.actor import ActorHandle
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

logger = logging.getLogger(__name__)

InputT = TypeVar("InputT")
OutputT = TypeVar("OutputT")
ServiceKeyT = uuid.UUID

ASYNC_SERVICE_ACTOR_NAME = "RayData_AsyncServiceActor"
ASYNC_SERVICE_NAMESPACE = "RayData_AsyncService"


class AsyncServiceTask(ABC, Generic[InputT, OutputT]):
    """A unit of work that runs asynchronously in the AsyncServiceActor process.

    Subclasses implement ``run()`` to perform expensive computation (CPU-bound)
    or concurrent I/O (via the provided ThreadPoolExecutor).

    Set ``min_interval_s`` to throttle how often the task is submitted.
    The ``AsyncServiceHandle`` enforces this on the scheduling thread.
    """

    min_interval_s: float = 0

    @abstractmethod
    def run(self, obj: InputT, tpe: futures.ThreadPoolExecutor) -> OutputT:
        ...


@runtime_checkable
class AsyncRefreshable(Protocol):
    """Protocol for components that participate in async state refresh.

    Both ``PhysicalOperator`` and ``ExecutionCallback`` can implement this
    via structural subtyping — no inheritance required.

    The executor manages the submit/poll lifecycle for all registered
    refreshables; components only implement these three methods.

    The flow each scheduling step:
        1. ``build_refresh_input(executor)`` — snapshot state (scheduling thread)
        2. ``task.run(snapshot, tpe)`` — expensive work (actor process)
        3. ``apply_refresh_result(result, executor)`` — apply output (scheduling thread)
    """

    def create_async_task(self) -> Optional["AsyncServiceTask"]:
        """Return the task to register with the async service, or None to skip."""
        ...

    def build_refresh_input(self) -> Any:
        """Build a serializable input snapshot for the async task.

        Called on the scheduling thread. The *executor* is passed so
        implementations need not hold a reference to it.
        """
        ...

    def apply_refresh_result(self, result: Any) -> None:
        """Apply the result from the async task back to internal state.

        Called on the scheduling thread when the result is ready.
        """
        ...


class ErrorPolicy(Enum):
    LOG_AND_RETRY = "log_and_retry"
    PROPAGATE = "propagate"


@ray.remote(num_cpus=0)
class AsyncServiceActor:
    """Singleton Ray actor that hosts async service tasks in a separate process.

    Multiple executors/datasets can register tasks via unique ServiceKeyT keys.
    Each ``update()`` call runs the task synchronously within the actor process,
    and the result is returned to the caller via ObjectRef.
    """

    def __init__(self):
        self._tasks: Dict[ServiceKeyT, AsyncServiceTask] = {}
        self._tpe = futures.ThreadPoolExecutor(max_workers=1)

    def register(self, task: AsyncServiceTask) -> ServiceKeyT:
        service_key = uuid.uuid4()
        self._tasks[service_key] = task
        return service_key

    def update(self, key: ServiceKeyT, state_obj: Any) -> Any:
        task = self._tasks[key]
        return task.run(state_obj, self._tpe)

    def unregister(self, key: ServiceKeyT):
        self._tasks.pop(key, None)


class AsyncServiceHandle(Generic[InputT, OutputT]):
    """Client-side handle for non-blocking interaction with an AsyncServiceTask.

    Used in the scheduling loop to submit snapshots and poll for
    results via ``ray.wait(timeout=0)`` without blocking.
    """

    def __init__(
        self,
        task: AsyncServiceTask,
        error_policy: ErrorPolicy = ErrorPolicy.LOG_AND_RETRY,
    ):

        self._key = ray.get(get_or_create_async_service_actor().register.remote(task))
        self._min_interval_s = task.min_interval_s
        self._error_policy = error_policy
        self._pending_ref: Optional[ray.ObjectRef] = None
        self._last_submit_time: float = 0

    def should_submit(self) -> bool:
        """True if no request is in-flight and the minimum interval has elapsed."""
        if self._pending_ref is not None:
            print("pending ref is None")
            return False
        if self._min_interval_s > 0:
            print(f"delta is {time.monotonic() - self._last_submit_time}")
            return time.monotonic() - self._last_submit_time >= self._min_interval_s
        return True

    def request_refresh(self, state: InputT) -> None:
        """Submit state to the actor for async computation.

        Only call when ``should_submit()`` returns True.
        """
        assert self._pending_ref is None, "Previous refresh still in flight"
        self._last_submit_time = time.monotonic()
        self._pending_ref = get_or_create_async_service_actor().update.remote(
            self._key, state
        )

    def try_get_result(self) -> Optional[OutputT]:
        """Non-blocking poll. Returns the result if ready, None otherwise."""
        assert self._pending_ref is not None, "No pending ref yet"
        ready, _ = ray.wait([self._pending_ref], timeout=0)
        if not ready:
            return None

        try:
            return ray.get(self._pending_ref, timeout=1)
        except ray.exceptions.GetTimeoutError:
            pass
        except Exception as e:
            if self._error_policy == ErrorPolicy.PROPAGATE:
                raise
            logger.debug(f"Async service task failed: {e}")
        finally:
            self._pending_ref = None

    def has_in_flight_request(self) -> bool:
        return self._pending_ref is not None

    def shutdown(self) -> None:
        if self._pending_ref is not None:
            ray.cancel(self._pending_ref, force=False)
            self._pending_ref = None
        get_or_create_async_service_actor().unregister.remote(self._key)


def get_or_create_async_service_actor() -> ActorHandle:
    """Get or create the singleton AsyncServiceActor.

    The actor is co-located on the driver node via NodeAffinitySchedulingStrategy
    for low-latency serialization. It uses ``get_if_exists=True`` so multiple
    datasets/executors share the same actor.
    """
    scheduling_strategy = NodeAffinitySchedulingStrategy(
        ray.get_runtime_context().get_node_id(),
        soft=False,
    )
    return AsyncServiceActor.options(
        name=ASYNC_SERVICE_ACTOR_NAME,
        namespace=ASYNC_SERVICE_NAMESPACE,
        get_if_exists=True,
        lifetime="detached",
        scheduling_strategy=scheduling_strategy,
    ).remote()
