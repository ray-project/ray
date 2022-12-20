from dataclasses import dataclass
from typing import List, Optional

from ray.air.execution.actor_request import ActorInfo


@dataclass
class ExecutionEvent:
    """Execution event emitted from the actor manager.

    All events should inherit this interface.
    """

    pass


@dataclass
class FutureResult(ExecutionEvent):
    """Event emitted when a future successfully resolves."""

    actor_info: ActorInfo


@dataclass
class MultiFutureResult(ExecutionEvent):
    """Event emitted when a collection of synchronous futures successfully resolves."""

    results: List[FutureResult]


@dataclass
class FutureCancelled(FutureResult):
    """Event emitted when a future has been cancelled."""

    pass


@dataclass
class FutureFailed(FutureResult):
    """Event emitted when a future failed due to a ``RayTaskError``."""

    exception: Exception


@dataclass
class MultiFutureFailed(MultiFutureResult):
    """Event emitted when a collection of synchronous futures failed."""

    results: List[ExecutionEvent]
    exception: Exception


@dataclass
class ActorStarted(ExecutionEvent):
    """Event emitted when an actor started."""

    actor_info: ActorInfo


@dataclass
class ActorStopped(ExecutionEvent):
    """Event emitted when an actor stopped."""

    actor_info: ActorInfo


@dataclass
class ActorFailed(ExecutionEvent):
    """Event emitted when an actor failed."""

    actor_info: ActorInfo
    exception: Optional[Exception] = None
