from dataclasses import dataclass
from typing import Any, Optional

import ray.actor
from ray.air.execution.actor_request import ActorInfo
from ray.air.execution.resources.request import ResourceRequest


@dataclass
class ExecutionEvent:
    pass


@dataclass
class _ResourceReady(ExecutionEvent):
    resource_request: ResourceRequest


@dataclass
class FutureResult(ExecutionEvent):
    actor: ray.actor.ActorHandle


@dataclass
class NativeResult(ExecutionEvent):
    data: Any


@dataclass
class FutureFailed(FutureResult):
    exception: Exception


@dataclass
class ActorStarted(ExecutionEvent):
    actor: ray.actor.ActorHandle
    actor_info: ActorInfo


@dataclass
class ActorStopped(ExecutionEvent):
    actor: ray.actor.ActorHandle
    actor_info: ActorInfo
    exception: Optional[Exception] = None
