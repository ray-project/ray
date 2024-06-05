from dataclasses import dataclass
from typing import Optional

from ray.actor import ActorHandle


@dataclass
class WorkerStatus:
    running: bool
    error: Optional[Exception] = None


@dataclass(frozen=True)
class Worker:
    # metadata
    actor: ActorHandle
