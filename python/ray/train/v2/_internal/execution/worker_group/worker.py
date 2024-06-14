from dataclasses import dataclass
from typing import Optional


@dataclass
class WorkerStatus:
    running: bool
    error: Optional[Exception] = None


@dataclass(frozen=True)
class Worker:
    # TODO: fill in this class in the next PR.
    # metadata: WorkerMetadata
    # actor: ActorHandle
    pass
