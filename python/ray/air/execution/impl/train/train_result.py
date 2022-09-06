from dataclasses import dataclass

from ray.air._internal.checkpoint_manager import _TrackedCheckpoint
from ray.air.execution.event import ExecutionEvent
from ray.train._internal.session import TrainingEvent


@dataclass
class TrainSetupIPEvent(ExecutionEvent):
    ip: str


@dataclass
class TrainInitEvent(ExecutionEvent):
    pass


@dataclass
class TrainStartEvent(ExecutionEvent):
    pass


@dataclass
class TrainTrainingEvent(ExecutionEvent):
    result: TrainingEvent


@dataclass
class TrainSavingEvent(ExecutionEvent):
    tracked_checkpoint: _TrackedCheckpoint
