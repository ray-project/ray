from dataclasses import dataclass

from ray.air._internal.checkpoint_manager import _TrackedCheckpoint
from ray.air.execution.event import FutureResult
from ray.train._internal.session import TrainingResult


@dataclass
class TrainSetupIPEvent(FutureResult):
    ip: str


@dataclass
class TrainInitEvent(FutureResult):
    pass


@dataclass
class TrainStartEvent(FutureResult):
    pass


@dataclass
class TrainTrainingEvent(FutureResult):
    result: TrainingResult


@dataclass
class TrainSavingEvent(FutureResult):
    tracked_checkpoint: _TrackedCheckpoint
