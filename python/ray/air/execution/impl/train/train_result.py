from dataclasses import dataclass

from ray.air._internal.checkpoint_manager import _TrackedCheckpoint
from ray.air.execution.result import ExecutionResult
from ray.train._internal.session import TrainingResult


@dataclass
class TrainSetupIPResult(ExecutionResult):
    ip: str


@dataclass
class TrainInitResult(ExecutionResult):
    pass


@dataclass
class TrainStartResult(ExecutionResult):
    pass


@dataclass
class TrainTrainingResult(ExecutionResult):
    result: TrainingResult


@dataclass
class TrainSavingResult(ExecutionResult):
    tracked_checkpoint: _TrackedCheckpoint
