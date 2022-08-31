from typing import Dict

from dataclasses import dataclass

from ray.air._internal.checkpoint_manager import _TrackedCheckpoint
from ray.air.execution.result import ExecutionResult


@dataclass
class TrainTrainingResult(ExecutionResult):
    metrics: Dict


@dataclass
class TrainSavingResult(ExecutionResult):
    tracked_checkpoint: _TrackedCheckpoint
