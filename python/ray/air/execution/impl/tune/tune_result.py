from typing import Dict

from dataclasses import dataclass

from ray.air._internal.checkpoint_manager import _TrackedCheckpoint
from ray.air.execution.result import ExecutionResult


@dataclass
class TuneTrainingResult(ExecutionResult):
    metrics: Dict


@dataclass
class TuneSavingResult(ExecutionResult):
    tracked_checkpoint: _TrackedCheckpoint
