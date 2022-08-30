from dataclasses import dataclass

from typing import Dict

from ray.air._internal.checkpoint_manager import _TrackedCheckpoint
from ray.air.execution.resources.request import ResourceRequest


@dataclass
class ExecutionResult:
    pass


@dataclass
class ExecutionException(ExecutionResult):
    exception: Exception


@dataclass
class ResourceResult(ExecutionResult):
    resource_request: ResourceRequest


@dataclass
class TrainingResult(ExecutionResult):
    metrics: Dict


@dataclass
class SavingResult(ExecutionResult):
    tracked_checkpoint: _TrackedCheckpoint
    metrics: Dict
