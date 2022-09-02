from typing import Dict, Union

from dataclasses import dataclass

from ray.air.execution.result import ExecutionResult


@dataclass
class TuneTrainingResult(ExecutionResult):
    metrics: Dict


@dataclass
class TuneRestoringResult(ExecutionResult):
    pass


@dataclass
class TuneSavingResult(ExecutionResult):
    dir_or_data: Union[str, Dict]
