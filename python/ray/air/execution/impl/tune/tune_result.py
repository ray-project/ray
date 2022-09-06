from typing import Dict, Union

from dataclasses import dataclass

from ray.air.execution.event import FutureResult


@dataclass
class TuneTrainingEvent(FutureResult):
    metrics: Dict


@dataclass
class TuneRestoringEvent(FutureResult):
    pass


@dataclass
class TuneSavingEvent(FutureResult):
    dir_or_data: Union[str, Dict]
