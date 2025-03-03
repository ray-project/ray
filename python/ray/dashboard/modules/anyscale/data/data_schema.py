from typing import List, Dict, Optional
from enum import Enum
from dataclasses import dataclass


class OperatorState(str, Enum):
    UNKNOWN = "UNKNOWN"
    RUNNING = "RUNNING"
    FAILED = "FAILED"
    FINISHED = "FINISHED"


@dataclass
class Metric:
    name: str
    current_value: float
    max_over_time: float


@dataclass
class OperatorMetrics:
    id: str
    name: str
    state: OperatorState
    progress: int
    total: int
    metrics: Dict[str, Metric]


@dataclass
class DatasetMetrics:
    id: str
    name: str
    state: str
    job_id: str
    session_name: str
    progress: int
    total: int
    start_time: int
    end_time: Optional[int]
    operator_metrics: List[OperatorMetrics]


@dataclass
class DatasetResponse:
    datasets: List[DatasetMetrics]
