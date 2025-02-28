from typing import List, Optional
from dataclasses import dataclass


@dataclass
class Metric:
    id: str
    name: str
    current_value: Optional[float]
    max_over_time: Optional[float]


@dataclass
class OperatorMetrics:
    id: str
    name: str
    state: str
    progress: int
    total: int
    metrics: List[Metric]


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
