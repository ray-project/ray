from dataclasses import dataclass, field
from typing import List, Type

from ray.anyscale.data.issue_detection.detectors.hanging_detector import (
    HangingExecutionIssueDetector,
    HangingExecutionIssueDetectorConfig,
)
from ray.anyscale.data.issue_detection.issue_detector import IssueDetector

DEFAULT_DETECTION_TIME_INTERVAL_S = 30.0


@dataclass
class IssueDetectorsConfiguration:
    hanging_detector_config: HangingExecutionIssueDetectorConfig = field(
        default=HangingExecutionIssueDetectorConfig
    )
    detectors: List[Type[IssueDetector]] = field(
        default_factory=lambda: [HangingExecutionIssueDetector]
    )
    detection_time_interval_s: float = field(default=DEFAULT_DETECTION_TIME_INTERVAL_S)
