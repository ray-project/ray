from dataclasses import dataclass, field
from typing import List, Type

from ray.data._internal.issue_detection.detectors import (
    HangingExecutionIssueDetector,
    HangingExecutionIssueDetectorConfig,
    HighMemoryIssueDetector,
    HighMemoryIssueDetectorConfig,
)
from ray.data._internal.issue_detection.issue_detector import IssueDetector


@dataclass
class IssueDetectorsConfiguration:
    hanging_detector_config: HangingExecutionIssueDetectorConfig = field(
        default=HangingExecutionIssueDetectorConfig
    )
    high_memory_detector_config: HighMemoryIssueDetectorConfig = field(
        default=HighMemoryIssueDetectorConfig
    )
    detectors: List[Type[IssueDetector]] = field(
        default_factory=lambda: [HangingExecutionIssueDetector, HighMemoryIssueDetector]
    )
