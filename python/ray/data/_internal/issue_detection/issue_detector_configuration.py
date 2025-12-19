from dataclasses import dataclass, field
from typing import List, Type

from ray.data._internal.issue_detection.detectors import (
    HangingExecutionIssueDetector,
    HangingExecutionIssueDetectorConfig,
    HashShuffleAggregatorIssueDetector,
    HashShuffleAggregatorIssueDetectorConfig,
    HighMemoryIssueDetector,
    HighMemoryIssueDetectorConfig,
)
from ray.data._internal.issue_detection.issue_detector import IssueDetector


@dataclass
class IssueDetectorsConfiguration:
    hanging_detector_config: HangingExecutionIssueDetectorConfig = field(
        default_factory=HangingExecutionIssueDetectorConfig
    )
    hash_shuffle_detector_config: HashShuffleAggregatorIssueDetectorConfig = field(
        default_factory=HashShuffleAggregatorIssueDetectorConfig
    )
    high_memory_detector_config: HighMemoryIssueDetectorConfig = field(
        default_factory=HighMemoryIssueDetectorConfig
    )
    detectors: List[Type[IssueDetector]] = field(
        default_factory=lambda: [
            HangingExecutionIssueDetector,
            HashShuffleAggregatorIssueDetector,
            HighMemoryIssueDetector,
        ]
    )
