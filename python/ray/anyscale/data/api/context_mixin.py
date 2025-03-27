from dataclasses import dataclass, field
from typing import Optional, TYPE_CHECKING

from ray.anyscale.data.checkpoint.interfaces import CheckpointConfig
from ray.anyscale.data.issue_detection.issue_detector_configuration import (
    IssueDetectorsConfiguration,
)

if TYPE_CHECKING:
    from ray.anyscale.data.issue_detection.issue_detector_configuration import (
        IssueDetectorsConfiguration,
    )


def _issue_detectors_config_factory() -> "IssueDetectorsConfiguration":
    # Lazily import to avoid circular dependencies.
    from ray.anyscale.data.issue_detection.issue_detector_configuration import (
        IssueDetectorsConfiguration,
    )

    return IssueDetectorsConfiguration()


@dataclass
class DataContextMixin:
    """A mix-in class that allows adding Anyscale proprietary
    attributes and methods to :class:`~ray.data.DataContext`."""

    # Configuration for Ray Data checkpointing.
    # If None, checkpointing is disabled.
    checkpoint_config: Optional["CheckpointConfig"] = None

    # Configuration for Issue Detection
    issue_detectors_config: "IssueDetectorsConfiguration" = field(
        default_factory=_issue_detectors_config_factory
    )
