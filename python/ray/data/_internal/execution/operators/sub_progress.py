import typing
from abc import ABC, abstractmethod
from typing import Dict, Optional

if typing.TYPE_CHECKING:
    from ray.data._internal.progress.base_progress import (
        ProgressMetrics,
        SubProgressUpdater,
    )


class SubProgressMixin(ABC):
    """Abstract class for operators that support driver-side sub-progress tracking."""

    @abstractmethod
    def get_sub_progress_metrics(self) -> Optional[Dict[str, "ProgressMetrics"]]:
        """
        Returns sub-progress metrics keyed by sub-progress name.
        """
        ...

    def get_sub_progress_updaters(self) -> Optional[Dict[str, "SubProgressUpdater"]]:
        """Returns driver-side helpers for mutating sub-progress metrics."""
        return None
