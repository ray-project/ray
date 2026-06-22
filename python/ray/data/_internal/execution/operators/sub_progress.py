import typing
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple

from ray.data._internal.progress.utils import DEFAULT_PROGRESS_BAR_MAX_NAME_LENGTH

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

    @abstractmethod
    def get_sub_progress_updaters(self) -> Optional[Dict[str, "SubProgressUpdater"]]:
        """Returns driver-side helpers for mutating sub-progress metrics."""
        ...

    @classmethod
    def _create_sub_progress_state(
        cls, names: List[str]
    ) -> Tuple[Dict[str, "ProgressMetrics"], Dict[str, "SubProgressUpdater"]]:
        from ray.data._internal.progress.base_progress import (
            ProgressMetrics,
            SubProgressUpdater,
        )

        metrics = {
            name: ProgressMetrics(name=name, total=None, completed=0) for name in names
        }
        updaters = {
            name: SubProgressUpdater(
                metrics,
                name=name,
                max_name_length=DEFAULT_PROGRESS_BAR_MAX_NAME_LENGTH,
            )
            for name in names
        }
        return metrics, updaters
