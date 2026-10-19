import logging
import time
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, List, Union

from ray.util.placement_group import PlacementGroup, remove_placement_group

if TYPE_CHECKING:
    from ray.util.tpu import SlicePlacementGroup

logger = logging.getLogger(__name__)


class PlacementGroupHandle(ABC):
    """Unified interface for placement groups in Ray Train.

    This abstract base class provides a common interface for both standard
    PlacementGroup and SlicePlacementGroup, allowing WorkerGroup to handle
    them uniformly without conditional logic.
    """

    @property
    @abstractmethod
    def placement_group(self) -> PlacementGroup:
        """The underlying PlacementGroup for worker scheduling."""
        ...

    @property
    def placement_groups(self) -> List[PlacementGroup]:
        """All underlying PlacementGroups for worker scheduling."""
        return [self.placement_group]

    @abstractmethod
    def wait(self, timeout_seconds: Union[float, int] = 30) -> bool:
        """Wait for the placement group to be ready within the specified time.
        Args:
            timeout_seconds: Timeout in seconds.
        Returns:
            True if the placement group is created. False otherwise.
        """
        ...

    @abstractmethod
    def shutdown(self) -> None:
        """Release all resources associated with this placement group.

        After calling this method, the placement group should no longer be used.
        """
        ...


class DefaultPlacementGroupHandle(PlacementGroupHandle):
    """Wrapper for standard PlacementGroup."""

    def __init__(self, pg: PlacementGroup):
        self._pg = pg

    @property
    def placement_group(self) -> PlacementGroup:
        return self._pg

    def wait(self, timeout_seconds: Union[float, int] = 30) -> bool:
        try:
            return self._pg.wait(timeout_seconds)
        except Exception:
            logger.warning(
                "Placement group wait failed; treating as not ready.",
                exc_info=True,
            )
            return False

    def shutdown(self) -> None:
        remove_placement_group(self._pg)


class SlicePlacementGroupHandle(PlacementGroupHandle):
    """Wrapper for SlicePlacementGroup that delegates to its underlying PlacementGroup(s)."""

    def __init__(self, spg: "SlicePlacementGroup"):
        self._spg = spg

    @property
    def placement_groups(self) -> List[PlacementGroup]:
        return self._spg.placement_groups

    @property
    def placement_group(self) -> PlacementGroup:
        pgs = self.placement_groups
        if len(pgs) > 1:
            raise ValueError(
                f"Cannot access singular `placement_group` when managing {len(pgs)} slices. "
                "Please use `placement_groups` instead."
            )
        return pgs[0]

    def wait(self, timeout_seconds: Union[float, int] = 30) -> bool:
        try:
            pgs = self.placement_groups
            if not pgs:
                return False
            deadline = time.monotonic() + timeout_seconds
            for pg in pgs:
                if not pg.wait(max(0.0, deadline - time.monotonic())):
                    return False
            return True
        except Exception:
            logger.warning(
                "Slice placement group wait failed; treating as not ready.",
                exc_info=True,
            )
            return False

    def shutdown(self) -> None:
        self._spg.shutdown()
