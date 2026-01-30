from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Union

from ray.types import ObjectRef
from ray.util.placement_group import PlacementGroup, remove_placement_group

if TYPE_CHECKING:
    from ray.util.tpu import SlicePlacementGroup


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

    @abstractmethod
    def ready(self) -> ObjectRef:
        """Returns an ObjectRef to check if the placement group is ready.

        Compatible with ray.get() and ray.wait().
        """
        ...

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

    def ready(self) -> ObjectRef:
        return self._pg.ready()

    def wait(self, timeout_seconds: Union[float, int] = 30) -> bool:
        return self._pg.wait(timeout_seconds)

    def shutdown(self) -> None:
        remove_placement_group(self._pg)


class SlicePlacementGroupHandle(PlacementGroupHandle):
    """Wrapper for SlicePlacementGroup that delegates to its underlying PlacementGroup."""

    def __init__(self, spg: "SlicePlacementGroup"):
        self._spg = spg

    @property
    def placement_group(self) -> PlacementGroup:
        return self._spg.placement_group

    def ready(self) -> ObjectRef:
        return self._spg.placement_group.ready()

    def wait(self, timeout_seconds: Union[float, int] = 30) -> bool:
        return self._spg.placement_group.wait(timeout_seconds)

    def shutdown(self) -> None:
        self._spg.shutdown()
