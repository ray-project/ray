from typing import Any, List, Union, Dict, Optional


from ray.rllib.utils.framework import try_import_torch
from ray.util.annotations import DeveloperAPI
from ray.rllib.utils.metrics.stats.series import StatsBase

torch, _ = try_import_torch()


@DeveloperAPI
class ItemSeriesStats(StatsBase):
    """A Stats object that tracks a series of items.

    Use this if you want to track a series of items that should not be reduced.
    An example would be to log actions and translate them into a chart to visualize
    the distribution of actions outside of RLlib.

    Note that at the root level, the internal item list can grow to `window * len(incoming_stats)`.
    """

    stats_cls_identifier = "item_series"

    def __init__(
        self, clear_on_reduce: bool = False, window: Optional[int] = 1, **kwargs
    ):
        super().__init__(**kwargs)
        self._window = window
        self._clear_on_reduce = clear_on_reduce
        self.items: List[Any] = []

    def get_state(self) -> Dict[str, Any]:
        state = super().get_state()
        state["items"] = self.items
        state["clear_on_reduce"] = self._clear_on_reduce
        state["window"] = self._window
        return state

    def set_state(self, state: Dict[str, Any]) -> None:
        super().set_state(state)
        self.items = state["items"]
        self._clear_on_reduce = state["clear_on_reduce"]
        self._window = state["window"]

    def push(self, item: Any) -> None:
        """Pushes a item into this Stats object."""
        self.items.append(item)
        if len(self.items) > self._window:
            self.items.popleft()

    def reduce(self, compile: bool = True) -> Union[Any, List[Any]]:
        """Reduces the internal values list according to the constructor settings.

        Args:
            compile: Argument is ignored for ItemSeriesStats.
            clear_on_reduce: If True, the internal values list is set to an empty list.

        Returns:
            The reduced value (can be of any type, depending on the input values and
            reduction method).
        """
        items = self.items

        if self._clear_on_reduce:
            self.items = []
        else:
            self.items = self.items

        return items

    def __len__(self) -> int:
        """Returns the length of the internal values list."""
        return len(self.items)

    @staticmethod
    def merge(self, incoming_stats: List["ItemSeriesStats"]) -> None:
        """Merges ItemSeriesStats objects.

        If `root_stats` is None, we use the first incoming ItemSeriesStats object as the new base ItemSeriesStats object.
        If `root_stats` is not None, we merge all incoming ItemSeriesStats objects into the base ItemSeriesStats object.

        Args:
            root_stats: The base ItemSeriesStats object to merge into.
            incoming_stats: The list of ItemSeriesStats objects to merge.

        Returns:
            The merged ItemSeriesStats object.
        """
        assert (
            self._is_root_stats
        ), "ItemSeriesStats should only be merged at root level"

        new_values = []
        for s in [self, *incoming_stats]:
            new_values.extend(s.items)

        # Note: At the root level, the internal item list can grow to `window * len(incoming_stats)`
        self._set_items(new_values)
        self._has_new_values = True

    def __repr__(self) -> str:
        return f"ItemSeriesStats(window={self._window}; len={len(self)})"
