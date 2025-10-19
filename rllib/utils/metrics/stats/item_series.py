from typing import Any, List, Union, Dict, Optional
from itertools import chain
from collections import deque

from ray.util.annotations import DeveloperAPI
from ray.rllib.utils.metrics.stats.series import StatsBase


@DeveloperAPI
class ItemSeriesStats(StatsBase):
    """A Stats object that tracks a series of items.

    Use this if you want to track a series of items that should not be reduced.
    An example would be to log actions and translate them into a chart to visualize
    the distribution of actions outside of RLlib.

    This class should not handle GPU tensors.

    Note that at the root level, the internal item list can grow to `window * len(incoming_stats)`.
    """

    stats_cls_identifier = "item_series"

    def __init__(self, window: Optional[int] = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._window = window
        self.items: List[Any] = []

    def _set_items(self, new_items):
        # For stats with window, use a deque with maxlen=window.
        # This way, we never store more values than absolutely necessary.
        if self._window:
            self.items = deque(new_items, maxlen=self._window)
        # For infinite windows, use `new_values` as-is (a list).
        else:
            self.items = new_items

    def get_state(self) -> Dict[str, Any]:
        state = super().get_state()
        state["items"] = self.items
        state["window"] = self._window
        return state

    def set_state(self, state: Dict[str, Any]) -> None:
        super().set_state(state)
        self.items = state["items"]
        self._window = state["window"]

    def push(self, item: Any) -> None:
        """Pushes a item into this Stats object.

        This method does not handle GPU tensors.

        Args:
            item: The item to push. Can be of any type but data should be in CPU memory.
        """
        self.items.append(item)
        if self._window and len(self.items) > self._window:
            self.items.popleft()

    def reduce(self, compile: bool = True) -> Union[Any, "ItemSeriesStats"]:
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
            self._set_items([])
        else:
            self.items = self.items

        if compile:
            return items

        return_stats = self.similar_to(self)
        return_stats.items = items
        return return_stats

    def __len__(self) -> int:
        """Returns the length of the internal items list."""
        return len(self.items)

    def peek(self, compile: bool = True) -> List[Any]:
        """Returns the internal items list.

        This does not alter the internal items list.

        Args:
            compile: Argument is ignored for ItemSeriesStats.

        Returns:
            The internal items list.
        """
        return self.items

    def merge(self, incoming_stats: List["ItemSeriesStats"]) -> None:
        """Merges ItemSeriesStats objects.

        Args:
            incoming_stats: The list of ItemSeriesStats objects to merge.

        Returns:
            The merged ItemSeriesStats object.
        """
        assert (
            self._is_root_stats
        ), "ItemSeriesStats should only be merged at root level"

        all_items = [s.items for s in incoming_stats]
        all_items = list(chain.from_iterable(all_items))
        self._set_items(all_items)

    def __repr__(self) -> str:
        return f"ItemSeriesStats(window={self._window}; len={len(self)})"
