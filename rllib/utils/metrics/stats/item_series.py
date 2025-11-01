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
        if self._window and self.is_leaf:
            # Window always counts at leafs only (or non-root stats)
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
        # Root stats objects that are not leaf stats (i.e., aggregated from other components)
        # should not be pushed to
        if not self.is_leaf:
            raise ValueError(
                "Cannot push values to non-leaf stats objects that are aggregated from other components. "
                "These stats are only updated through merge operations. "
                "Use leaf stats (created via direct logging) for push operations."
            )
        self.items.append(item)
        if self._window and len(self.items) > self._window:
            self.items.popleft()

    def reduce(self, compile: bool = True) -> Union[Any, "ItemSeriesStats"]:
        """Reduces the internal values list according to the constructor settings.

        Args:
            compile: Argument is ignored for ItemSeriesStats.

        Returns:
            The reduced value (can be of any type, depending on the input values and
            reduction method).
        """
        items = self.items
        self._set_items([])

        if compile:
            return items

        return_stats = self.clone(self)
        return_stats._set_items(items)
        return return_stats

    def __len__(self) -> int:
        """Returns the length of the internal items list."""
        return len(self.items)

    def peek(self, compile: bool = True, latest_merged_only: bool = False) -> List[Any]:
        """Returns the internal items list.

        This does not alter the internal items list.

        Args:
            compile: Argument is ignored for ItemSeriesStats.
            latest_merged_only: If True, only considers the latest merged values.
                This parameter only works on root stats objects.
                When enabled, peek() will only use the items from the most recent merge operation.

        Returns:
            The internal items list.
        """
        # Check latest_merged_only validity
        if latest_merged_only and not self.is_root:
            raise ValueError(
                "latest_merged_only can only be used on root stats objects "
                "(is_root=True)"
            )

        # If latest_merged_only is True, use only the latest merged items
        if latest_merged_only:
            if self.latest_merged is None:
                # No merged items yet, return empty list
                return []
            # Use only the latest merged items
            return self.latest_merged
        else:
            # Normal peek behavior
            return self.items

    def merge(self, incoming_stats: List["ItemSeriesStats"]):
        """Merges ItemSeriesStats objects.

        Args:
            incoming_stats: The list of ItemSeriesStats objects to merge.

        Returns:
            The merged ItemSeriesStats object.
        """
        assert self.is_root, "ItemSeriesStats should only be merged at root level"

        new_items = [s.items for s in incoming_stats]
        new_items = list(chain.from_iterable(new_items))
        all_items = list(self.items) + new_items
        self.items = all_items

        # Track merged values for latest_merged_only peek functionality
        if self.is_root:
            # Store the items that were merged in this operation (from incoming_stats only)
            self.latest_merged = new_items

    @staticmethod
    def _get_init_args(stats_object=None, state=None) -> Dict[str, Any]:
        """Returns the initialization arguments for this Stats object."""
        super_args = StatsBase._get_init_args(stats_object=stats_object, state=state)
        if state is not None:
            return {
                **super_args,
                "window": state["window"],
            }
        elif stats_object is not None:
            return {
                **super_args,
                "window": stats_object._window,
            }
        return super_args

    def __repr__(self) -> str:
        return f"ItemSeriesStats(window={self._window}; len={len(self)})"
