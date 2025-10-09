from typing import Any, List, Union, Dict


from ray.rllib.utils.framework import try_import_torch
from ray.util.annotations import DeveloperAPI
from ray.rllib.utils.metrics.stats.stats_base import StatsBase
import numpy as np

torch, _ = try_import_torch()


@DeveloperAPI
class ItemStats(StatsBase):
    """A Stats object that tracks a single item.

    Use this if you want to track a single item that should not be reduced.
    An example would be to log the total loss.
    """

    stats_cls_identifier = "item"

    def __init__(self, **kwargs):
        """Initializes a ItemStats instance."""
        self._item = np.nan
        super().__init__(**kwargs)

    def get_state(self) -> Dict[str, Any]:
        state = super().get_state()
        state["item"] = self._item
        return state

    def set_state(self, state: Dict[str, Any]) -> None:
        super().set_state(state)
        self._item = state["item"]

    def __len__(self) -> int:
        return 1

    def reduce(self, compile: bool = True) -> Union[Any, List[Any]]:
        """Returns the internal item directly or as a list.

        If `clear_on_reduce` is True, the internal item is set to None.

        Args:
            compile: If True, the value is returned directly.
                If False, the value is returned as a single-element list.

        Returns:
            The value (can be of any type, depending on the input value).
        """
        item = self._item

        if self._clear_on_reduce:
            self._item = np.nan

        if compile:
            return item

        return [item]

    def push(self, item: Any) -> None:
        """Pushes a item into this Stats object."""
        self._item = item

    def merge(root_stats: "StatsBase", *stats: "StatsBase") -> None:
        """Merges Stats objects."""
        if root_stats is None:
            root_stats = stats[0].similar_to(stats[0])
            root_stats._is_root_stats = True

        assert root_stats._is_root_stats, "Stats should only be merged at root level"
        assert type(root_stats) is type(stats[0]) and isinstance(
            root_stats, ItemStats
        ), "All incoming stats must be of type ItemStats"

        root_stats._item = stats[0]._item

        return root_stats

    def peek(self, compile: bool = True) -> Union[Any, List[Any]]:
        """Returns the result of reducing.

        This does not alter the internal value.

        Args:
            compile: If True, the result is compiled into a single value.

        Returns:
            The result of reducing the internal values list.
        """

        if compile:
            return self._item
        return [self._item]

    def __repr__(self) -> str:
        return f"ValueStats({self.peek()}; clear_on_reduce={self._clear_on_reduce})"
