from typing import Any, List, Union, Dict


from ray.rllib.utils.framework import try_import_torch
from ray.util.annotations import DeveloperAPI
from ray.rllib.utils.metrics.stats.base import StatsBase

torch, _ = try_import_torch()


@DeveloperAPI
class ItemStats(StatsBase):
    """A Stats object that tracks a single item.

    Use this if you want to track a single item that should not be reduced.
    An example would be to log the total loss.
    """

    stats_cls_identifier = "item"

    def __init__(self, *args, **kwargs):
        """Initializes a ItemStats instance."""
        super().__init__(*args, **kwargs)
        self._item = None

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
            self._item = None

        if compile:
            return item

        return [item]

    def push(self, item: Any) -> None:
        """Pushes a item into this Stats object."""
        self._item = item

    @staticmethod
    def merge(self, incoming_stats: List["ItemStats"]) -> None:
        """Merges ItemStats objects.

        Args:
            incoming_stats: The list of ItemStats objects to merge.

        Returns:
            The merged ItemStats object.
        """
        assert self._is_root_stats, "ItemStats should only be merged at root level"

        not_none_items = [s._item for s in incoming_stats if s._item is not None]

        self._set_values(not_none_items)

    def peek(self, compile: bool = True) -> Union[Any, List[Any]]:
        """Returns the internal item.

        This does not alter the internal item.

        Args:
            compile: If True, return the internal item directly.
                If False, return the internal item as a single-element list.

        Returns:
            The internal item.
        """
        if compile:
            return self._item
        return [self._item]

    def __repr__(self) -> str:
        return f"ValueStats({self.peek()}; clear_on_reduce={self._clear_on_reduce})"
