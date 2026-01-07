from typing import Any, Dict, List, Union

from ray.rllib.utils.metrics.stats.base import StatsBase
from ray.rllib.utils.metrics.stats.utils import single_value_to_cpu
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class ItemStats(StatsBase):
    """A Stats object that tracks a single item.

    Note the follwing limitation: That, when calling `ItemStats.merge()`, we replace the current item.
    This is because there can only be a single item tracked by definition.

    This class will check if the logged item is a GPU tensor.
    If it is, it will be converted to CPU memory.

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

    def reduce(self, compile: bool = True) -> Union[Any, "ItemStats"]:
        item = self._item
        self._item = None

        item = single_value_to_cpu(item)

        if compile:
            return item

        return_stats = self.clone()
        return_stats._item = item
        return return_stats

    def push(self, item: Any) -> None:
        """Pushes a value into this Stats object.

        Args:
            item: The value to push. Can be of any type.
                GPU tensors are moved to CPU memory.

        Returns:
            None
        """
        # Put directly onto CPU memory. peek(), reduce() and merge() don't handle GPU tensors.
        self._item = single_value_to_cpu(item)

    def merge(self, incoming_stats: List["ItemStats"]) -> None:
        """Merges ItemStats objects.

        Args:
            incoming_stats: The list of ItemStats objects to merge.

        Returns:
            None. The merge operation modifies self in place.
        """
        assert (
            len(incoming_stats) == 1
        ), "ItemStats should only be merged with one other ItemStats object which replaces the current item"

        self._item = incoming_stats[0]._item

    def peek(
        self, compile: bool = True, latest_merged_only: bool = False
    ) -> Union[Any, List[Any]]:
        """Returns the internal item.

        This does not alter the internal item.

        Args:
            compile: If True, return the internal item directly.
                If False, return the internal item as a single-element list.
            latest_merged_only: This parameter is ignored for ItemStats.
                ItemStats tracks a single item, not a series of merged values.
                The current item is always returned regardless of this parameter.

        Returns:
            The internal item.
        """
        # ItemStats doesn't support latest_merged_only since it tracks a single item
        # Just return the current item regardless

        item = single_value_to_cpu(self._item)
        if compile:
            return item
        return [item]

    def __repr__(self) -> str:
        return f"ItemStats({self.peek()}"
