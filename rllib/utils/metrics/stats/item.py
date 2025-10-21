from typing import Any, List, Union, Dict


from ray.util.annotations import DeveloperAPI
from ray.rllib.utils.metrics.stats.base import StatsBase
from ray.rllib.utils.metrics.stats.utils import single_value_to_cpu


@DeveloperAPI
class ItemStats(StatsBase):
    """A Stats object that tracks a single item.

    Note the follwing limitation: That, when calling `ItemStats.merge()`, we replace the current item.
    This is because there can only be a single item tracked by definition.

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

        if compile:
            return item

        return_stats = self.clone(self)
        return_stats._item = item
        return return_stats

    def push(self, item: Any) -> None:
        # Put directly onto CPU memory. peek(), reduce() and merge() don't handle GPU tensors.
        self._item = single_value_to_cpu(item)

    def merge(self, incoming_stats: List["ItemStats"]):
        """Merges ItemStats objects.

        Args:
            incoming_stats: The list of ItemStats objects to merge.

        Returns:
            The merged ItemStats object.
        """
        assert (
            len(incoming_stats) == 1
        ), "ItemStats should only be merged with one other ItemStats object"

        self._item = incoming_stats[0]._item

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
        return f"ItemStats({self.peek()}"
