from typing import Any, List, Union, Dict


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
    """

    stats_cls_identifier = "item_series"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.items: List[Any] = []

    def get_state(self) -> Dict[str, Any]:
        state = super().get_state()
        state["items"] = self.items
        return state

    def set_state(self, state: Dict[str, Any]) -> None:
        super().set_state(state)
        self.items = state["items"]

    def push(self, item: Any) -> None:
        """Pushes a item into this Stats object."""
        self.items.append(item)

    def reduce(self, compile: bool = True) -> Union[Any, List[Any]]:
        """Reduces the internal values list according to the constructor settings.

        The internal values list is set to an empty list.
        """
        items = self.items

        if self._clear_on_reduce:
            self.items = []
        else:
            self.items = self.items

        return items[0] if compile else items

    def __len__(self) -> int:
        """Returns the length of the internal values list."""
        return len(self.items)

    @staticmethod
    def merge(root_stats, incoming_stats: List["ItemSeriesStats"]) -> "ItemSeriesStats":
        """Merges ItemSeriesStats objects.

        If `root_stats` is None, we use the first incoming ItemSeriesStats object as the new base ItemSeriesStats object.
        If `root_stats` is not None, we merge all incoming ItemSeriesStats objects into the base ItemSeriesStats object.

        Args:
            root_stats: The base ItemSeriesStats object to merge into.
            incoming_stats: The list of ItemSeriesStats objects to merge.

        Returns:
            The merged ItemSeriesStats object.
        """
        if root_stats is None:
            # This should happen the first time we reduce this stat to the root logger
            root_stats = incoming_stats[0].similar_to(incoming_stats[0])
            root_stats._is_root_stats = True

        assert root_stats._is_root_stats, "Stats should only be merged at root level"

        new_values = []
        for s in [root_stats, *incoming_stats]:
            # Make sure that all incoming state have the same type.
            if not isinstance(s, ItemSeriesStats):
                raise ValueError(
                    f"All incoming stats must be of type {ItemSeriesStats}"
                )
            new_values.extend(s.values)

        root_stats._set_buffer(new_values)
        root_stats._has_new_values = True

        return root_stats

    def __repr__(self) -> str:
        return f"ItemSeriesStats(window={self._window}; len={len(self)})"
