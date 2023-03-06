from typing import Any, List

from ray.data._internal.logical.interfaces import LogicalOperator


class FromItems(LogicalOperator):
    """Logical operator for `from_items`."""

    def __init__(
        self,
        items: List[Any],
        parallelism: int = -1,
    ):
        super().__init__("FromItems", [])
        self._items = items
        self._parallelism = parallelism
