from typing import Any, Dict, List

from ray.data._internal.logical.operators.map_operator import AbstractMap
from ray.data.datasource.datasource import Datasource


class FromItems(AbstractMap):
    """Logical operator for from_items."""

    def __init__(
        self,
        items: List[Any],
        parallelism: int = -1,
    ):
        super().__init__("FromItems", None, None)
        self._items = items
        self._parallelism = parallelism
