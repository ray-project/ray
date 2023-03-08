from typing import Any, List, TYPE_CHECKING

from ray.data._internal.logical.interfaces import LogicalOperator

if TYPE_CHECKING:
    import tensorflow as tf
    import torch


class FromItems(LogicalOperator):
    """Logical operator for `from_items`."""

    def __init__(
        self,
        items: List[Any],
        parallelism: int = -1,
        op_name: str = "FromItems",
    ):
        super().__init__(op_name, [])
        self._items = items
        self._parallelism = parallelism


class FromTF(FromItems):
    """Logical operator for `from_tf`."""

    def __init__(self, dataset: "tf.data.Dataset"):
        self._dataset = dataset
        super().__init__(list(dataset.as_numpy_iterator()), op_name="FromTF")


class FromTorch(FromItems):
    """Logical operator for `from_torch`."""

    def __init__(
        self,
        dataset: "torch.utils.data.Dataset",
    ):
        self._dataset = dataset
        super().__init__(list(dataset), op_name="FromTorch")
