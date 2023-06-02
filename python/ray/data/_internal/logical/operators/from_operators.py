import abc
from typing import TYPE_CHECKING, Any, Generic, List, TypeVar, Union

from ray.data._internal.execution.interfaces import RefBundle
from ray.data._internal.logical.interfaces import LogicalOperator

T = TypeVar("T")

if TYPE_CHECKING:
    import numpy as np
    import pandas as pd
    import pyarrow as pa

    ArrowTable = Union["pa.Table", bytes]


class AbstractFrom(LogicalOperator, Generic[T], metaclass=abc.ABCMeta):
    """Abstract logical operator for `from_*`."""

    def __init__(self, input_data: List[RefBundle]):
        super().__init__(self.op_name(), [])
        self._input_data = input_data

    @abc.abstractmethod
    def op_name(self) -> str:
        """Returns the name of the operator."""
        pass

    @property
    def input_data(self) -> List[RefBundle]:
        return self._input_data


class FromItems(AbstractFrom[Any]):
    """Logical operator for `from_items`."""

    def op_name(self) -> str:
        return "FromItems"


class FromNumpy(AbstractFrom["np.ndarray"]):
    """Logical operator for `from_numpy`."""

    def op_name(self) -> str:
        return "FromNumpy"


class FromArrow(AbstractFrom["ArrowTable"]):
    """Logical operator for `from_arrow`."""

    def op_name(self) -> str:
        return "FromArrow"


class FromPandas(AbstractFrom["pd.DataFrame"]):
    """Logical operator for `from_pandas`."""

    def op_name(self) -> str:
        return "FromPandas"
