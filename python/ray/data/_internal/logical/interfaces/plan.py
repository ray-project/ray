from typing import TYPE_CHECKING

from .operator import Operator

if TYPE_CHECKING:
    from ray.data import DataContext


class Plan:
    """Abstract class for logical/physical execution plans.

    This plan should hold an operator representing the plan DAG and any auxiliary data
    that's useful for plan optimization or execution.
    """

    def __init__(self, context: "DataContext"):
        self._context = context

    @property
    def dag(self) -> Operator:
        raise NotImplementedError

    @property
    def context(self) -> "DataContext":
        return self._context
