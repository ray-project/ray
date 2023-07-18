from .operator import Operator


class Plan:
    """Abstract class for logical/physical execution plans.

    This plan should hold an operator representing the plan DAG and any auxiliary data
    that's useful for plan optimization or execution.
    """

    @property
    def dag(self) -> Operator:
        raise NotImplementedError
