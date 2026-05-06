from abc import ABC, abstractmethod
from typing import Callable, Iterator, List


class Operator(ABC):
    """Abstract class for operators.

    Operators live on the driver side of the Dataset only.
    """

    name: str
    input_dependencies: List["Operator"]

    @property
    def dag_str(self) -> str:
        """String representation of the whole DAG."""
        if self.input_dependencies:
            out_str = ", ".join([x.dag_str for x in self.input_dependencies])
            out_str += " -> "
        else:
            out_str = ""
        out_str += f"{self.__class__.__name__}[{self.name}]"
        return out_str

    def post_order_iter(self) -> Iterator["Operator"]:
        """Depth-first traversal of this operator and its input dependencies."""
        for op in self.input_dependencies:
            yield from op.post_order_iter()
        yield self

    @abstractmethod
    def _apply_transform(
        self, transform: Callable[["Operator"], "Operator"]
    ) -> "Operator":
        """Recursively applies transformation (in post-order) to the operators DAG

        NOTE: This operation should be opting in to avoid in-place modifications,
              instead creating new operations whenever any operator needs to be
              updated.
        """
        ...

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}[{self.name}]"

    def __str__(self) -> str:
        return repr(self)
