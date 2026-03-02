import copy
from typing import Callable, Iterator, List


class Operator:
    """Abstract class for operators.

    Operators live on the driver side of the Dataset only.
    """

    def __init__(
        self,
        name: str,
        input_dependencies: List["Operator"],
    ):
        self._name = name
        self._input_dependencies = input_dependencies

    @property
    def name(self) -> str:
        return self._name

    @property
    def dag_str(self) -> str:
        """String representation of the whole DAG."""
        if self.input_dependencies:
            out_str = ", ".join([x.dag_str for x in self.input_dependencies])
            out_str += " -> "
        else:
            out_str = ""
        out_str += f"{self.__class__.__name__}[{self._name}]"
        return out_str

    @property
    def input_dependencies(self) -> List["Operator"]:
        """List of operators that provide inputs for this operator."""
        assert hasattr(
            self, "_input_dependencies"
        ), "Operator.__init__() was not called."
        return self._input_dependencies

    def post_order_iter(self) -> Iterator["Operator"]:
        """Depth-first traversal of this operator and its input dependencies."""
        for op in self.input_dependencies:
            yield from op.post_order_iter()
        yield self

    def _apply_transform(
        self, transform: Callable[["Operator"], "Operator"]
    ) -> "Operator":
        """Recursively applies transformation (in post-order) to the operators DAG

        NOTE: This operation should be opting in to avoid in-place modifications,
              instead creating new operations whenever any operator needs to be
              updated.
        """

        transformed_input_ops = []
        has_changes = False

        for input_op in self.input_dependencies:
            transformed_input_op = input_op._apply_transform(transform)
            transformed_input_ops.append(transformed_input_op)
            if transformed_input_op is not input_op:
                has_changes = True

        if has_changes:
            # Make a shallow copy to avoid modifying operators in-place
            target = copy.copy(self)

            target._input_dependencies = transformed_input_ops
        else:
            target = self

        return transform(target)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}[{self._name}]"

    def __str__(self) -> str:
        return repr(self)
