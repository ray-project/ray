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
        self._output_dependencies = []

        self._wire_output_deps(input_dependencies)

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

    @property
    def output_dependencies(self) -> List["Operator"]:
        """List of operators that consume outputs from this operator."""
        assert hasattr(
            self, "_output_dependencies"
        ), "Operator.__init__() was not called."
        return self._output_dependencies

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
        new_ops = []

        for input_op in self.input_dependencies:
            transformed_input_op = input_op._apply_transform(transform)
            transformed_input_ops.append(transformed_input_op)
            # Keep track of new input ops
            if transformed_input_op is not input_op:
                new_ops.append(transformed_input_op)

        if new_ops:
            # NOTE: Only newly created ops need to have output deps
            #       wired in
            self._wire_output_deps(new_ops)
            self._input_dependencies = transformed_input_ops

        return transform(self)

    def _wire_output_deps(self, input_dependencies: List["Operator"]):
        for x in input_dependencies:
            assert isinstance(x, Operator), x
            x._output_dependencies.append(self)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}[{self._name}]"

    def __str__(self) -> str:
        return repr(self)
