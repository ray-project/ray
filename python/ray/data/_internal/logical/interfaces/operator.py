from typing import Iterator, List


class Operator:
    """Abstract class for operators.

    Operators live on the driver side of the Dataset only.
    """

    def __init__(self, name: str, input_dependencies: List["Operator"]):
        self._name = name
        self._input_dependencies = input_dependencies
        self._output_dependencies = []
        for x in input_dependencies:
            assert isinstance(x, Operator), x
            x._output_dependencies.append(self)

    @property
    def name(self) -> str:
        return self._name

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

    def __repr__(self) -> str:
        if self.input_dependencies:
            out_str = ", ".join([str(x) for x in self.input_dependencies])
            out_str += " -> "
        else:
            out_str = ""
        out_str += f"{self.__class__.__name__}[{self._name}]"
        return out_str

    def __str__(self) -> str:
        return repr(self)
