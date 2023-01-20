from typing import List


class Operator:
    """Abstract class for operators.

    Operators are stateful and non-serializable; they live on the driver side of the
    Dataset only.
    """

    def __init__(self, name: str, input_dependencies: List["Operator"]):
        self._name = name
        self._input_dependencies = input_dependencies
        for x in input_dependencies:
            assert isinstance(x, Operator), x

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

    def __reduce__(self):
        raise ValueError("Operator is not serializable.")

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


class LogicalOperator(Operator):
    """Abstract class for logical operators.

    A logical operator describes transformation, and later is converted into
    physical operator.
    """

    def __init__(self, name: str, input_dependencies: List["LogicalOperator"]):
        super().__init__(name, input_dependencies)
        for x in input_dependencies:
            assert isinstance(x, LogicalOperator), x

    def get_physical_dag(self):
        """Get the corresponding DAG of physical operators.

        Returns:
            DAG of physical operators as List[PhysicalOperator].
        """
        raise NotImplementedError


class Rule:
    """Abstract class for optimization rule."""

    def apply(dag: Operator) -> Operator:
        """Apply the optimization rule to the DAG of operators."""
        raise NotImplementedError


class Optimizer:
    """Abstract class for optimizers.

    An optimizers transforms a DAG of operators with a list of predefined rules.
    """

    @property
    def rules(self) -> List[Rule]:
        """List of predefined rules for this optimizer."""
        raise NotImplementedError

    def optimize(self, dag: Operator) -> Operator:
        """Optimize operators with a list of rules."""
        for rule in self.rules:
            dag = rule.apply(dag)
        return dag
