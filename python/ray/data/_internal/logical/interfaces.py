from typing import Dict, Iterator, List, TYPE_CHECKING

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces import PhysicalOperator


class Operator:
    """Abstract class for operators.

    Operators live on the driver side of the Datastream only.
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


class LogicalOperator(Operator):
    """Abstract class for logical operators.

    A logical operator describes transformation, and later is converted into
    physical operator.
    """

    def __init__(self, name: str, input_dependencies: List["LogicalOperator"]):
        super().__init__(name, input_dependencies)
        for x in input_dependencies:
            assert isinstance(x, LogicalOperator), x


class Plan:
    """Abstract class for logical/physical execution plans.

    This plan should hold an operator representing the plan DAG and any auxiliary data
    that's useful for plan optimization or execution.
    """

    @property
    def dag(self) -> Operator:
        raise NotImplementedError


class LogicalPlan(Plan):
    """The plan with a DAG of logical operators."""

    def __init__(self, dag: LogicalOperator):
        self._dag = dag

    @property
    def dag(self) -> LogicalOperator:
        """Get the DAG of logical operators."""
        return self._dag


class PhysicalPlan(Plan):
    """The plan with a DAG of physical operators."""

    def __init__(
        self, dag: "PhysicalOperator", op_map: Dict["PhysicalOperator", LogicalOperator]
    ):
        self._dag = dag
        self._op_map = op_map

    @property
    def dag(self) -> "PhysicalOperator":
        """Get the DAG of physical operators."""
        return self._dag

    @property
    def op_map(self) -> Dict["PhysicalOperator", LogicalOperator]:
        """
        Get a mapping from physical operators to their corresponding logical operator.
        """
        return self._op_map


class Rule:
    """Abstract class for optimization rule."""

    def apply(plan: Plan) -> Plan:
        """Apply the optimization rule to the execution plan."""
        raise NotImplementedError


class Optimizer:
    """Abstract class for optimizers.

    An optimizers transforms a DAG of operators with a list of predefined rules.
    """

    @property
    def rules(self) -> List[Rule]:
        """List of predefined rules for this optimizer."""
        raise NotImplementedError

    def optimize(self, plan: Plan) -> Plan:
        """Optimize operators with a list of rules."""
        for rule in self.rules:
            plan = rule.apply(plan)
        return plan
