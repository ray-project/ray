import abc
from typing import List, Type

from .plan import Plan


class Rule:
    """Abstract class for optimization rule."""

    def __init__(self):
        self._enabled = True

    def name(self) -> str:
        return self.__class__.__name__

    def enabled(self) -> bool:
        return self._enabled

    def enable(self):
        self._enabled = True

    def disable(self):
        self._enabled = False

    def __call__(self, plan):
        if not self.enabled():
            return plan
        return self.apply(plan)

    def apply(self, plan: Plan) -> Plan:
        """Apply the optimization rule to the execution plan."""
        raise NotImplementedError

    @classmethod
    def dependencies(cls) -> List[Type["Rule"]]:
        """List of rules that must be applied before this rule."""
        return []

    @classmethod
    def dependents(cls) -> List[Type["Rule"]]:
        """List of rules that must be applied after this rule."""
        return []


class Optimizer:
    """Abstract class for optimizers.

    An optimizers transforms a DAG of operators with a list of predefined rules.
    """

    @property
    def rules(self) -> List[Rule]:
        """List of predefined rules for this optimizer."""
        raise NotImplementedError

    @abc.abstractmethod
    def active_rules(self) -> List[Rule]:
        raise NotImplementedError

    def optimize(self, plan: Plan) -> Plan:
        """Optimize operators with a list of rules."""
        # Apply rules until the plan is not changed
        previous_plan = plan
        while True:
            for rule in self.active_rules():
                plan = rule(plan)
            # TODO: Eventually we should implement proper equality.
            # Using str to check equality seems brittle
            if plan.dag.dag_str == previous_plan.dag.dag_str:
                break
            previous_plan = plan
        return plan
