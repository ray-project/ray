from typing import List

from .plan import Plan


class Rule:
    """Abstract class for optimization rule."""

    def apply(self, plan: Plan) -> Plan:
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
