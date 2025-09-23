

import copy
import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Union

from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalOperatorSupportsProjectionPushdown,
    LogicalPlan,
    Rule,
)
from ray.data._internal.logical.operators.all_to_all_operator import Repartition
from ray.data._internal.logical.operators.map_operator import Project
from ray.data._internal.logical.operators.read_operator import Read
from ray.data.expressions import Expr

logger = logging.getLogger(__name__)


class ShuffleFusion(Rule):
    """Optimization rule that fuses shuffle operations together.

    If there are redundant Shuffle operators, it removes the `Project` operator from
    the graph.
    """

    def apply(self, plan: LogicalPlan) -> LogicalPlan:
        dag = plan.dag
        new_dag = dag._apply_transform(self.fuse_with_upstream)

        return LogicalPlan(new_dag, plan.context) if dag is not new_dag else plan

    @classmethod
    def fuse_with_upstream(cls, op: LogicalOperator) -> LogicalOperator:
        child_op = op
        parent_ops = op.input_dependencies
        if len(parent_ops) == 1:
            parent_op = parent_ops[0]
            if isinstance(parent_op, Repartition) and isinstance(child_op, Repartition):
                # TODO(justin): make this whole process a helper function, I see it a lot.

                # 1. Create a new operator
                twin_op = copy.copy(child_op)

                # 2. Rewire parent.inputs (aka grandparent) -> twin_op
                grandparent_ops = parent_op.input_dependencies
                twin_op.input_dependencies = grandparent_ops

                # 3. Rewire grandparent -> twin_op
                assert len(grandparent_ops) == 1
                grandparent_op = grandparent_ops[0]
                grandparent_op.output_dependencies = [twin_op]
                return twin_op
            else:
                # TODO(justin): case for other rules
                ...
                

        return op

