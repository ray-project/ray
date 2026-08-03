"""Derive ``ListFiles`` pushdown state from its consuming ``ReadFiles`` scanner.

This is the single source of truth for the read constraints a ``ListFiles``
applies while listing. It runs once, after every other logical rule, so no rule
has to remember to keep the two operators in sync.
"""
from dataclasses import replace
from typing import List

from ray.data._internal.datasource_v2.logical_optimizers import (
    derive_list_files_pushdown,
)
from ray.data._internal.logical.interfaces import LogicalOperator, LogicalPlan, Rule
from ray.data._internal.logical.operators.read_operator import ListFiles, ReadFiles

__all__ = [
    "DeriveListFilesPushdown",
]


class DeriveListFilesPushdown(Rule):
    """Recompute every ``ListFiles``' pushed-down read constraints from scratch.

    A metadata-aware indexer (e.g. the footer-based Parquet indexer) reads
    ``predicate`` / ``projected_columns`` / ``limit`` off ``ListFiles`` at
    planning time to prune row groups by their statistics, size only projected
    columns, and stop listing early. Those constraints are only sound if they
    are no stronger than what the downstream ``ReadFiles`` actually applies --
    a predicate on ``ListFiles`` that the reader does not evaluate prunes row
    groups nobody re-checks, silently dropping rows.

    Rather than have each pushdown rule mirror its own state onto ``ListFiles``
    (an invariant every future rule would have to re-establish by hand), this
    rule derives the state from the consuming ``ReadFiles`` scanner and
    overwrites whatever was there. Deriving is unconditional in both
    directions: a ``ListFiles`` whose consumer is not a ``ReadFiles`` -- e.g.
    after ``PushdownCountFiles`` rewrites the plan -- is reset to no
    constraints. So a rule that weakens or drops a scanner's predicate
    automatically weakens listing too, and the worst a future rule can cause is
    listing more than it needs to.

    Runs in ``LogicalOptimizer._post_optimize``, i.e. after the rule loop has
    reached a fixed point, so it observes each scanner's final state.
    """

    def apply(self, plan: LogicalPlan) -> LogicalPlan:  # pyrefly: ignore[bad-override]
        def transform(node: LogicalOperator) -> LogicalOperator:
            inputs = node.input_dependencies
            if not any(isinstance(input_op, ListFiles) for input_op in inputs):
                return node

            # ``ReadFiles`` is the only consumer that applies these constraints
            # downstream; for anything else they must be dropped.
            scanner = node.scanner if isinstance(node, ReadFiles) else None
            predicate, projected_columns, limit = derive_list_files_pushdown(scanner)

            new_inputs: List[LogicalOperator] = []
            changed = False
            for input_op in inputs:
                if isinstance(input_op, ListFiles) and (
                    input_op.predicate is not predicate
                    or input_op.projected_columns != projected_columns
                    or input_op.limit != limit
                ):
                    input_op = replace(
                        input_op,
                        predicate=predicate,
                        projected_columns=projected_columns,
                        limit=limit,
                    )
                    changed = True
                new_inputs.append(input_op)

            if not changed:
                return node
            return node._with_new_input_dependencies(new_inputs)

        dag = plan.dag._apply_transform(transform)

        # A bare ``ListFiles`` root has no consumer at all, so nothing above
        # applies its constraints either.
        if isinstance(dag, ListFiles) and (
            dag.predicate is not None
            or dag.projected_columns is not None
            or dag.limit is not None
        ):
            dag = replace(dag, predicate=None, projected_columns=None, limit=None)

        return LogicalPlan(dag=dag, context=plan.context)
