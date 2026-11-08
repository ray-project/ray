"""Derive ``ListFiles`` pushdown state from its consuming ``ReadFiles`` scanner.

This is the single source of truth for the read constraints a ``ListFiles``
applies while listing. It runs once, after every other logical rule, so no rule
has to remember to keep the two operators in sync.
"""

from dataclasses import replace
from typing import TYPE_CHECKING, Optional, Tuple

from ray.data._internal.datasource_v2.common.pushdown_utils import (
    derive_list_files_pushdown,
)
from ray.data._internal.logical.interfaces import LogicalOperator, LogicalPlan, Rule
from ray.data._internal.logical.operators.map_operator import Filter
from ray.data._internal.logical.operators.read_operator import ListFiles, ReadFiles

if TYPE_CHECKING:
    from ray.data.context import DataContext
    from ray.data.expressions import Expr


__all__ = [
    "DeriveListFilesPushdown",
]


class DeriveListFilesPushdown(Rule):
    """Sync each ``ListFiles``' listing constraints from its consuming scanner.

    Listing-time prune (predicate / projected columns / limit) is only sound if
    it is no stronger than what the downstream ``ReadFiles`` applies. This rule
    derives that state from the scanner once, after the optimize loop, instead
    of having every pushdown rule mirror onto ``ListFiles``. Non-``ReadFiles``
    consumers (e.g. after ``PushdownCountFiles``) clear the constraints.
    A separate residual hint is backed by the retained Filter, never exact count.

    The limit is never derived onto a ``ListFiles`` that shuffles row groups
    (``FileShuffleConfig.small_chunks_shuffle``): the shuffle runs on the listed
    rows, so an early stop would shrink the shuffle pool. The scanner keeps its
    limit; it applies after the shuffle.
    """

    def apply(self, plan: LogicalPlan) -> LogicalPlan:  # pyrefly: ignore[bad-override]
        def transform(node: LogicalOperator) -> LogicalOperator:
            inputs = node.input_dependencies
            if not any(isinstance(input_op, ListFiles) for input_op in inputs):
                return node

            # ``ReadFiles`` is the only consumer that applies these constraints
            # downstream; for anything else they must be dropped.
            scanner = node.scanner if isinstance(node, ReadFiles) else None
            pushdown = derive_list_files_pushdown(scanner)

            new_inputs: list[LogicalOperator] = []
            changed = False
            for input_op in inputs:
                if not isinstance(input_op, ListFiles):
                    new_inputs.append(input_op)
                    continue
                # Listing stops at the limit *before* the row-group shuffle, so
                # the shuffle would only permute the first files listed.
                limit = None if _shuffles_row_groups(input_op) else pushdown.limit
                if (
                    input_op.predicate is not pushdown.predicate
                    or input_op.projected_columns != pushdown.projected_columns
                    or input_op.limit != limit
                    # Identity like ``predicate``: ``Expr.__eq__`` builds an
                    # expression, not a bool. Rebuilt per call, so this is
                    # unequal whenever a pruner exists.
                    or input_op.partition_pruner is not pushdown.partition_pruner
                ):
                    # Spelled out rather than ``**asdict(pushdown)``: that
                    # deep-copies, which would clone the pruner on every pass.
                    input_op = replace(
                        input_op,
                        predicate=pushdown.predicate,
                        projected_columns=pushdown.projected_columns,
                        limit=limit,
                        partition_pruner=pushdown.partition_pruner,
                    )
                    changed = True
                new_inputs.append(input_op)

            if not changed:
                return node
            return node._with_new_input_dependencies(new_inputs)

        def clear_listing_hint(node: LogicalOperator) -> LogicalOperator:
            if not isinstance(node, ListFiles):
                return node
            indexer = node.file_indexer.without_residual_pruning()
            if indexer is node.file_indexer:
                return node
            return replace(node, file_indexer=indexer)

        # Clear every source before any future eligibility early return. This
        # includes bare ListFiles and sources whose ReadFiles disappeared.
        # Clearing clones capable indexers, never shared configurations.
        dag = plan.dag._apply_transform(clear_listing_hint)
        dag = dag._apply_transform(transform)

        def configure(node: LogicalOperator) -> LogicalOperator:
            candidate = _residual_listing_candidate(node, plan.context)
            if candidate is None:
                return node
            read, listing, residual = candidate
            indexer = listing.file_indexer.with_residual_pruning(read.scanner, residual)
            if indexer is listing.file_indexer:
                return node
            configured = replace(listing, file_indexer=indexer)
            read = read._with_new_input_dependencies([configured])
            return node._with_new_input_dependencies([read])

        return LogicalPlan(dag=configure(dag), context=plan.context)


def _residual_listing_candidate(
    dag: LogicalOperator, context: "DataContext"
) -> Optional[Tuple[ReadFiles, ListFiles, "Expr"]]:
    """Match the supported row-query shape without changing its operators."""
    # Query-level restrictions: other roots and execution semantics decline.
    if (
        type(dag) is not Filter
        or len(dag.input_dependencies) != 1
        or context.execution_options.preserve_order
        or context.max_errored_blocks != 0
        or context.checkpoint_config is not None
    ):
        return None
    read = dag.input_dependencies[0]
    if (
        type(read) is not ReadFiles
        or read.block_udf is not None
        or len(read.input_dependencies) != 1
    ):
        return None
    listing = read.input_dependencies[0]
    if (
        type(listing) is not ListFiles
        or listing.limit is not None
        or listing.shuffle_config_factory() is not None
    ):
        return None
    residual = dag.predicate_expr
    if residual is None:
        return None
    return read, listing, residual


def _shuffles_row_groups(list_files: ListFiles) -> bool:
    shuffle_config = list_files.shuffle_config_factory()
    return shuffle_config is not None and shuffle_config.small_chunks_shuffle
