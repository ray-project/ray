"""Relay settled read predicates into Delta's log-driven file listing."""

from dataclasses import replace

from ray.data._internal.datasource_v2.listing.delta_file_indexer import DeltaFileIndexer
from ray.data._internal.datasource_v2.scanners.arrow_file_scanner import (
    ArrowFileScanner,
)
from ray.data._internal.logical.interfaces import LogicalOperator, LogicalPlan, Rule
from ray.data._internal.logical.operators.read_operator import ListFiles, ReadFiles

__all__ = [
    "PushdownDeltaFilePruning",
]


class PushdownDeltaFilePruning(Rule):
    """Give ``ListFiles`` the predicates that ``PredicatePushdown`` settled.

    ``PredicatePushdown`` lands a query's predicates on the *scanner*, which
    enforces them while reading. For a Delta table those same predicates can
    also be answered from the transaction log -- partition values and
    per-file min/max statistics are recorded there -- so a file that cannot
    match need never be listed, sized, chunked, or scheduled. This rule
    copies them onto the upstream :class:`DeltaFileIndexer` to make that
    happen.

    It must run *after* ``PredicatePushdown``, whose output it reads.

    Skipping this rule costs performance and nothing else. The scanner still
    holds and applies the predicates, so a plan shape this rule doesn't
    recognize reads more files than it needs but returns the same rows. No
    correctness argument may be built on this rule firing.
    """

    def apply(self, plan: LogicalPlan) -> LogicalPlan:
        dag = plan.dag
        new_dag = dag._apply_transform(self._push_into_listing)
        return LogicalPlan(new_dag, plan.context) if dag is not new_dag else plan

    @classmethod
    def _push_into_listing(cls, op: LogicalOperator) -> LogicalOperator:
        if not isinstance(op, ReadFiles):
            return op

        scanner = op.scanner
        if not isinstance(scanner, ArrowFileScanner):
            return op
        if scanner.predicate is None and scanner.partition_predicate is None:
            return op

        assert len(op.input_dependencies) == 1, len(op.input_dependencies)
        list_files = op.input_dependencies[0]
        if not isinstance(list_files, ListFiles):
            return op

        indexer = list_files.file_indexer
        if not isinstance(indexer, DeltaFileIndexer):
            return op

        # ``op.schema`` is the unprojected table schema, which is what the
        # indexer needs to cast the log's string partition values before
        # comparing them.
        new_indexer = indexer.with_predicates(
            partition_predicate=scanner.partition_predicate,
            data_predicate=scanner.predicate,
            table_schema=op.schema,
        )
        return replace(
            op, input_dependencies=[replace(list_files, file_indexer=new_indexer)]
        )
