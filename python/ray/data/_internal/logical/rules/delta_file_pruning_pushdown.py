"""Relay settled read predicates into Delta's log-driven file listing."""

from dataclasses import replace
from typing import List, Type

from ray.data._internal.datasource_v2.listing.delta_file_indexer import DeltaFileIndexer
from ray.data._internal.datasource_v2.scanners.arrow_file_scanner import (
    ArrowFileScanner,
)
from ray.data._internal.logical.interfaces import LogicalOperator, LogicalPlan, Rule
from ray.data._internal.logical.operators.read_operator import ListFiles, ReadFiles
from ray.data._internal.logical.rules.predicate_pushdown import PredicatePushdown

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

    Skipping this rule costs performance and nothing else. The scanner holds
    the same predicates and enforces them while reading -- data predicates
    through the PyArrow scanner filter, partition predicates by parsing the
    file path -- so a plan shape this rule doesn't recognize reads more files
    than it needs but returns the same rows.

    That second guarantee only holds because ``read_delta`` gives the
    partitioning a ``field_types`` mapping, so path-parsed partition values
    are typed and comparable to the predicate's literals. Tables whose
    partition types can't be expressed that way stay on the V1 read path
    rather than depend on this rule for a correct answer.
    """

    @classmethod
    def dependencies(cls) -> List[Type["Rule"]]:
        # This rule reads the predicates ``PredicatePushdown`` settles onto
        # the scanner, so it has to see the plan afterwards. Declared rather
        # than left to list order: ``Ruleset`` only honors declared edges, and
        # position is merely a tiebreaker among rules with no dependencies.
        return [PredicatePushdown]

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
