"""Add the generated ID column to Parquet reads when the plan is optimized.

``CheckpointConfig(generated_id_column=...)`` can be set on a dataset after
``read_parquet()`` or ``read_delta()`` built it (Ray Train does this), so the
read itself can't decide whether to add the column. This rule decides from the
config of the plan being executed.
"""

import logging
from dataclasses import replace
from typing import TYPE_CHECKING

import pyarrow as pa

from ray.data._internal.logical.interfaces import LogicalOperator, LogicalPlan, Rule
from ray.data._internal.logical.operators.map_operator import Project
from ray.data._internal.logical.operators.read_operator import ReadFiles
from ray.data.expressions import col
from ray.util.debug import log_once

if TYPE_CHECKING:
    from ray.data._internal.datasource_v2.formats.delta.delta_scanner import (
        DeltaScanner,
    )

logger = logging.getLogger(__name__)

__all__ = [
    "BindGeneratedIdColumn",
]


class BindGeneratedIdColumn(Rule):
    """Add the configured generated ID column to every V2 Parquet read.

    The column is a :class:`~ray.data.checkpoint.generated_id.GeneratedIdColumn`
    appended to the read's ``ParquetScanner``. ``Project``\\s expand ``*``
    against the schema they see when they're built, which doesn't have the
    column yet, so the rule also adds it to each ``Project`` that lists its
    columns explicitly.

    A read or ``Project`` that keeps no columns is left alone: nothing
    downstream of it can use the IDs.
    """

    def apply(self, plan: LogicalPlan) -> LogicalPlan:  # pyrefly: ignore[bad-override]
        config = plan.context.checkpoint_config
        if config is None or config.generated_id_column is None:
            return plan
        # Declared ``str`` so the narrowing survives the closure below.
        name: str = config.generated_id_column

        def transform(op: LogicalOperator) -> LogicalOperator:
            if isinstance(op, ReadFiles):
                return _bind_read(op, name)
            if isinstance(op, Project):
                return _keep_column(op, name)
            return op

        dag = plan.dag._apply_transform(transform)
        if dag is plan.dag:
            return plan
        return LogicalPlan(dag=dag, context=plan.context)


def _bind_read(op: ReadFiles, name: str) -> ReadFiles:
    # Lazy imports: ``ray.data.checkpoint`` imports ``ray.data.context``.
    from ray.data._internal.datasource_v2.formats.delta.delta_scanner import (
        DeltaScanner,
    )
    from ray.data._internal.datasource_v2.formats.parquet.parquet_scanner import (
        ParquetScanner,
    )
    from ray.data.checkpoint.generated_id import GeneratedIdColumn
    from ray.data.checkpoint.interfaces import InvalidCheckpointingConfig

    scanner = op.scanner
    if isinstance(scanner, DeltaScanner):
        _warn_if_delta_version_not_pinned(scanner)
    # Other formats can't produce the IDs; the planner rejects them.
    if not isinstance(scanner, ParquetScanner) or scanner.columns == ():
        return op

    column = GeneratedIdColumn(name)
    if column not in scanner.synthesized_columns:
        if any(
            synthesized_column.name == name
            for synthesized_column in scanner.synthesized_columns
        ):
            raise InvalidCheckpointingConfig(
                f"Generated ID column {name!r} conflicts with an existing "
                "synthesized column of the same name. Pick a different "
                "`CheckpointConfig(generated_id_column=...)`."
            )
        if name in scanner.schema.names:
            raise InvalidCheckpointingConfig(
                f"Column {name!r} already exists in the Parquet data, so Ray "
                "Data can't add its generated ID column of the same name. Pick "
                "a different `CheckpointConfig(generated_id_column=...)`."
            )
        # ``read_schema()`` looks up every projected column in ``schema``.
        scanner = replace(
            scanner,
            schema=scanner.schema.append(pa.field(name, column.type)),
            synthesized_columns=(*scanner.synthesized_columns, column),
        )
    if scanner.columns is not None and name not in scanner.columns:
        scanner = replace(scanner, columns=(*scanner.columns, name))

    if scanner is op.scanner:
        return op
    return replace(op, scanner=scanner)


def _warn_if_delta_version_not_pinned(scanner: "DeltaScanner") -> None:
    """Warn that a resumed job may read a newer version of the Delta table.

    A generated ID names a row's place in a data file. Delta data files never
    change, so a job resumed at the same table version finds every ID again.
    But ``read_delta`` without ``version=`` reads the latest version, and
    rows a later commit rewrote into new files (``OPTIMIZE``, an overwrite)
    get new IDs and are processed again.
    """
    if scanner.version_pinned_by_caller or not log_once(
        f"delta_generated_id_unpinned_version:{scanner.table_uri}"
    ):
        return
    logger.warning(
        "`generated_id_column` is set for a `read_delta` of the latest version of "
        f"{scanner.table_uri!r}. A resumed job reads the version that is latest "
        "then, and processes again any rows that a commit in between rewrote "
        "into new files, for example by compaction or an overwrite. Pass "
        "`version=` to `read_delta` to resume against the same version."
    )


def _keep_column(op: Project, name: str) -> Project:
    if (
        not op.exprs
        or op.has_star_expr()
        or any(expr.name == name for expr in op.exprs)
    ):
        return op
    input_schema = op.input_dependencies[0].infer_schema()
    if input_schema is None or not isinstance(input_schema, pa.Schema):
        return op
    if name not in input_schema.names:
        return op
    return replace(op, exprs=[*op.exprs, col(name)])
