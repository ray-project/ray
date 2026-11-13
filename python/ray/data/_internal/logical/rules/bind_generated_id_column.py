"""Add the generated ID column to Parquet reads when the plan is optimized.

``CheckpointConfig(generated_id_column=...)`` can be set on a dataset after
``read_parquet()`` built it (Ray Train does this), so the read itself can't
decide whether to add the column. This rule decides from the config of the
plan being executed.
"""

from dataclasses import replace

import pyarrow as pa

from ray.data._internal.logical.interfaces import LogicalOperator, LogicalPlan, Rule
from ray.data._internal.logical.operators.map_operator import Project
from ray.data._internal.logical.operators.read_operator import ReadFiles
from ray.data.expressions import col

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
    from ray.data._internal.datasource_v2.formats.parquet.parquet_scanner import (
        ParquetScanner,
    )
    from ray.data.checkpoint.generated_id import GeneratedIdColumn
    from ray.data.checkpoint.interfaces import InvalidCheckpointingConfig

    scanner = op.scanner
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
