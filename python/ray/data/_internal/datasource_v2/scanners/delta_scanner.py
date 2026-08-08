"""Scanner for Delta Lake reads."""

from dataclasses import dataclass

from ray.data._internal.datasource_v2.scanners.parquet_scanner import ParquetScanner
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
@dataclass(frozen=True)
class DeltaScanner(ParquetScanner):
    """Parquet scanner that treats partition predicates as a pruning hint.

    ``ParquetScanner`` enforces a partition predicate by parsing values out
    of each file's path and evaluating the expression against them. That
    evaluation swallows any error and conservatively keeps the file, which is
    sound only when nothing else depends on it -- but the optimizer removes
    the ``Filter`` once a scanner accepts the predicate, leaving path parsing
    as the sole enforcement point.

    For a Delta table the values in a path are a lossy view of what the log
    records: a null partition is the literal directory ``__HIVE_DEFAULT_
    PARTITION__``, an empty string is an empty path segment, and the declared
    column type lives only in the log. Predicates such as ``col("p").is_in
    ([...])`` over a nullable partition column therefore fail to evaluate and
    are silently treated as matching everything.

    Declining enforcement keeps the ``Filter`` in the plan, where the
    predicate is applied to the partition columns the reader has already
    materialized -- exactly typed, with real nulls.
    :class:`~ray.data._internal.logical.rules.derive_list_files_pushdown.DeriveListFilesPushdown`
    still reports the same predicate to ``ListFiles``, so the Delta log can
    skip files from it and the pruning is kept -- only the correctness burden
    is moved.
    """

    @property
    def enforces_partition_predicate(self) -> bool:
        return False
