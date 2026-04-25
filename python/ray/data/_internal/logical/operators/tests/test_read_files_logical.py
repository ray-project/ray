"""Unit tests for :class:`ReadFiles`.

Verifies pushdown scaffolding (projection/predicate capability dispatch,
immutable scanner substitution) and schema inference without triggering
physical execution. Each test wires a minimal ``ListFiles`` upstream
op so ``ReadFiles`` (which now has one input dependency) can be
constructed.
"""
import os

import pyarrow as pa
import pyarrow.parquet as pq

from ray.data._internal.datasource_v2.listing.file_indexer import (
    NonSamplingFileIndexer,
)
from ray.data._internal.datasource_v2.listing.listing_utils import (
    sample_files,
)
from ray.data._internal.datasource_v2.parquet_datasource_v2 import (
    ParquetDatasourceV2,
)
from ray.data._internal.datasource_v2.scanners.parquet_scanner import (
    ParquetScanner,
)
from ray.data._internal.logical.operators import Filter, ListFiles, ReadFiles
from ray.data.datasource.partitioning import Partitioning, PartitionStyle
from ray.data.expressions import col


def _mk_parquet(path, table):
    pq.write_table(table, str(path))


def _mk_read_files(tmp_path) -> ReadFiles:
    f = tmp_path / "data.parquet"
    _mk_parquet(f, pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]}))

    datasource = ParquetDatasourceV2([str(f)])
    indexer = NonSamplingFileIndexer(ignore_missing_paths=False)
    sample = sample_files(indexer, datasource.paths, datasource.filesystem)
    schema = datasource.infer_schema(sample)
    scanner = datasource.create_scanner(schema=schema)

    list_files_op = ListFiles(
        paths=list(datasource.paths),
        file_indexer=indexer,
        filesystem=datasource.filesystem,
        source_paths=list(datasource.paths),
        file_extensions=datasource.file_extensions,
    )

    return ReadFiles(
        input_op=list_files_op,
        datasource=datasource,
        scanner=scanner,
        schema=schema,
        parallelism=-1,
    )


def _mk_partitioned_read_files(tmp_path) -> ReadFiles:
    """Hive-partitioned dataset with partition column ``country``."""
    for country, value in (("US", 1), ("CA", 2)):
        d = tmp_path / f"country={country}"
        os.makedirs(d, exist_ok=True)
        _mk_parquet(d / "data.parquet", pa.table({"a": [value], "b": [str(value)]}))

    partitioning = Partitioning(
        PartitionStyle.HIVE, base_dir=str(tmp_path), field_names=["country"]
    )
    datasource = ParquetDatasourceV2([str(tmp_path)], partitioning=partitioning)
    indexer = NonSamplingFileIndexer(ignore_missing_paths=False)
    sample = sample_files(indexer, datasource.paths, datasource.filesystem)
    schema = datasource.infer_schema(sample)
    scanner = datasource.create_scanner(schema=schema, partitioning=partitioning)

    list_files_op = ListFiles(
        paths=list(datasource.paths),
        file_indexer=indexer,
        filesystem=datasource.filesystem,
        source_paths=list(datasource.paths),
        file_extensions=datasource.file_extensions,
    )

    return ReadFiles(
        input_op=list_files_op,
        datasource=datasource,
        scanner=scanner,
        schema=schema,
        parallelism=-1,
    )


def test_construction_stores_schema_and_infer_schema_returns_it(tmp_path):
    op = _mk_read_files(tmp_path)
    assert op.infer_schema().names == ["a", "b"]


def test_input_dependency_is_list_files(tmp_path):
    op = _mk_read_files(tmp_path)
    assert isinstance(op.input_dependency, ListFiles)


def test_supports_projection_pushdown_true_for_parquet_scanner(tmp_path):
    op = _mk_read_files(tmp_path)
    assert op.supports_projection_pushdown() is True


def test_apply_projection_returns_new_op_with_pruned_scanner(tmp_path):
    op = _mk_read_files(tmp_path)
    new_op = op.apply_projection({"a": "a"})

    assert new_op is not op
    assert isinstance(new_op.scanner, ParquetScanner)
    assert new_op.scanner.columns == ("a",)
    # Original scanner untouched
    assert isinstance(op.scanner, ParquetScanner)
    assert op.scanner.columns is None


def test_apply_projection_none_is_noop(tmp_path):
    op = _mk_read_files(tmp_path)
    assert op.apply_projection(None) is op


def test_supports_and_apply_predicate_pushdown(tmp_path):
    op = _mk_read_files(tmp_path)
    assert op.supports_predicate_pushdown() is True

    new_op = op.apply_predicate(col("a") > 1)
    assert new_op is not op
    assert isinstance(new_op.scanner, ParquetScanner)
    assert new_op.scanner.predicate is not None
    # Original scanner untouched
    assert isinstance(op.scanner, ParquetScanner)
    assert op.scanner.predicate is None


def test_apply_predicate_partition_only_routes_to_prune_partitions(tmp_path):
    op = _mk_partitioned_read_files(tmp_path)

    new_op = op.apply_predicate(col("country") == "US")

    assert isinstance(new_op, ReadFiles) and new_op is not op
    assert new_op.scanner.partition_predicate is not None
    assert new_op.scanner.predicate is None
    assert op.scanner.partition_predicate is None


def test_apply_predicate_mixed_and_splits_into_data_and_partition(tmp_path):
    op = _mk_partitioned_read_files(tmp_path)

    new_op = op.apply_predicate((col("a") > 0) & (col("country") == "US"))

    assert isinstance(new_op, ReadFiles) and new_op is not op
    assert new_op.scanner.predicate is not None
    assert new_op.scanner.partition_predicate is not None


def test_apply_predicate_mixed_or_keeps_filter_above(tmp_path):
    op = _mk_partitioned_read_files(tmp_path)

    # Mixed-column ``OR`` can't be safely split — neither bucket is
    # populated, so ``apply_predicate`` returns ``self`` and the rule
    # leaves the ``Filter`` above ``ReadFiles`` untouched.
    result = op.apply_predicate((col("a") > 0) | (col("country") == "US"))
    assert result is op


def test_apply_predicate_mixed_and_with_unsplittable_residual(tmp_path):
    op = _mk_partitioned_read_files(tmp_path)

    # Top-level ``AND`` of one pure-data, one pure-partition, and one
    # mixed-OR conjunct: the first two push, the OR stays as a residual
    # ``Filter`` so we don't silently drop it.
    pure_data = col("a") > 0
    pure_partition = col("country") == "US"
    mixed_or = (col("a") < 100) | (col("country") == "CA")

    result = op.apply_predicate(pure_data & pure_partition & mixed_or)

    assert isinstance(result, Filter)
    assert isinstance(result.input_dependency, ReadFiles)
    new_read = result.input_dependency
    assert new_read.scanner.predicate is not None
    assert new_read.scanner.partition_predicate is not None
    # The residual carried by the new Filter is exactly the mixed-OR
    # conjunct that couldn't be pushed.
    assert result.predicate_expr.structurally_equals(mixed_or)
