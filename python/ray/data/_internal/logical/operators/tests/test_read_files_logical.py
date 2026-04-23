"""Unit tests for :class:`ReadFiles`.

Verifies pushdown scaffolding (projection/predicate capability dispatch,
immutable scanner substitution) and schema inference without triggering
physical execution. Each test wires a minimal ``ListFiles`` upstream
op so ``ReadFiles`` (which now has one input dependency) can be
constructed.
"""
import pyarrow as pa
import pyarrow.parquet as pq

from ray.data._internal.datasource_v2.listing.file_indexer import (
    NonSamplingFileIndexer,
)
from ray.data._internal.datasource_v2.listing.listing_utils import (
    sample_first_file,
)
from ray.data._internal.datasource_v2.parquet_datasource_v2 import (
    ParquetDatasourceV2,
)
from ray.data._internal.datasource_v2.scanners.parquet_scanner import (
    ParquetScanner,
)
from ray.data._internal.logical.operators import ListFiles, ReadFiles
from ray.data.expressions import col


def _mk_parquet(path, table):
    pq.write_table(table, str(path))


def _mk_read_files(tmp_path) -> ReadFiles:
    f = tmp_path / "data.parquet"
    _mk_parquet(f, pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]}))

    datasource = ParquetDatasourceV2([str(f)])
    indexer = NonSamplingFileIndexer(ignore_missing_paths=False)
    sample = sample_first_file(indexer, datasource.paths, datasource.filesystem)
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
    assert op.scanner.columns is None


def test_apply_projection_none_is_noop(tmp_path):
    op = _mk_read_files(tmp_path)
    assert op.apply_projection(None) is op


def test_supports_and_apply_predicate_pushdown(tmp_path):
    op = _mk_read_files(tmp_path)
    assert op.supports_predicate_pushdown() is True

    new_op = op.apply_predicate(col("a") > 1)
    assert new_op is not op
    assert new_op.scanner.predicate is not None
    # Original scanner untouched
    assert op.scanner.predicate is None
