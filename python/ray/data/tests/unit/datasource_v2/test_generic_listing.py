from dataclasses import FrozenInstanceError

import pyarrow as pa
import pyarrow.dataset as ds
import pytest

import ray
import ray.cloudpickle as pickle
from ray.data._internal.datasource_v2.common.listing_pruning import (
    substitute_partition_values,
)
from ray.data._internal.datasource_v2.formats.parquet.footer_file_indexer import (
    FooterFileIndexer,
)
from ray.data._internal.datasource_v2.formats.parquet.listing_pruning import (
    ListingPruningHint,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_scanner import (
    ParquetScanner,
)
from ray.data._internal.logical.interfaces import LogicalPlan
from ray.data._internal.logical.operators import Filter, ListFiles, ReadFiles
from ray.data._internal.logical.operators.count_operator import Count
from ray.data._internal.logical.operators.n_ary_operator import Union
from ray.data._internal.logical.rules.derive_list_files_pushdown import (
    DeriveListFilesPushdown,
)
from ray.data.context import DataContext
from ray.data.expressions import col, lit

TYPES = (("p", "string"), ("x", "int64"), ("flag", "bool"))
R = (col("p") == "a") | (col("x") > 9)


def _literal(expr):
    from ray.data.expressions import LiteralExpr

    assert isinstance(expr, LiteralExpr)
    return expr


def _binary(expr):
    from ray.data.expressions import BinaryExpr

    assert isinstance(expr, BinaryExpr)
    return expr


def _footer_indexer(value):
    from ray.data._internal.datasource_v2.formats.parquet.footer_file_indexer import (
        FooterFileIndexer,
    )

    assert isinstance(value, FooterFileIndexer)
    return value


@pytest.mark.parametrize(
    "expr",
    [
        R,
        (lit("a") == col("p")) | (lit(9) < col("x")),
        ~((col("p") != "a") & (col("x") <= 9)),
        (col("p") == "a") | col("x").is_null(),
        (col("p") == "a") | col("x").is_not_null(),
        ((col("p") == "a") | (col("x") > 9)) | (col("flag") == True),  # noqa: E712
    ],
)
@pytest.mark.parametrize("partition", ["a", "b"])
def test_substitution_matches_arrow_on_nullable_memory_table(expr, partition):
    before = str(expr)
    result = substitute_partition_values(expr, {"p": partition}, TYPES)
    assert result is not None
    table = pa.table(
        {
            "p": [partition] * 4,
            "x": [None, 1, 10, 20],
            "flag": [True, False, None, False],
        }
    )
    # Arrow in-memory evaluator only, not Ray reader/end-to-end evidence.
    expected = ds.dataset(table).to_table(filter=expr.to_pyarrow())
    actual = ds.dataset(table).to_table(filter=result.to_pyarrow())
    assert actual.equals(expected)
    assert str(expr) == before


@pytest.mark.parametrize(
    "expr",
    [
        col("x") + 1,
        col("x").is_in([1, None]),
        col("x") == col("x"),
        col("x") == True,  # noqa: E712
        col("p") < "z",
        col("missing") == 1,
        col("x") == 2**63,
        col("x") == None,  # noqa: E711
        col("x") & (col("p") == "a"),
    ],
)
def test_unsupported_is_decline_not_false(expr):
    assert substitute_partition_values(expr, {"p": "b"}, TYPES) is None


@pytest.mark.parametrize(
    "values",
    [
        {"p": None},
        {"p": 1},
        {"x": True},
        {"x": 2**63},
        {"p": "\ud800"},
        {"unknown": "a"},
        {},
    ],
)
def test_invalid_replacements_decline_without_casting(values):
    assert substitute_partition_values(R, values, TYPES) is None


def test_no_constant_folding_and_no_shared_expression_leaves(monkeypatch):
    import ray.data._internal.datasource_v2.common.listing_pruning as module

    visitor = module._ColumnSubstitutionVisitor
    calls = []

    class Observed(visitor):
        def __init__(self, mapping):
            calls.append(mapping)
            super().__init__(mapping)

    monkeypatch.setattr(module, "_ColumnSubstitutionVisitor", Observed)
    result = substitute_partition_values(R, {"p": "a"}, TYPES)
    assert calls and _literal(_binary(_binary(result).left).left).value == "a"
    assert _binary(result).right.structurally_equals(
        _binary(R).right
    )  # true OR x still intact
    assert _binary(_binary(result).right).left is not _binary(_binary(R).right).left
    _binary(_binary(result).right).left.data_type._physical_dtype = str
    assert _binary(_binary(R).right).left.data_type._physical_dtype is object


def hint():
    # Use a real admitted context for this detached lifecycle hint; no file
    # access or physical reader is invoked.
    schema = pa.schema([("p", pa.string()), ("x", pa.int64()), ("flag", pa.bool_())])
    from ray.data.datasource.partitioning import Partitioning, PartitionStyle

    context = ParquetScanner(
        schema=schema, partitioning=Partitioning(PartitionStyle.HIVE, field_names=["p"])
    ).listing_pruning_context()
    assert context is not None
    return ListingPruningHint.from_residual(context, R)


def make_plan(indexer, q):
    schema = pa.schema([("p", pa.string()), ("x", pa.int64()), ("flag", pa.bool_())])
    scanner = ParquetScanner(schema=schema, predicate=q, columns=("x",))
    listing = ListFiles(
        paths=["unopened.parquet"],
        file_indexer=indexer,
        filesystem=None,
        source_paths=["unopened.parquet"],
    )
    read = ReadFiles(
        datasource_name="Parquet",
        scanner=scanner,
        schema=schema,
        parallelism=1,
        input_dependencies=[listing],
    )
    return listing, read, Filter(predicate_expr=R, input_dependencies=[read])


def apply(root):
    return DeriveListFilesPushdown().apply(LogicalPlan(root, DataContext()))


@pytest.mark.parametrize("shape", ["bare", "read", "filter", "count", "shared"])
def test_lifecycle_clears_stale_state_and_keeps_q_r(shape):
    original = FooterFileIndexer(ignore_missing_paths=False)
    prior = hint()
    configured = original.with_listing_hint(prior)
    # Arithmetic Q need not fit residual snapshot domain; it is never evaluated.
    q = (col("x") / 0) > 1
    listing, read, filtered = make_plan(configured, q)
    root = {
        "bare": listing,
        "read": read,
        "filter": filtered,
        "count": Count(input_dependencies=[read]),
        "shared": Union(input_dependencies=[filtered, listing, read]),
    }[shape]
    derived = apply(root)
    for node in derived.dag.post_order_iter():
        if isinstance(node, ListFiles):
            assert _footer_indexer(node.file_indexer)._listing_hint is None
        if isinstance(node, ReadFiles):
            assert isinstance(node.scanner, ParquetScanner)
            assert node.scanner.predicate is q
        if isinstance(node, Filter):
            assert node.predicate_expr is R
    again = apply(derived.dag)
    assert all(
        _footer_indexer(n.file_indexer)._listing_hint is None
        for n in again.dag.post_order_iter()
        if isinstance(n, ListFiles)
    )
    assert configured._listing_hint is prior and original._listing_hint is None
    assert not ray.is_initialized()


def test_hint_detaches_residual_and_copy_clear_roundtrip():
    candidate = hint()
    assert candidate is not None
    clone = pickle.loads(pickle.dumps(candidate))
    assert clone.residual.structurally_equals(R)
    _binary(_binary(clone.residual).left).left.data_type._physical_dtype = int
    assert (
        _binary(_binary(clone.residual).left).left.data_type._physical_dtype is object
    )
    with pytest.raises(FrozenInstanceError):
        # Intentional illegal assignment: verify the runtime frozen guard.
        candidate._residual = ()  # pyrefly: ignore[read-only]
    assert (
        ListingPruningHint.from_residual(candidate.context, col("x").is_in([1, None]))
        is None
    )
    indexer = FooterFileIndexer(ignore_missing_paths=False).with_listing_hint(clone)
    cleared = indexer.with_listing_hint(None)
    assert cleared is not indexer and cleared._listing_hint is None
    assert indexer._listing_hint is clone
    assert cleared.with_listing_hint(None) is cleared


def test_unknown_indexer_override_is_not_probed():
    class Unknown(FooterFileIndexer):
        def with_listing_hint(self, hint):
            raise AssertionError("unknown hook must not be invoked")

    indexer = Unknown(ignore_missing_paths=False)
    _, _, root = make_plan(indexer, col("x") > 1)
    derived = apply(root)
    assert (
        next(
            n for n in derived.dag.post_order_iter() if isinstance(n, ListFiles)
        ).file_indexer
        is indexer
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
