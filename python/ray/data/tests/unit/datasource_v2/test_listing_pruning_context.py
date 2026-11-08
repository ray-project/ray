"""Value and configuration tests; no hint is installed by an optimizer here."""

from dataclasses import FrozenInstanceError

import pyarrow as pa
import pytest

import ray.cloudpickle as cloudpickle
from ray.data._internal.datasource_v2.common.listing_pruning import (
    _snapshot_expression,
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
from ray.data.datasource.partitioning import Partitioning, PartitionStyle
from ray.data.expressions import (
    ColumnExpr,
    LiteralExpr,
    col,
    lit,
)


def _binary(expr):
    from ray.data.expressions import BinaryExpr

    assert isinstance(expr, BinaryExpr)
    return expr


def _scanner(**kwargs):
    config = dict(
        schema=pa.schema([("x", pa.int64()), ("q", pa.int64()), ("p", pa.string())]),
        partitioning=Partitioning(PartitionStyle.HIVE, field_names=["p"]),
    )
    config.update(kwargs)
    schema = config.pop("schema")
    return ParquetScanner(schema=schema, **config)


def _hint():
    context = _scanner().listing_pruning_context()
    assert context is not None
    return ListingPruningHint.from_residual(context, (col("p") == "a") | (col("x") > 9))


@pytest.mark.parametrize("columns", [None, (), ("p", "x"), ("x", "x", "p")])
def test_context_preserves_reader_schema_and_hidden_predicate(columns):
    scanner = _scanner(columns=columns, predicate=col("q") > 0)
    context = scanner.listing_pruning_context()
    assert context is not None
    reader = scanner.create_reader()
    assert context.file_schema.equals(reader._file_dataset_schema, check_metadata=True)
    assert context.file_schema_signature == (("x", "int64"), ("q", "int64"))
    assert "q" in context.file_schema.names  # Hidden Q still belongs to the fixed pin.
    assert reader._predicate is scanner.predicate


def test_context_snapshots_mutable_configuration_without_resolving_paths():
    names, columns = ["p"], ["x", "p"]
    types: dict[str, type] = {"p": str}
    scheme = Partitioning(PartitionStyle.HIVE, field_names=names, field_types=types)
    scanner = _scanner(partitioning=scheme, columns=columns)
    assert scheme._normalized_base_dir is None
    context = scanner.listing_pruning_context()
    assert context is not None
    assert scheme._normalized_base_dir is None
    names.append("later")
    types["p"] = int
    columns.clear()
    assert context.partitioning.field_names == ("p",)
    assert context.partitioning.field_types == (("p", "string"),)
    assert context.file_schema.names == ["x", "q"]
    with pytest.raises(FrozenInstanceError):
        # Intentional illegal assignment: verify the runtime frozen guard.
        context.partitioning = None  # pyrefly: ignore[read-only]


@pytest.mark.parametrize(
    "changes",
    [
        {"schema": pa.schema([])},
        {"schema": pa.schema([("p", pa.string())])},
        {
            "schema": pa.schema(
                [("x", pa.int64()), ("x", pa.int64()), ("p", pa.string())]
            )
        },
        {"schema": pa.schema([("x", pa.float32()), ("p", pa.string())])},
        {"parquet_format_kwargs": {"pre_buffer": True}},
        {"partitioning": None},
        {"partitioning": Partitioning(PartitionStyle.HIVE)},
        {"partitioning": Partitioning(PartitionStyle.DIRECTORY, field_names=["p"])},
        {
            "partitioning": Partitioning(
                PartitionStyle.HIVE, field_names=["p"], field_types={"p": float}
            )
        },
    ],
)
def test_unknown_layout_declines(changes):
    assert _scanner(**changes).listing_pruning_context() is None


def test_unknown_scanner_and_synthesis_decline_without_callbacks():
    class Derived(ParquetScanner):
        pass

    class Column:
        @property
        def name(self):
            raise AssertionError("must not access synthesis")

    assert (
        Derived(
            schema=_scanner().schema, partitioning=_scanner().partitioning
        ).listing_pruning_context()
        is None
    )
    assert _scanner(synthesized_columns=(Column(),)).listing_pruning_context() is None


def test_snapshot_is_immutable_even_when_expr_dtype_changes():
    q, residual = col("q") > 1, (col("p") == "a") | ~(col("x") <= 9)
    context = _scanner().listing_pruning_context()
    assert context is not None
    hint = ListingPruningHint.from_residual(context, residual)
    assert hint is not None
    expected = hint._residual
    _binary(q).right.data_type._physical_dtype = str
    _binary(_binary(residual).left).left.data_type._physical_dtype = bool
    assert hint._residual == expected
    one = hint.residual
    _binary(_binary(one).left).left.data_type._physical_dtype = int
    assert _binary(_binary(hint.residual).left).left.data_type._physical_dtype is object
    assert cloudpickle.loads(cloudpickle.dumps(hint))._residual == expected
    with pytest.raises(FrozenInstanceError):
        # Intentional illegal assignment: verify the runtime frozen guard.
        hint._residual = ()  # pyrefly: ignore[read-only]


@pytest.mark.parametrize(
    "expr",
    [
        col("x") + 1,
        col("x") == (col("q") + 1),
        col("x").not_in([1, 2]),
        LiteralExpr(None),
        LiteralExpr(2**63),
        LiteralExpr(-(2**63) - 1),
        LiteralExpr("\ud800"),
    ],
)
def test_snapshot_rejects_unknown_shape_without_evaluation(expr):
    with pytest.raises(ValueError):
        _snapshot_expression(expr)


def test_mutable_unknown_literal_and_subclass_are_not_inspected():
    class Hostile:
        def __eq__(self, other):
            raise AssertionError("equality")

        def __repr__(self):
            raise AssertionError("repr")

    # Inject only in a test so LiteralExpr's existing constructor cannot inspect it.
    expr = lit(1)
    object.__setattr__(expr, "value", Hostile())
    with pytest.raises(ValueError):
        _snapshot_expression(expr)

    class DerivedColumn(ColumnExpr):
        @property
        def name(self):
            raise AssertionError("subclass name")

    with pytest.raises(ValueError):
        _snapshot_expression(DerivedColumn("x"))


def test_int_and_bool_snapshots_are_distinct_and_nested_shapes_survive():
    assert _snapshot_expression(lit(True)) != _snapshot_expression(lit(1))
    expr = ~((lit(1) < col("x")) & ((col("p") != "b") | col("x").is_null()))
    context = _scanner().listing_pruning_context()
    assert context is not None
    hint = ListingPruningHint.from_residual(context, expr)
    assert hint is not None
    assert hint.residual.structurally_equals(expr)


def test_indexer_configuration_is_copied_and_serializable():
    original = FooterFileIndexer(ignore_missing_paths=False, coalesce_bytes=777)
    hint = _hint()
    assert hint is not None
    configured = original.with_listing_hint(hint)
    assert configured is not original
    assert original._listing_hint is None
    assert configured._listing_hint is hint
    assert configured._coalesce_bytes == original._coalesce_bytes == 777
    cleared = configured.with_listing_hint(None)
    assert cleared is not configured and cleared._listing_hint is None
    assert configured._listing_hint is hint
    assert cleared.with_listing_hint(None) is cleared
    restored = cloudpickle.loads(cloudpickle.dumps(configured))
    assert restored._listing_hint._residual == hint._residual
    assert restored._listing_hint.context.file_schema.equals(hint.context.file_schema)

    class Derived(FooterFileIndexer):
        pass

    derived = Derived(ignore_missing_paths=False)
    assert derived.with_listing_hint(hint) is derived
    assert derived._listing_hint is None


def test_context_rejects_extension_and_custom_inputs_without_callback():
    class Extension(pa.ExtensionType):
        def __init__(self):
            super().__init__(pa.int64(), "listing.context.extension")

        def __arrow_ext_serialize__(self):
            return b""

        def __hash__(self):
            raise AssertionError("custom extension hash")

        def __eq__(self, other):
            raise AssertionError("custom extension equality")

    # Keep this object alive: this fixture isolates hash/equality callbacks.
    # Dead-instance reconstruction is a separate logical-schema admission gate.
    extension = Extension()
    schema = pa.schema([("x", extension), ("p", pa.string())])
    assert _scanner(schema=schema).listing_pruning_context() is None

    class HostileContainer(dict):
        def __bool__(self):
            raise AssertionError("custom truthiness")

        def items(self):
            raise AssertionError("custom items")

    assert (
        _scanner(parquet_format_kwargs=HostileContainer()).listing_pruning_context()
        is None
    )
    scheme = Partitioning(PartitionStyle.HIVE, field_names=["p"])
    scheme.field_types = HostileContainer()
    assert _scanner(partitioning=scheme).listing_pruning_context() is None
    assert _scanner(filesystem=object()).listing_pruning_context() is None


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
