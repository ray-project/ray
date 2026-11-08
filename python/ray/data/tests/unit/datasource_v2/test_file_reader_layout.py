"""Independent baseline oracles for the behavior-preserving reader extraction."""

from types import SimpleNamespace
from typing import cast

import pyarrow as pa
import pyarrow.dataset as pds
import pyarrow.parquet as pq
import pytest

from ray.data._internal.datasource_v2.common.file_reader import (
    FileFormat,
    FileReader,
    _resolve_file_dataset_schema,
)
from ray.data._internal.datasource_v2.interfaces.synthesized_columns import (
    SynthesizedColumn,
)
from ray.data.datasource.partitioning import PathPartitionParser


class _Extension(pa.ExtensionType):
    def __init__(self):
        super().__init__(pa.int64(), "listing.component.extension")

    def __arrow_ext_serialize__(self):
        return b""

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        return cls()


class _ForbiddenNames:
    @property
    def name(self):
        raise AssertionError("synthesized name must not be read")

    @property
    def _scheme(self):
        raise AssertionError("partition scheme must not be read")

    def __iter__(self):
        raise AssertionError("names must not be iterated")


def _baseline_pin(schema, partition_keys, synthesized):
    # Frozen source's computation, independent of the new shared helpers.
    if schema is None:
        return None
    if any(isinstance(f.type, pa.ExtensionType) for f in schema):
        return None
    fields = [
        f
        for f in schema
        if f.name not in set(partition_keys) and f.name not in set(synthesized)
    ]
    return pa.schema(fields) if fields else None


@pytest.mark.parametrize(
    "schema",
    [
        None,
        pa.schema([]),
        pa.schema([("x", pa.int64()), ("p", pa.string())]),
        pa.schema([("p", pa.string())]),
        pa.schema([("x", pa.int64()), ("x", pa.string())]),
        pa.schema(
            [
                pa.field("x", pa.int64(), metadata={b"field": b"yes"}),
                pa.field("p", pa.string()),
            ],
            metadata={b"schema": b"lost-as-before"},
        ),
        pa.schema([("x", pa.int64()), ("p", _Extension())]),
    ],
)
@pytest.mark.parametrize(
    "partition,synthesized", [((), ()), (("p",), ()), ((), ("p",)), (("x",), ("p",))]
)
def test_pin_matches_frozen_computation(schema, partition, synthesized):
    expected = _baseline_pin(schema, partition, synthesized)
    if schema is not None and not any(
        isinstance(f.type, pa.ExtensionType) for f in schema
    ):
        actual = _resolve_file_dataset_schema(schema, partition, synthesized)
        if expected is None:
            assert actual is None
        else:
            assert actual is not None
            assert actual.equals(expected, check_metadata=True)
    # Structural spies intentionally expose only names; this test never reads data.
    reader = FileReader(
        FileFormat.PARQUET,
        schema=schema,
        synthesized_columns=tuple(
            cast(SynthesizedColumn, SimpleNamespace(name=n)) for n in synthesized
        ),
    )
    reader._partition_parser = cast(
        PathPartitionParser,
        SimpleNamespace(_scheme=SimpleNamespace(field_names=partition)),
    )
    if expected is None:
        assert reader._file_dataset_schema is None
    else:
        pinned = reader._file_dataset_schema
        assert pinned is not None
        assert pinned.equals(expected, check_metadata=True)
        assert reader._file_dataset_schema is reader._file_dataset_schema


@pytest.mark.parametrize("schema", [None, pa.schema([("p", _Extension())])])
def test_declined_pin_does_not_touch_name_properties(schema):
    reader = FileReader(
        FileFormat.PARQUET,
        schema=schema,
        synthesized_columns=(cast(SynthesizedColumn, _ForbiddenNames()),),
    )
    # Deliberately invalid spies prove the early guard never probes their names.
    reader._partition_parser = cast(PathPartitionParser, _ForbiddenNames())
    assert reader._file_dataset_schema is None


def test_actual_dataset_pin_is_identical_for_all_manifest_subsets(tmp_path):
    left, right = tmp_path / "left.parquet", tmp_path / "right.parquet"
    pq.write_table(pa.table({"x": pa.array([None], type=pa.null()), "q": [1]}), left)
    pq.write_table(pa.table({"x": [7], "q": [2], "extra": [True]}), right)
    pin = pa.schema([("x", pa.int64()), ("q", pa.int64())])
    for paths in ([left], [right], [left, right], [right, left]):
        dataset = pds.dataset([str(p) for p in paths], format="parquet", schema=pin)
        assert dataset.schema.equals(pin, check_metadata=True)
    # This establishes pin/layout identity, not safe physical-type admission.
    assert pq.read_metadata(left).schema.column(0).physical_type != "INT64"
    assert pq.read_metadata(right).num_columns == 3


@pytest.mark.parametrize("columns", [[], ["p"], ["path", "x"], ["p", "x", "path"]])
def test_actual_reader_projection_keeps_rows_and_derived_columns(tmp_path, columns):
    from ray.data._internal.arrow_block import _BATCH_SIZE_PRESERVING_STUB_COL_NAME
    from ray.data._internal.datasource_v2.common.synthesized_columns import PathColumn
    from ray.data._internal.datasource_v2.formats.parquet.parquet_file_reader import (
        ParquetFileReader,
    )
    from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
    from ray.data.datasource.partitioning import Partitioning, PartitionStyle

    folder = tmp_path / "p=a"
    folder.mkdir()
    path = folder / "part.parquet"
    pq.write_table(pa.table({"x": [1, 2, 3]}), path, row_group_size=2)
    schema = pa.schema([("x", pa.int64()), ("p", pa.string()), ("path", pa.string())])
    reader = ParquetFileReader(
        schema=schema,
        columns=columns,
        partitioning=Partitioning(
            PartitionStyle.HIVE, base_dir=str(tmp_path), field_names=["p"]
        ),
        synthesized_columns=(PathColumn(),),
    )
    manifest = FileManifest.construct_manifest(
        paths=[str(path)], sizes=[path.stat().st_size], chunk_metadatas=[None]
    )
    result = pa.concat_tables(list(reader.read(manifest)))
    assert result.num_rows == 3
    assert result.column_names == (columns or [_BATCH_SIZE_PRESERVING_STUB_COL_NAME])
    expected = {"x": [1, 2, 3], "p": ["a"] * 3, "path": [str(path)] * 3}
    for column in columns:
        assert result[column].to_pylist() == expected[column]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
