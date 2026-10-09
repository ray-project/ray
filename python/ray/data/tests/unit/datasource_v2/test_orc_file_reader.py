"""Unit tests for DSV2 ORC scanning and file reading."""

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as pds
import pytest
from pyarrow import orc

from ray.data._internal.arrow_block import _BATCH_SIZE_PRESERVING_STUB_COL_NAME
from ray.data._internal.datasource_v2.common.file_reader import FileFormat
from ray.data._internal.datasource_v2.common.synthesized_columns import PathColumn
from ray.data._internal.datasource_v2.formats.orc.orc_file_reader import OrcFileReader
from ray.data._internal.datasource_v2.formats.orc.orc_scanner import OrcScanner
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.object_extensions.arrow import ArrowPythonObjectType
from ray.data.datasource.partitioning import Partitioning, PartitionStyle
from ray.data.expressions import col


def _write_orc(path: Path, table: pa.Table, **kwargs) -> None:
    with pa.OSFile(str(path), "wb") as sink:
        orc.write_table(table, sink, **kwargs)


def _manifest(*paths: Path) -> FileManifest:
    return FileManifest.construct_manifest(
        paths=[str(path) for path in paths],
        sizes=[path.stat().st_size for path in paths],
        chunk_metadatas=[None] * len(paths),
    )


def test_orc_reader_yields_batches(tmp_path):
    path = tmp_path / "data.orc"
    expected = pa.table({"id": [0, 1, 2, 3, 4], "name": ["a", "b", "c", "d", "e"]})
    _write_orc(path, expected)

    scanner = OrcScanner(schema=expected.schema, batch_size=2)
    batches = list(scanner.create_reader().read(_manifest(path)))

    assert scanner.read_schema() == expected.schema
    assert [batch.num_rows for batch in batches] == [2, 2, 1]
    assert pa.concat_tables(batches).equals(expected)


def test_orc_reader_reads_multiple_stripes(tmp_path):
    path = tmp_path / "multi.orc"
    expected = pa.table({"id": list(range(100_000))})
    _write_orc(path, expected, stripe_size=64 * 1024)
    assert orc.ORCFile(str(path)).nstripes > 1

    batches = list(
        OrcScanner(schema=expected.schema, batch_size=4096)
        .create_reader()
        .read(_manifest(path))
    )

    assert len(batches) > 1
    assert all(batch.num_rows <= 4096 for batch in batches)
    assert pa.concat_tables(batches).equals(expected)


def test_orc_reader_aligns_explicit_schema_across_files(tmp_path):
    left_path = tmp_path / "left.orc"
    right_path = tmp_path / "right.orc"
    _write_orc(left_path, pa.table({"id": [1], "left": ["a"]}))
    _write_orc(right_path, pa.table({"id": [2], "right": [True]}))
    schema = pa.schema(
        [
            ("id", pa.int64()),
            ("left", pa.string()),
            ("right", pa.bool_()),
        ]
    )

    batches = list(
        OrcScanner(schema=schema).create_reader().read(_manifest(left_path, right_path))
    )
    result = pa.concat_tables(batches)

    assert result.schema == schema
    assert result.to_pylist() == [
        {"id": 1, "left": "a", "right": None},
        {"id": 2, "left": None, "right": True},
    ]


def test_orc_reader_projection_filter_and_limit(tmp_path):
    path = tmp_path / "data.orc"
    table = pa.table({"id": [0, 1, 2, 3], "label": ["a", "b", "c", "d"]})
    _write_orc(path, table)

    scanner, residual = OrcScanner(schema=table.schema, batch_size=2).push_filters(
        col("id") >= 1
    )
    scanner = scanner.prune_columns(["label"]).push_limit(2)
    result = pa.concat_tables(list(scanner.create_reader().read(_manifest(path))))

    assert residual is None
    assert scanner.read_schema().names == ["label"]
    assert result.to_pylist() == [{"label": "b"}, {"label": "c"}]


def test_orc_reader_empty_projection_preserves_rows(tmp_path):
    path = tmp_path / "data.orc"
    table = pa.table({"id": [0, 1, 2]})
    _write_orc(path, table)

    scanner = OrcScanner(schema=table.schema, batch_size=2).prune_columns([])
    batches = list(scanner.create_reader().read(_manifest(path)))

    assert scanner.read_schema().names == []
    assert sum(batch.num_rows for batch in batches) == 3
    assert all(
        batch.column_names == [_BATCH_SIZE_PRESERVING_STUB_COL_NAME]
        for batch in batches
    )


@pytest.mark.parametrize(
    "columns", [None, ["year", "id"]], ids=["no_projection", "projection"]
)
def test_orc_reader_synthesizes_partition_and_path(tmp_path, columns):
    partition_dir = tmp_path / "year=2024"
    partition_dir.mkdir()
    path = partition_dir / "data.orc"
    _write_orc(path, pa.table({"id": [1, 2]}))
    # The datasource passes the final schema, including synthesized columns.
    schema = pa.schema(
        [("id", pa.int64()), ("path", pa.string()), ("year", pa.string())]
    )

    scanner = OrcScanner(
        schema=schema,
        partitioning=Partitioning(
            PartitionStyle.HIVE, base_dir=str(tmp_path), field_names=["year"]
        ),
        synthesized_columns=(PathColumn(),),
    )
    if columns is not None:
        scanner = scanner.prune_columns(columns)
    result = pa.concat_tables(list(scanner.create_reader().read(_manifest(path))))

    assert result.schema.names == scanner.read_schema().names
    assert result.column("year").to_pylist() == ["2024", "2024"]
    if columns is None:
        assert result.to_pylist() == [
            {"id": 1, "year": "2024", "path": str(path)},
            {"id": 2, "year": "2024", "path": str(path)},
        ]
    else:
        assert result.to_pylist() == [
            {"year": "2024", "id": 1},
            {"year": "2024", "id": 2},
        ]


def test_orc_reader_reports_corrupt_file_path(tmp_path):
    path = tmp_path / "broken.orc"
    path.write_bytes(b"not an ORC file")

    with pytest.raises((pa.ArrowInvalid, OSError)) as exc:
        list(
            OrcScanner(schema=pa.schema([("id", pa.int64())]))
            .create_reader()
            .read(_manifest(path))
        )

    assert str(path) in str(exc.value)


def test_orc_reader_rejects_pickle_object_columns(monkeypatch):
    storage = pa.array([b"payload"], type=pa.large_binary())
    extension = pa.ExtensionArray.from_storage(ArrowPythonObjectType(), storage)
    table = pa.table({"object": extension})
    reader = OrcFileReader(format=FileFormat.ORC)

    with pytest.raises(ValueError, match="arrow_pickled_object"):
        reader._on_batch_read(table)

    monkeypatch.setenv("RAY_DATA_AUTOLOAD_PICKLE_OBJECT_SCALAR", "1")
    reader._on_batch_read(table)


def test_orc_reader_empty_file_yields_no_batches(tmp_path):
    path = tmp_path / "empty.orc"
    table = pa.table({"id": pa.array([], type=pa.int64())})
    _write_orc(path, table)

    assert (
        list(OrcScanner(schema=table.schema).create_reader().read(_manifest(path)))
        == []
    )


def test_orc_reader_nested_struct(tmp_path):
    path = tmp_path / "nested.orc"
    payload_type = pa.struct([("values", pa.list_(pa.int32()))])
    expected = pa.table(
        {
            "id": [1, 2],
            "payload": pa.array(
                [{"values": [1, 2]}, {"values": [3]}], type=payload_type
            ),
        }
    )
    _write_orc(path, expected)

    batches = list(
        OrcScanner(schema=expected.schema).create_reader().read(_manifest(path))
    )

    assert pa.concat_tables(batches).equals(expected)


def test_orc_reader_projects_synthesized_path(tmp_path):
    path = tmp_path / "data.orc"
    _write_orc(path, pa.table({"id": [1, 2]}))
    schema = pa.schema([("id", pa.int64())])
    scanner = OrcScanner(
        schema=schema, synthesized_columns=(PathColumn(),)
    ).prune_columns(["path"])

    batches = list(scanner.create_reader().read(_manifest(path)))

    assert scanner.read_schema().names == ["path"]
    assert pa.concat_tables(batches).to_pylist() == [
        {"path": str(path)},
        {"path": str(path)},
    ]


def test_orc_reader_adds_synthesized_path_missing_from_schema(tmp_path):
    path = tmp_path / "data.orc"
    table = pa.table({"id": [1, 2]})
    _write_orc(path, table)
    scanner = OrcScanner(schema=table.schema, synthesized_columns=(PathColumn(),))

    result = pa.concat_tables(list(scanner.create_reader().read(_manifest(path))))

    assert scanner.read_schema().names == ["id", "path"]
    assert result.schema.names == scanner.read_schema().names
    assert result.to_pylist() == [
        {"id": 1, "path": str(path)},
        {"id": 2, "path": str(path)},
    ]


def test_orc_scanner_read_schema_projects_synthesized_column_missing_from_schema():
    scanner = OrcScanner(
        schema=pa.schema([("id", pa.int64())]),
        synthesized_columns=(PathColumn(),),
    ).prune_columns(["path"])

    assert scanner.read_schema() == pa.schema([("path", pa.string())])


def test_orc_scanner_read_schema_uses_synthesized_type_for_existing_field():
    schema = pa.schema([("id", pa.int64()), ("path", pa.int64())])
    scanner = OrcScanner(
        schema=schema, synthesized_columns=(PathColumn(),)
    ).prune_columns(["path"])

    assert scanner.read_schema() == pa.schema([("path", pa.string())])


def test_orc_reader_uses_synthesized_type_when_replacing_file_column(tmp_path):
    path = tmp_path / "data.orc"
    table = pa.table({"id": [1, 2], "path": [10, 20], "value": ["a", "b"]})
    _write_orc(path, table)

    scanner = OrcScanner(schema=table.schema, synthesized_columns=(PathColumn(),))
    batches = list(scanner.create_reader().read(_manifest(path)))
    result = pa.concat_tables(batches)

    assert scanner.read_schema().field("path").type == pa.string()
    assert scanner.read_schema().names == ["id", "path", "value"]
    assert result.schema.names == scanner.read_schema().names
    assert result.schema.field("path").type == pa.string()
    assert result.column("path").to_pylist() == [str(path), str(path)]
    assert result.column("value").to_pylist() == ["a", "b"]


@pytest.mark.parametrize("field_names", [None, ["year"]])
@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("root_has_year", [False, True])
@pytest.mark.parametrize("columns", [None, ["year", "id"], []])
def test_orc_partition_projection_preserves_mixed_file_values(
    tmp_path, field_names, reverse, root_has_year, columns
):
    root_path = tmp_path / "root.orc"
    partition_dir = tmp_path / "year=2024"
    partition_dir.mkdir()
    partition_path = partition_dir / "data.orc"
    root: dict[str, list[int] | list[str]] = {"id": [1]}
    if root_has_year:
        root["year"] = ["from-file"]
    _write_orc(root_path, pa.table(root))
    _write_orc(partition_path, pa.table({"id": [3]}))
    paths = [root_path, partition_path]
    if reverse:
        paths.reverse()
    scanner = OrcScanner(
        schema=pa.schema([("id", pa.int64()), ("year", pa.string())]),
        partitioning=Partitioning(
            PartitionStyle.HIVE, base_dir=str(tmp_path), field_names=field_names
        ),
    )
    if columns is not None:
        scanner = scanner.prune_columns(columns)
    batches = list(scanner.create_reader().read(_manifest(*paths)))
    assert sum(batch.num_rows for batch in batches) == 2
    result = pa.concat_tables(batches)
    if columns == []:
        assert result.column_names == [_BATCH_SIZE_PRESERVING_STUB_COL_NAME]
    else:
        assert result.column_names == scanner.read_schema().names
        assert sorted(result.to_pylist(), key=lambda row: row["id"]) == [
            {"id": 1, "year": "from-file" if root_has_year else None},
            {"id": 3, "year": "2024"},
        ]


@pytest.mark.parametrize("field_names", [None, ["year"]])
@pytest.mark.parametrize("columns", [None, ["id"], ["year", "id"]])
@pytest.mark.parametrize("file_year", ["2024", "from-file", None])
def test_orc_partition_file_columns_are_validated_before_projection(
    tmp_path, field_names, columns, file_year
):
    partition_dir = tmp_path / "year=2024"
    partition_dir.mkdir()
    path = partition_dir / "data.orc"
    table = pa.table({"id": [1], "year": pa.array([file_year], type=pa.string())})
    _write_orc(path, table)
    scanner = OrcScanner(
        schema=table.schema,
        partitioning=Partitioning(
            PartitionStyle.HIVE, base_dir=str(tmp_path), field_names=field_names
        ),
    )
    if columns is not None:
        scanner = scanner.prune_columns(columns)
    if file_year != "2024":
        with pytest.raises(ValueError, match="Partition column year"):
            list(scanner.create_reader().read(_manifest(path)))
    else:
        result = pa.concat_tables(list(scanner.create_reader().read(_manifest(path))))
        assert result.column_names == scanner.read_schema().names
        assert result.column("id").to_pylist() == [1]
        if columns != ["id"]:
            assert result.column("year").to_pylist() == ["2024"]


def test_orc_partitioned_schema_keeps_null_values_and_typed_partition(tmp_path):
    first_dir = tmp_path / "year=2023"
    first_dir.mkdir()
    root_path = first_dir / "data.orc"
    partition_dir = tmp_path / "year=2024"
    partition_dir.mkdir()
    path = partition_dir / "data.orc"
    _write_orc(
        root_path, pa.table({"id": [1], "value": pa.array([None], type=pa.string())})
    )
    _write_orc(path, pa.table({"id": [2], "value": ["two"]}))
    scanner = OrcScanner(
        schema=pa.schema(
            [("id", pa.int64()), ("value", pa.string()), ("year", pa.int64())]
        ),
        partitioning=Partitioning(
            PartitionStyle.HIVE, base_dir=str(tmp_path), field_types={"year": int}
        ),
    ).prune_columns(["year", "value", "id"])
    result = pa.concat_tables(
        list(scanner.create_reader().read(_manifest(root_path, path)))
    )
    assert result.schema == scanner.read_schema()
    assert result.to_pylist() == [
        {"year": 2023, "value": None, "id": 1},
        {"year": 2024, "value": "two", "id": 2},
    ]


def test_orc_partition_filters_are_accepted_without_path_only_pruning():
    scanner = OrcScanner(
        schema=pa.schema([("id", pa.int64()), ("year", pa.string())]),
        partitioning=Partitioning(PartitionStyle.HIVE, field_names=["year"]),
    )
    predicate = (col("year") == "2024") & (col("id") > 1)
    pushed, residual = scanner.push_filters(predicate)
    assert pushed.predicate is not None
    assert pushed.predicate.structurally_equals(predicate)
    assert residual is None
    assert scanner.partition_columns == set()


@pytest.fixture
def orc_read_requests(monkeypatch):
    """Record real Arrow scanner and ORC stripe requests without replacing I/O."""
    requests = {"scans": [], "stripes": []}
    native_dataset = pds.dataset
    native_orc_file = orc.ORCFile

    class Fragment:
        def __init__(self, fragment):
            self._fragment = fragment

        def __getattr__(self, name):
            return getattr(self._fragment, name)

        def scanner(self, **kwargs):
            requests["scans"].append(
                {
                    "path": getattr(self._fragment, "path", None),
                    "columns": kwargs.get("columns"),
                    "filter": kwargs.get("filter"),
                }
            )
            return self._fragment.scanner(**kwargs)

    class Dataset:
        def __init__(self, dataset):
            self._dataset = dataset

        def __getattr__(self, name):
            return getattr(self._dataset, name)

        def get_fragments(self, *args, **kwargs):
            return (
                Fragment(fragment)
                for fragment in self._dataset.get_fragments(*args, **kwargs)
            )

    class OrcFile:
        def __init__(self, source):
            self._file = native_orc_file(source)

        def __getattr__(self, name):
            return getattr(self._file, name)

        def read_stripe(self, stripe, columns=None):
            requests["stripes"].append((stripe, columns))
            return self._file.read_stripe(stripe, columns=columns)

    monkeypatch.setattr(
        pds, "dataset", lambda *a, **kw: Dataset(native_dataset(*a, **kw))
    )
    monkeypatch.setattr(orc, "ORCFile", OrcFile)
    return requests


@pytest.mark.parametrize("stored_partition", [False, True])
@pytest.mark.parametrize("columns", [["id"], ["year"], []])
def test_partitioned_orc_decodes_only_required_columns(
    tmp_path, orc_read_requests, stored_partition, columns
):
    directory = tmp_path / "year=2024"
    directory.mkdir()
    path = directory / "data.orc"
    payload: list[str] = ["large" * 1024] * 2
    data: dict[str, list[int] | list[str]] = {
        "id": [1, 2],
        "payload": payload,
    }
    if stored_partition:
        data["year"] = ["2024", "2024"]
    _write_orc(path, pa.table(data))
    scanner = OrcScanner(
        schema=pa.schema(
            [("id", pa.int64()), ("payload", pa.string()), ("year", pa.string())]
        ),
        partitioning=Partitioning(PartitionStyle.HIVE),
    ).prune_columns(columns)
    batches = list(scanner.create_reader().read(_manifest(path)))
    assert sum(batch.num_rows for batch in batches) == 2
    if stored_partition:
        assert len(orc_read_requests["stripes"]) == 1
        assert set(orc_read_requests["stripes"][0][1]) == set(columns) | {"year"}
    else:
        assert orc_read_requests["stripes"] == []
        assert orc_read_requests["scans"][0]["columns"] == [
            name for name in columns if name != "year"
        ]
    assert all(
        "payload" not in request["columns"] for request in orc_read_requests["scans"]
    )
    if columns:
        result = pa.concat_tables(batches)
        assert result.column_names == columns
        assert result.column(columns[0]).to_pylist() == (
            [1, 2] if columns == ["id"] else ["2024", "2024"]
        )


@pytest.mark.parametrize("stored_partition", [False, True])
def test_partitioned_orc_prunes_data_without_bypassing_validation(
    tmp_path, orc_read_requests, stored_partition
):
    directory = tmp_path / "year=2023"
    directory.mkdir()
    path = directory / "data.orc"
    payload: list[str] = ["large" * 1024] * 2
    data: dict[str, list[int] | list[str]] = {
        "id": [1, 2],
        "payload": payload,
    }
    if stored_partition:
        data["year"] = ["2023", "2023"]
    _write_orc(path, pa.table(data))
    scanner = OrcScanner(
        schema=pa.schema(
            [("id", pa.int64()), ("payload", pa.string()), ("year", pa.string())]
        ),
        predicate=col("year") == "2024",
        partitioning=Partitioning(PartitionStyle.HIVE),
    )
    assert list(scanner.create_reader().read(_manifest(path))) == []
    assert orc_read_requests["scans"] == []
    assert orc_read_requests["stripes"] == ([(0, ["year"])] if stored_partition else [])


@pytest.mark.parametrize("predicate", [col("id") > 100, col("year") == "2025"])
@pytest.mark.parametrize("file_year", ["wrong", None])
def test_orc_filter_cannot_hide_stored_partition_conflicts(
    tmp_path, predicate, file_year
):
    directory = tmp_path / "year=2024"
    directory.mkdir()
    path = directory / "data.orc"
    _write_orc(
        path,
        pa.table({"id": [1], "year": pa.array([file_year], type=pa.string())}),
    )
    scanner = OrcScanner(
        schema=pa.schema([("id", pa.int64()), ("year", pa.string())]),
        predicate=predicate,
        partitioning=Partitioning(PartitionStyle.HIVE),
    ).prune_columns(["id"])
    with pytest.raises(ValueError, match="Partition column year"):
        list(scanner.create_reader().read(_manifest(path)))


@pytest.mark.parametrize("stored_partition", [False, True])
def test_orc_predicate_columns_survive_projection_until_filtering(
    tmp_path, orc_read_requests, stored_partition
):
    directory = tmp_path / "year=2024"
    directory.mkdir()
    path = directory / "data.orc"
    data: dict[str, list[int] | list[str] | pa.Array] = {
        "id": [1, 2, 3],
        "payload": ["unused"] * 3,
    }
    if stored_partition:
        data["year"] = pa.array(["2024", None, "2024"], type=pa.string())
    _write_orc(path, pa.table(data))
    scanner = OrcScanner(
        schema=pa.schema(
            [("id", pa.int64()), ("payload", pa.string()), ("year", pa.string())]
        ),
        predicate=col("id") > 1,
        batch_size=1,
        limit=1,
        partitioning=Partitioning(PartitionStyle.HIVE),
    ).prune_columns(["year"])
    result = pa.concat_tables(list(scanner.create_reader().read(_manifest(path))))
    assert result.to_pylist() == [{"year": "2024"}]
    assert all(request["filter"] is not None for request in orc_read_requests["scans"])
    if stored_partition:
        assert set(orc_read_requests["stripes"][0][1]) == {"id", "year"}
    else:
        assert orc_read_requests["scans"][0]["columns"] == ["id"]


@pytest.mark.parametrize("field_names", [None, ["year"]])
@pytest.mark.parametrize("root_has_year", [False, True])
@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize(
    "predicate,with_root_year,without_root_year",
    [
        (col("year") == "2024", [3], [3]),
        ((col("year") == "2024") | (col("id") == 1), [1, 3], [1, 3]),
        ((col("year") == "2024") & (col("id") > 1), [3], [3]),
        (~(col("year") == "2024"), [1], []),
        (col("year").is_null(), [], [1]),
    ],
)
def test_orc_partition_predicates_preserve_mixed_root_files(
    tmp_path,
    field_names,
    root_has_year,
    reverse,
    predicate,
    with_root_year,
    without_root_year,
):
    root = tmp_path / "root.orc"
    directory = tmp_path / "year=2024"
    directory.mkdir()
    partitioned = directory / "data.orc"
    data: dict[str, list[int] | list[str]] = {"id": [1]}
    if root_has_year:
        data["year"] = ["from-file"]
    _write_orc(root, pa.table(data))
    _write_orc(partitioned, pa.table({"id": [3]}))
    paths = [partitioned, root] if reverse else [root, partitioned]
    scanner = OrcScanner(
        schema=pa.schema([("id", pa.int64()), ("year", pa.string())]),
        predicate=predicate,
        partitioning=Partitioning(PartitionStyle.HIVE, field_names=field_names),
    ).prune_columns(["id"])
    batches = list(scanner.create_reader().read(_manifest(*paths)))
    ids = sorted(row["id"] for batch in batches for row in batch.to_pylist())
    assert ids == (with_root_year if root_has_year else without_root_year)


def test_orc_partition_pruning_uses_output_types(tmp_path, orc_read_requests):
    directory = tmp_path / "year=2024"
    directory.mkdir()
    path = directory / "data.orc"
    _write_orc(path, pa.table({"id": [1, 2]}))
    scanner = OrcScanner(
        schema=pa.schema([("id", pa.int64()), ("year", pa.int64())]),
        predicate=col("year") > 2023,
        partitioning=Partitioning(PartitionStyle.HIVE),
    ).prune_columns(["year"])
    result = pa.concat_tables(list(scanner.create_reader().read(_manifest(path))))
    assert result.to_pylist() == [{"year": 2024}, {"year": 2024}]
    assert orc_read_requests["scans"][0]["columns"] == []


@pytest.mark.parametrize("field_names", [None, ["key"]])
@pytest.mark.parametrize(
    "operation", ["full", "project", "filter", "filter_only", "empty"]
)
@pytest.mark.parametrize(
    "physical_type,logical_type,path_value,stored_value,expected,field_type",
    [
        pytest.param(
            pa.string(), pa.string(), "north", "north", "north", str, id="string"
        ),
        pytest.param(pa.int32(), pa.int32(), "42", 42, 42, str, id="int32"),
        pytest.param(pa.bool_(), pa.bool_(), "TRUE", True, True, bool, id="typed-bool"),
        pytest.param(
            pa.int32(),
            pa.int64(),
            "2147483647",
            2147483647,
            2147483647,
            int,
            id="integer-widening",
        ),
        pytest.param(
            pa.float32(), pa.float64(), "0.1", 0.1, 0.1, str, id="float-widening"
        ),
        pytest.param(
            pa.float32(),
            pa.float64(),
            "-1e-20",
            -1e-20,
            -1e-20,
            float,
            id="typed-small-float",
        ),
        pytest.param(
            pa.float64(),
            pa.float32(),
            "0.1",
            0.1,
            0.10000000149011612,
            str,
            id="float-narrowing",
        ),
        pytest.param(
            pa.int64(),
            pa.float64(),
            "9007199254740993",
            9007199254740993,
            9007199254740992.0,
            str,
            id="integer-to-float",
        ),
        pytest.param(
            pa.int32(), pa.string(), "00042", 42, "00042", str, id="integer-to-string"
        ),
        pytest.param(
            pa.string(), pa.int64(), "00042", "00042", 42, str, id="string-to-integer"
        ),
        pytest.param(
            pa.float32(),
            pa.string(),
            "0.1",
            0.1,
            "0.1",
            float,
            id="typed-float-to-string",
        ),
        pytest.param(
            pa.decimal128(6, 2),
            pa.decimal128(10, 4),
            "1.2300",
            Decimal("1.23"),
            Decimal("1.2300"),
            str,
            id="decimal-scale",
        ),
        pytest.param(
            pa.decimal128(6, 2),
            pa.string(),
            "1.2300",
            Decimal("1.23"),
            "1.2300",
            str,
            id="decimal-to-string",
        ),
        pytest.param(
            pa.date32(),
            pa.date32(),
            "2024-01-02",
            date(2024, 1, 2),
            date(2024, 1, 2),
            str,
            id="date",
        ),
        pytest.param(
            pa.timestamp("ns"),
            pa.timestamp("ms"),
            "2024-01-02 03:04:05.123",
            datetime(2024, 1, 2, 3, 4, 5, 123000),
            datetime(2024, 1, 2, 3, 4, 5, 123000),
            str,
            id="timestamp-unit",
        ),
    ],
)
def test_orc_stored_partition_uses_logical_values_before_filter(
    tmp_path,
    field_names,
    operation,
    physical_type,
    logical_type,
    path_value,
    stored_value,
    expected,
    field_type,
):
    directory = tmp_path / f"key={path_value}"
    directory.mkdir()
    path = directory / "data.orc"
    _write_orc(
        path,
        pa.table(
            {
                "id": [1, 2, 3],
                "key": pa.array([stored_value, None, stored_value], type=physical_type),
            }
        ),
    )
    scanner = OrcScanner(
        schema=pa.schema([("id", pa.int64()), ("key", logical_type)]),
        batch_size=1,
        partitioning=Partitioning(
            PartitionStyle.HIVE,
            field_names=field_names,
            field_types={"key": field_type},
        ),
    )
    if operation in {"filter", "filter_only", "empty"}:
        scanner, residual = scanner.push_filters(
            (col("key") == expected) | (col("id") < 0)
        )
        assert residual is None
    if operation in {"project", "filter"}:
        scanner = scanner.prune_columns(["key"])
    elif operation == "filter_only":
        scanner = scanner.prune_columns(["id"]).push_limit(2)
    elif operation == "empty":
        scanner = scanner.prune_columns([])
    batches = list(scanner.create_reader().read(_manifest(path)))
    assert sum(batch.num_rows for batch in batches) == (
        2 if operation == "filter_only" else 3
    )
    result = pa.concat_tables(batches)
    if operation == "filter_only":
        assert result.to_pylist() == [{"id": 1}, {"id": 2}]
    elif operation == "empty":
        assert result.column_names == [_BATCH_SIZE_PRESERVING_STUB_COL_NAME]
    else:
        assert result.column("key").type == logical_type
        assert result.column("key").to_pylist() == [expected] * 3


@pytest.mark.parametrize("columns", [None, ["key"], ["id"]])
@pytest.mark.parametrize(
    "physical_type,path_value,stored_value",
    [(pa.int32(), "00042", 42), (pa.float32(), "0.1", 0.1), (pa.bool_(), "true", True)],
)
def test_orc_stored_partition_without_schema_uses_path_strings(
    tmp_path,
    columns,
    physical_type,
    path_value,
    stored_value,
):
    directory = tmp_path / f"key={path_value}"
    directory.mkdir()
    path = directory / "data.orc"
    _write_orc(
        path,
        pa.table(
            {"id": [1, 2], "key": pa.array([stored_value, None], type=physical_type)}
        ),
    )
    reader = OrcFileReader(
        format=FileFormat.ORC,
        columns=columns,
        partitioning=Partitioning(PartitionStyle.HIVE),
        predicate=(col("key") == path_value) | (col("id") < 0),
    )
    result = pa.concat_tables(list(reader.read(_manifest(path))))
    assert result.num_rows == 2
    if columns != ["id"]:
        assert result.column("key").type == pa.string()
        assert result.column("key").to_pylist() == [path_value] * 2


@pytest.mark.parametrize("predicate", [None, col("id") < 0, col("key") == "excluded"])
@pytest.mark.parametrize(
    "physical_type,path_value,stored_value,message",
    [
        (pa.int8(), "256", 1, "cannot be cast"),
        (pa.float32(), "0.1", 0.2, "Partition column key"),
    ],
)
def test_orc_logical_partition_synthesis_cannot_hide_invalid_physical_values(
    tmp_path,
    predicate,
    physical_type,
    path_value,
    stored_value,
    message,
):
    directory = tmp_path / f"key={path_value}"
    directory.mkdir()
    path = directory / "data.orc"
    _write_orc(
        path, pa.table({"id": [1], "key": pa.array([stored_value], type=physical_type)})
    )
    scanner = OrcScanner(
        schema=pa.schema([("id", pa.int64()), ("key", pa.string())]),
        predicate=predicate,
        partitioning=Partitioning(PartitionStyle.HIVE),
    ).prune_columns(["id"])
    with pytest.raises(ValueError, match=message):
        list(scanner.create_reader().read(_manifest(path)))


@pytest.mark.parametrize(
    "path_value,logical_type", [("128", pa.int8()), ("not-an-integer", pa.int64())]
)
def test_orc_invalid_logical_partition_cast_is_not_hidden_by_data_filter(
    tmp_path,
    path_value,
    logical_type,
):
    directory = tmp_path / f"key={path_value}"
    directory.mkdir()
    path = directory / "data.orc"
    _write_orc(path, pa.table({"id": [1], "key": [path_value]}))
    scanner = OrcScanner(
        schema=pa.schema([("id", pa.int64()), ("key", logical_type)]),
        predicate=col("id") < 0,
        partitioning=Partitioning(PartitionStyle.HIVE),
    ).prune_columns(["key"])
    with pytest.raises(pa.ArrowInvalid):
        list(scanner.create_reader().read(_manifest(path)))


def test_orc_partition_predicate_type_errors_are_not_pruned(tmp_path):
    directory = tmp_path / "year=2024"
    directory.mkdir()
    path = directory / "data.orc"
    _write_orc(path, pa.table({"id": [1]}))
    scanner = OrcScanner(
        schema=pa.schema([("id", pa.int64()), ("year", pa.string())]),
        predicate=col("year") > 2023,
        partitioning=Partitioning(PartitionStyle.HIVE),
    )
    with pytest.raises((pa.ArrowInvalid, pa.ArrowNotImplementedError)):
        list(scanner.create_reader().read(_manifest(path)))


@pytest.mark.parametrize("predicate", [col("id") > 100, col("year") == "2025"])
def test_orc_validates_all_stored_partition_keys(tmp_path, predicate):
    directory = tmp_path / "year=2024" / "month=01"
    directory.mkdir(parents=True)
    path = directory / "data.orc"
    _write_orc(path, pa.table({"id": [1], "month": ["wrong"]}))
    scanner = OrcScanner(
        schema=pa.schema(
            [("id", pa.int64()), ("year", pa.string()), ("month", pa.string())]
        ),
        predicate=predicate,
        partitioning=Partitioning(PartitionStyle.HIVE),
    ).prune_columns(["id"])
    with pytest.raises(ValueError, match="Partition column month"):
        list(scanner.create_reader().read(_manifest(path)))


@pytest.mark.parametrize("stored_partition", [False, True])
@pytest.mark.parametrize(
    "predicate,expected", [(col("extra").is_null(), [1]), (col("extra") == "x", [])]
)
def test_orc_missing_predicate_column_is_null_filled(
    tmp_path, stored_partition, predicate, expected
):
    directory = tmp_path / "year=2024"
    directory.mkdir()
    path = directory / "data.orc"
    data: dict[str, list[int] | list[str]] = {"id": [1]}
    if stored_partition:
        data["year"] = ["2024"]
    _write_orc(path, pa.table(data))
    scanner = OrcScanner(
        schema=pa.schema(
            [("id", pa.int64()), ("year", pa.string()), ("extra", pa.string())]
        ),
        predicate=predicate,
        partitioning=Partitioning(PartitionStyle.HIVE),
    ).prune_columns(["id"])
    batches = list(scanner.create_reader().read(_manifest(path)))
    assert [row["id"] for batch in batches for row in batch.to_pylist()] == expected


def test_orc_filter_cannot_hide_decoded_pickle_column(tmp_path):
    physical_schema = pa.schema(
        [("id", pa.int64()), ("object", ArrowPythonObjectType())]
    )

    class Fragment:
        path = str(tmp_path / "root.orc")

        def __init__(self):
            self.physical_schema = physical_schema

        def scanner(self, **kwargs):
            pytest.fail("Unsafe decoded columns must be rejected before scanning")

    fragment = Fragment()
    reader = OrcScanner(
        schema=physical_schema,
        predicate=col("id") > 100,
        partitioning=Partitioning(PartitionStyle.HIVE),
    ).create_reader()
    with pytest.raises(ValueError, match="arrow_pickled_object"):
        list(
            reader._iter_fragment_tables(
                fragment, {"filter": (col("id") > 100).to_pyarrow()}
            )
        )


@pytest.mark.parametrize("mixed_or", [False, True])
def test_orc_synthesized_path_filters_stay_residual(mixed_or):
    scanner = OrcScanner(
        schema=pa.schema([("id", pa.int64())]),
        partitioning=Partitioning(PartitionStyle.HIVE),
        synthesized_columns=(PathColumn(),),
    )
    path_predicate = col("path") == "file.orc"
    predicate = (
        path_predicate | (col("id") > 1)
        if mixed_or
        else path_predicate & (col("id") > 1)
    )
    pushed, residual = scanner.push_filters(predicate)
    assert residual is not None
    if mixed_or:
        assert pushed is scanner
        assert residual.structurally_equals(predicate)
    else:
        assert pushed.predicate is not None
        assert pushed.predicate.structurally_equals(col("id") > 1)
        assert residual.structurally_equals(path_predicate)


@pytest.mark.parametrize("batch_size", [1, 2])
@pytest.mark.parametrize("values", [["2024", None], [None, "2024"]])
@pytest.mark.parametrize("columns", [None, ["year", "id"], ["id"], []])
def test_orc_nullable_partition_column_matches_v1_across_batches(
    tmp_path, batch_size, values, columns
):
    from ray.data._internal.datasource.orc_datasource import ORCDatasource
    from ray.data.datasource.file_based_datasource import _add_partitions_to_table

    partition_dir = tmp_path / "year=2024"
    partition_dir.mkdir()
    path = partition_dir / "data.orc"
    table = pa.table({"id": [1, 2], "year": pa.array(values, type=pa.string())})
    _write_orc(path, table)
    assert orc.ORCFile(str(path)).nstripes == 1
    legacy = ORCDatasource.__new__(ORCDatasource)
    with pa.OSFile(str(path), "rb") as source:
        expected = pa.concat_tables(
            [
                _add_partitions_to_table(stripe, {"year": "2024"})
                for stripe in legacy._read_stream(source, str(path))
            ]
        )

    scanner = OrcScanner(
        schema=table.schema,
        batch_size=batch_size,
        partitioning=Partitioning(PartitionStyle.HIVE, base_dir=str(tmp_path)),
    )
    if columns is not None:
        scanner = scanner.prune_columns(columns)
    batches = list(scanner.create_reader().read(_manifest(path)))
    assert all(batch.num_rows <= batch_size for batch in batches)
    assert sum(batch.num_rows for batch in batches) == expected.num_rows
    if columns == []:
        assert all(
            batch.column_names == [_BATCH_SIZE_PRESERVING_STUB_COL_NAME]
            for batch in batches
        )
    else:
        result = pa.concat_tables(batches)
        assert result.equals(expected if columns is None else expected.select(columns))


def test_orc_nullable_partition_column_aligns_missing_schema_fields(tmp_path):
    partition_dir = tmp_path / "year=2024"
    partition_dir.mkdir()
    path = partition_dir / "data.orc"
    _write_orc(
        path,
        pa.table({"id": [1, 2], "year": pa.array(["2024", None], type=pa.string())}),
    )
    scanner = OrcScanner(
        schema=pa.schema(
            [("id", pa.int64()), ("year", pa.string()), ("extra", pa.string())]
        ),
        batch_size=1,
        partitioning=Partitioning(PartitionStyle.HIVE, base_dir=str(tmp_path)),
    )
    result = pa.concat_tables(list(scanner.create_reader().read(_manifest(path))))
    assert result.schema == scanner.read_schema()
    assert result.to_pylist() == [
        {"id": 1, "year": "2024", "extra": None},
        {"id": 2, "year": "2024", "extra": None},
    ]


def test_orc_partitioned_empty_file_with_stored_partition_column(tmp_path):
    partition_dir = tmp_path / "year=2024"
    partition_dir.mkdir()
    path = partition_dir / "data.orc"
    table = pa.table(
        {"id": pa.array([], type=pa.int64()), "year": pa.array([], type=pa.string())}
    )
    _write_orc(path, table)
    scanner = OrcScanner(
        schema=table.schema,
        partitioning=Partitioning(PartitionStyle.HIVE, base_dir=str(tmp_path)),
    )
    assert list(scanner.create_reader().read(_manifest(path))) == []


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
