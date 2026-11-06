"""Unit tests for the Delta Lake ``DataSourceV2`` (no Ray cluster)."""

import sys
from types import SimpleNamespace
from typing import List, Optional, Sequence

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import ray.cloudpickle as cloudpickle
from ray.data._internal.datasource_v2.common.online_bin_packer import (
    OnlineBinPacker,
)
from ray.data._internal.datasource_v2.formats.delta.delta_datasource_v2 import (
    DeltaDatasourceV2,
    delta_datasource_v2_unsupported_reason,
)
from ray.data._internal.datasource_v2.formats.delta.delta_file_indexer import (
    DeltaFileIndexer,
    resolve_data_file_path,
)
from ray.data._internal.datasource_v2.formats.delta.delta_scanner import (
    DeltaScanner,
)
from ray.data._internal.datasource_v2.interfaces.datasource_v2 import (
    DatasourceCategory,
)
from ray.data.datasource.file_based_datasource import FileShuffleConfig
from ray.data.datasource.partitioning import HIVE_DEFAULT_PARTITION

deltalake = pytest.importorskip("deltalake")


def _write(path: str, table: pa.Table, **kwargs) -> None:
    deltalake.write_deltalake(path, table, **kwargs)


def _open(path: str, version: Optional[int] = None) -> "deltalake.DeltaTable":
    return deltalake.DeltaTable(path, version=version, without_files=True)


def _datasource(path: str, **kwargs) -> DeltaDatasourceV2:
    return DeltaDatasourceV2(_open(path, kwargs.pop("version", None)), **kwargs)


def _listed_paths(indexer, **kwargs) -> List[str]:
    return [
        info.path for info in indexer.list_file_infos(pa.array(["label"]), **kwargs)
    ]


def _log_paths(path: str, version: Optional[int] = None) -> List[str]:
    table = deltalake.DeltaTable(path, version=version)
    return sorted(
        resolve_data_file_path(p)
        for p in pa.table(table.get_add_actions(flatten=True))["path"].to_pylist()
    )


@pytest.fixture
def partitioned_table(tmp_path) -> str:
    path = str(tmp_path / "partitioned")
    _write(
        path,
        pa.table(
            {
                "id": [1, 2, 3, 4],
                "grp": ["a b", "e=f", None, "a b"],
                "value": [1.0, 2.0, 3.0, 4.0],
            }
        ),
        partition_by=["grp"],
    )
    return path


def test_datasource_lists_the_pinned_version(tmp_path):
    path = str(tmp_path / "table")
    _write(path, pa.table({"x": [1, 2]}))
    _write(path, pa.table({"x": [3]}), mode="append")

    datasource = _datasource(path, version=0)
    indexer = datasource._get_file_indexer()

    assert isinstance(indexer, DeltaFileIndexer)
    assert datasource.version == 0
    assert _listed_paths(indexer) == _log_paths(path, version=0)


def test_datasource_takes_schema_and_partitioning_from_the_log(partitioned_table):
    datasource = _datasource(partitioned_table)

    assert datasource.category == DatasourceCategory.DATA_LAKE
    assert datasource.schema_needs_file_sample is False
    schema = datasource.infer_schema(None)
    assert schema.names == ["id", "grp", "value"]
    assert schema.field("grp").type == pa.string()

    scanner = datasource.create_scanner(schema=schema, partitioning=None)

    assert isinstance(scanner, DeltaScanner)
    assert scanner.partitioning is not None
    assert scanner.partitioning.field_names == ["grp"]
    assert scanner.partitioning.null_fallback == HIVE_DEFAULT_PARTITION
    # Pinned so the partition column keeps its place in the output.
    assert scanner.columns == ("id", "grp", "value")
    assert scanner.read_schema().names == ["id", "grp", "value"]


@pytest.mark.parametrize(
    "columns, include_paths, expected",
    [
        (["value", "grp"], False, ("value", "grp")),
        (["value"], True, ("value", "path")),
        (None, True, ("id", "grp", "value", "path")),
        # ``path`` comes last, as with the V1 reader.
        (["path", "value"], True, ("value", "path")),
    ],
)
def test_datasource_projects_columns_in_order(
    partitioned_table, columns, include_paths, expected
):
    datasource = _datasource(
        partitioned_table, columns=columns, include_paths=include_paths
    )
    schema = datasource.infer_schema(None)

    scanner = datasource.create_scanner(schema=schema)

    assert scanner.columns == expected
    assert tuple(scanner.read_schema().names) == expected


def test_datasource_rejects_unknown_columns(partitioned_table):
    with pytest.raises(ValueError, match=r"\['missing'\] don't exist"):
        _datasource(partitioned_table, columns=["id", "missing"])
    # ``path`` exists only with ``include_paths``.
    with pytest.raises(ValueError, match=r"\['path'\] don't exist"):
        _datasource(partitioned_table, columns=["id", "path"])


def test_datasource_paths_and_partitioner(partitioned_table):
    datasource = _datasource(partitioned_table, shuffle="files")

    assert len(datasource.paths) == 1
    assert isinstance(datasource.get_file_partitioner(), OnlineBinPacker)
    # The framework doesn't shuffle a metadata-backed source's listing, so the
    # indexer does, with a seed drawn when the dataset is created.
    shuffle_config = datasource._get_file_indexer()._shuffle_config
    assert isinstance(shuffle_config, FileShuffleConfig)
    assert shuffle_config.seed is not None


def test_indexer_and_scanner_survive_worker_serialization(partitioned_table):
    """Listing and read tasks receive the indexer and the scanner pickled, and
    a read task creates its reader from the scanner."""
    datasource = _datasource(partitioned_table)
    scanner = datasource.create_scanner(schema=datasource.infer_schema(None))

    indexer = cloudpickle.loads(cloudpickle.dumps(datasource._get_file_indexer()))
    reader = cloudpickle.loads(cloudpickle.dumps(scanner)).create_reader()

    assert _listed_paths(indexer) == _log_paths(partitioned_table)
    manifests = indexer.as_whole_file_indexer().list_files(
        pa.array(["label"]), filesystem=None
    )
    rows = [row for m in manifests for t in reader.read(m) for row in t.to_pylist()]
    assert sorted(row["id"] for row in rows) == [1, 2, 3, 4]
    assert {row["grp"] for row in rows} == {"a b", "e=f", None}


def test_reads_through_a_filesystem_rooted_at_the_table(partitioned_table):
    """The default filesystem reads the relative paths the indexer lists."""
    datasource = _datasource(partitioned_table)
    (path, *_) = _listed_paths(datasource._get_file_indexer())

    with datasource._filesystem.open_input_file(path) as f:
        assert pq.read_metadata(f).num_rows > 0


def _stub_table(
    *,
    reader_version: int = 1,
    reader_features: Optional[List[str]] = None,
    configuration: Optional[dict] = None,
    partition_columns: Optional[List[str]] = None,
    column_names: Sequence[str] = ("x",),
    schema: Optional[pa.Schema] = None,
) -> SimpleNamespace:
    """A stand-in for ``DeltaTable`` with just what the eligibility check reads."""
    if schema is None:
        schema = pa.schema([(name, pa.int64()) for name in column_names])
    return SimpleNamespace(
        protocol=lambda: SimpleNamespace(
            min_reader_version=reader_version, reader_features=reader_features
        ),
        metadata=lambda: SimpleNamespace(
            configuration=configuration or {},
            partition_columns=partition_columns or [],
        ),
        schema=lambda: SimpleNamespace(to_arrow=lambda: schema),
    )


@pytest.mark.parametrize(
    "table, kwargs, expected",
    [
        (_stub_table(), {}, None),
        (_stub_table(reader_version=3, reader_features=["timestampNtz"]), {}, None),
        (_stub_table(), {"arrow_parquet_args": {"batch_size": 10}}, "PyArrow"),
        (_stub_table(reader_version=2), {}, "reader version 2"),
        # Only the unsupported features are named.
        (
            _stub_table(
                reader_version=3, reader_features=["timestampNtz", "deletionVectors"]
            ),
            {},
            "reader features ['deletionVectors']",
        ),
        (
            _stub_table(configuration={"delta.columnMapping.mode": "name"}),
            {},
            "column mapping",
        ),
        (
            _stub_table(configuration={"delta.columnMapping.mode": "none"}),
            {},
            None,
        ),
        (
            _stub_table(
                configuration={"delta.randomizeFilePrefixes": "true"},
                partition_columns=["p"],
            ),
            {},
            "random prefixes",
        ),
        # Random prefixes don't matter without partition columns.
        (
            _stub_table(configuration={"delta.randomizeFilePrefixes": "true"}),
            {},
            None,
        ),
        (
            _stub_table(
                schema=pa.schema([("x", pa.int64()), ("t", pa.timestamp("us", "UTC"))]),
                partition_columns=["t"],
            ),
            {},
            "time zone",
        ),
        # Without a time zone the directory value casts, and data columns
        # aren't parsed from paths.
        (
            _stub_table(
                schema=pa.schema(
                    [("t", pa.timestamp("us")), ("u", pa.timestamp("us", "UTC"))]
                ),
                partition_columns=["t"],
            ),
            {},
            None,
        ),
        (_stub_table(column_names=["path"]), {"include_paths": True}, "`path`"),
        (_stub_table(column_names=["path"]), {}, None),
        (SimpleNamespace(), {}, "couldn't be inspected"),
    ],
)
def test_delta_datasource_v2_unsupported_reason(table, kwargs, expected):
    reason = delta_datasource_v2_unsupported_reason(
        table,
        include_paths=kwargs.get("include_paths", False),
        arrow_parquet_args=kwargs.get("arrow_parquet_args", {}),
    )

    if expected is None:
        assert reason is None
    else:
        assert reason is not None and expected in reason


def test_generated_id_column_is_rejected(partitioned_table, tmp_path):
    from ray.data._internal.logical.interfaces import LogicalPlan
    from ray.data._internal.logical.operators import InputData, ReadFiles
    from ray.data._internal.logical.rules.bind_generated_id_column import (
        BindGeneratedIdColumn,
    )
    from ray.data.checkpoint import CheckpointConfig
    from ray.data.checkpoint.interfaces import InvalidCheckpointingConfig
    from ray.data.context import DataContext

    datasource = _datasource(partitioned_table)
    schema = datasource.infer_schema(None)
    read_files = ReadFiles(
        datasource_name=datasource.name,
        scanner=datasource.create_scanner(schema=schema),
        schema=schema,
        parallelism=-1,
        input_dependencies=[InputData(input_data=[])],
    )
    context = DataContext.get_current().copy()
    context.checkpoint_config = CheckpointConfig(
        checkpoint_path=str(tmp_path / "ckpt"), generated_id_column="generated_id"
    )

    with pytest.raises(InvalidCheckpointingConfig, match="read_delta"):
        BindGeneratedIdColumn().apply(LogicalPlan(dag=read_files, context=context))


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
