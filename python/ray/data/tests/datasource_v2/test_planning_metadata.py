import os
from collections import Counter
from dataclasses import replace

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import pytest

import ray
from ray.data._internal.datasource_v2.common import schema_inference as inference
from ray.data._internal.datasource_v2.common.file_metadata import (
    get_cached_file_metadata,
)
from ray.data._internal.datasource_v2.common.listing_utils import sample_files
from ray.data._internal.datasource_v2.common.non_sampling_file_indexer import (
    NonSamplingFileIndexer,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_datasource_v2 import (
    ParquetDatasourceV2,
    _read_parquet_metadata,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_file_reader import (
    ParquetFileReader,
)
from ray.data._internal.datasource_v2.interfaces.file_indexer import FileInfo
from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    FILE_MTIME_COLUMN_NAME,
    FILE_SIZE_COLUMN_NAME,
    FileManifest,
    FileMetadata,
)
from ray.data.tests.schema_planning_utils import (
    RecordingLocalFileSystem,
    UnknownSizeRecordingLocalFileSystem,
    read_schema_records as _records,
    recording_filesystem as _filesystem,
)
from ray.util.placement_group import (
    placement_group,
    placement_group_table,
    remove_placement_group,
)
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy


@pytest.mark.parametrize("datasource_v2", [True], indirect=True)
def test_metadata_count_preserves_storage_access(
    monkeypatch, tmp_path, ray_start_cluster, datasource_v2
):
    cluster = ray_start_cluster
    for _ in range(2):
        cluster.add_node(num_cpus=1, object_store_memory=100 * 1024 * 1024)
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)
    config = dict(
        min_files=0,
        batch_size=2,
        max_tasks=2,
        num_cpus=0.05,
        io_concurrency=2,
        metadata_budget_bytes=8192,
        task_metadata_budget_bytes=4096,
    )
    pg = placement_group([{"CPU": 0.1}])
    ray.get(pg.ready(), timeout=20)
    for key, value in config.items():
        monkeypatch.setattr(inference, "_FULL_SCHEMA_INFERENCE_" + key.upper(), value)
    paths = [tmp_path / f"f{i}.parquet" for i in range(6)]
    for i, path in enumerate(paths):
        pq.write_table(pa.table({"id": [i]}), path)
    trace = tmp_path / "reads.jsonl"
    try:
        monkeypatch.setenv("RAY_METADATA_ACCESS", "enabled")
        dataset = ray.data.read_parquet(
            list(map(str, paths)),
            merge_schema=True,
            filesystem=_filesystem(trace, require_env="RAY_METADATA_ACCESS"),
            runtime_env={"env_vars": {"RAY_METADATA_ACCESS": "enabled"}},
            num_cpus=0.1,
            ray_remote_args={
                "scheduling_strategy": PlacementGroupSchedulingStrategy(
                    pg, placement_group_bundle_index=0
                )
            },
        )
        before = _records(trace)
        assert len(before) == len(paths)
        assert all(r["pid"] == os.getpid() for r in before)
        pq.write_table(pa.table({"id": list(range(100, 104))}), paths[-1])
        monkeypatch.delenv("RAY_METADATA_ACCESS")
        assert dataset.count() == 9
        misses = _records(trace)[len(before) :]
        assert misses and {r["path"] for r in misses} == {str(paths[-1])}
        assert all(r["pid"] != os.getpid() for r in misses)
        assert {r["node"] for r in misses} == {
            placement_group_table(pg)["bundles_to_node_id"][0]
        }
    finally:
        remove_placement_group(pg)


@pytest.mark.parametrize("datasource_v2", [True], indirect=True)
def test_cached_metadata_survives_lineage_roundtrip(
    tmp_path, ray_start_regular_shared_2_cpus, datasource_v2
):
    paths = [tmp_path / f"f{i}.parquet" for i in range(3)]
    for i, path in enumerate(paths):
        pq.write_table(pa.table({"id": [i]}), path)
    trace = tmp_path / "opens.jsonl"
    dataset = ray.data.read_parquet(
        str(tmp_path), merge_schema=True, filesystem=_filesystem(trace)
    )
    serialized = dataset.serialize_lineage()
    before = _records(trace)
    assert Counter(r["path"] for r in before) == Counter(map(str, paths))

    pq.write_table(pa.table({"id": [99]}), tmp_path / "appended.parquet")
    restored = ray.data.Dataset.deserialize_lineage(serialized)
    assert restored.count() == 3
    assert _records(trace) == before
    assert sorted(row["id"] for row in restored.take_all()) == [0, 1, 2]


def _plan(tmp_path):
    paths = [tmp_path / f"{i}.parquet" for i in range(2)]
    raw = RecordingLocalFileSystem(skip_instance_cache=True)
    fs = pafs.PyFileSystem(pafs.FSSpecHandler(raw))
    tables = [
        pa.table(
            {
                "id": pa.array([], type=pa.int64()),
                "empty_only": pa.array([], type=pa.string()),
            }
        ),
        pa.table(
            {
                "id": [0, 1, 2],
                "late": [1, 2, 3],
            }
        ),
    ]
    for path, table in zip(paths, tables):
        pq.write_table(table, path, row_group_size=1)
    sample = sample_files(
        NonSamplingFileIndexer(ignore_missing_paths=False),
        list(map(str, paths)),
        fs,
        max_files=None,
        include_mtime=True,
    )
    source = ParquetDatasourceV2(list(map(str, paths)), filesystem=fs)
    result = source.infer_full_schema_with_metadata(
        sample, ray_remote_args={"num_cpus": 0}
    )
    return result, sample, fs, raw


@pytest.mark.parametrize("mtime_changed", [False, True])
def test_planning_row_counts_hit_and_miss(tmp_path, mtime_changed):
    result, sample, fs, raw = _plan(tmp_path)
    assert result.schema.names == ["id", "empty_only", "late"]
    metadata = result.metadata
    assert metadata is not None
    values = [FileMetadata.from_metadata(v) for v in metadata.to_pylist()]
    assert [v.num_rows for v in values] == [0, 3]
    assert len(raw.opens) == 2
    if mtime_changed:
        path = sample.paths[-1]
        mtime_ns = values[-1].mtime_ns
        assert mtime_ns is not None
        new_mtime_ns = mtime_ns + 10**9
        os.utime(path, ns=(new_mtime_ns, new_mtime_ns))
        assert fs.get_file_info(path).size == sample.file_sizes[-1]
        values[-1] = replace(values[-1], num_rows=999)
    manifest = FileManifest.construct_manifest(
        paths=sample.paths.tolist(),
        sizes=sample.file_sizes.tolist(),
        chunk_metadatas=[None] * 2,
        file_metadatas=values,
    )
    reader = ParquetFileReader(filesystem=fs)
    assert [v.num_rows for v in reader.read_metadata(manifest)] == [0, 3]
    assert len(raw.opens) == (3 if mtime_changed else 2)


@pytest.mark.parametrize(
    "has_size,stat_error,raises",
    [
        pytest.param(True, PermissionError, False, id="stat_denied"),
        pytest.param(True, NotImplementedError, False, id="stat_unsupported"),
        pytest.param(False, AssertionError, False, id="stat_unnecessary"),
        pytest.param(True, FileNotFoundError, True, id="missing"),
        pytest.param(True, OSError, True, id="io_error"),
    ],
)
def test_planning_row_count_stat_errors(tmp_path, has_size, stat_error, raises):
    class StatUnavailableFileSystem(pafs.LocalFileSystem):
        def get_file_info(self, paths):
            raise stat_error("optional stat unavailable")

    path = tmp_path / "data.parquet"
    pq.write_table(pa.table({"id": [1, 2, 3]}), path)
    manifest = FileManifest.construct_manifest(
        paths=[str(path)],
        sizes=[path.stat().st_size if has_size else None],
        chunk_metadatas=[None],
        file_metadatas=[FileMetadata(999)],
    )

    reader = ParquetFileReader(filesystem=StatUnavailableFileSystem())
    if raises:
        with pytest.raises(stat_error, match="optional stat unavailable"):
            list(reader.read_metadata(manifest))
    else:
        assert [metadata.num_rows for metadata in reader.read_metadata(manifest)] == [3]


@pytest.mark.parametrize("datasource_v2", [True], indirect=True)
@pytest.mark.parametrize("mtime_known", [True, False], ids=["mtime", "no_identity"])
def test_unknown_file_sizes_preserve_planning_metadata(
    tmp_path, ray_start_regular_shared_2_cpus, datasource_v2, mtime_known
):
    paths = [str(tmp_path / f"{i}.parquet") for i in range(2)]
    for i, path in enumerate(paths):
        pq.write_table(pa.table({"id": list(range(i + 1))}), path)
        timestamp = 1_600_000_000 + i
        os.utime(path, (timestamp, timestamp))
    trace = tmp_path / "reads.jsonl"
    raw = UnknownSizeRecordingLocalFileSystem(
        trace_path=str(trace), mtime_known=mtime_known, skip_instance_cache=True
    )
    fs = pafs.PyFileSystem(pafs.FSSpecHandler(raw))

    if mtime_known:
        indexer = NonSamplingFileIndexer(ignore_missing_paths=False)
        bounded = sample_files(indexer, paths, fs, max_files=1)
        sample = sample_files(indexer, paths, fs, max_files=None, include_mtime=True)
        assert bounded.paths.tolist() == paths[:1]
        assert bounded.known_file_sizes == [None]
        bounded_block = bounded.as_block()
        assert isinstance(bounded_block, pa.Table)
        assert FILE_MTIME_COLUMN_NAME not in bounded_block.column_names
        assert sample.paths.tolist() == paths
        assert sample.known_file_sizes == [None, None]
        block = sample.as_block()
        assert isinstance(block, pa.Table)
        assert block[FILE_SIZE_COLUMN_NAME].type == pa.int64()
        assert block[FILE_MTIME_COLUMN_NAME].to_pylist() == [
            1_600_000_000_000_000_000,
            1_600_000_001_000_000_000,
        ]

    dataset = ray.data.read_parquet(paths, filesystem=fs, merge_schema=True)
    assert dataset.schema(fetch_if_missing=False).base_schema.names == ["id"]
    before = _records(trace)
    assert Counter(r["path"] for r in before) == Counter(paths)
    assert dataset.count() == 3
    rereads = _records(trace)[len(before) :]
    assert Counter(r["path"] for r in rereads) == Counter([] if mtime_known else paths)


@pytest.mark.parametrize(
    "total,task,batch,records",
    [
        (0, 4096, 2, 0),
        (4096, 0, 2, 0),
        (31, 4096, 2, 0),
        (32, 4096, 2, 1),
        (96, 4096, 2, 2),
        (4096, 32, 256, 1),
        (4096, 64, 1, 2),
    ],
)
def test_payload_budgets_and_large_batches(
    monkeypatch, tmp_path, total, task, batch, records
):
    monkeypatch.setattr(
        inference, "_FULL_SCHEMA_INFERENCE_METADATA_BUDGET_BYTES", total
    )
    monkeypatch.setattr(
        inference, "_FULL_SCHEMA_INFERENCE_TASK_METADATA_BUDGET_BYTES", task
    )
    monkeypatch.setattr(inference, "_FULL_SCHEMA_INFERENCE_BATCH_SIZE", batch)
    result, sample, fs, raw = _plan(tmp_path)
    assert result.schema.names == ["id", "empty_only", "late"]
    metadata = result.metadata
    chunks = metadata.chunks if metadata is not None else []
    used = 0
    admitted = 0
    for chunk in chunks:
        values = [v for v in chunk.to_pylist() if v is not None]
        admitted += len(values)
        size = 32 * len(values)
        assert size <= task
        used += size
    assert admitted == records and used <= total


@pytest.mark.parametrize("change", ["size", "missing", "no_mtime", "empty"])
def test_cache_validation(tmp_path, change):
    path = tmp_path / "file"
    path.write_bytes(b"abc")
    fs = pafs.LocalFileSystem()
    info = fs.get_file_info(str(path))
    cached = FileMetadata(0, mtime_ns=info.mtime_ns)
    file = FileInfo(str(path), info.size, metadata=cached)
    if change == "size":
        path.write_bytes(b"longer")
    elif change == "missing":
        file = replace(file, path=str(tmp_path / "missing"))
    elif change == "no_mtime":
        file = replace(file, metadata=replace(cached, mtime_ns=None))
    elif change == "empty":
        file = replace(file, metadata=FileMetadata())
    if change == "missing":
        with pytest.raises(FileNotFoundError):
            get_cached_file_metadata(file, fs, [])
    else:
        actual = get_cached_file_metadata(file, fs, [])
        assert actual == (file.metadata if change == "no_mtime" else None)


@pytest.mark.parametrize(
    "distributed,expected",
    [
        pytest.param(False, [1, 2, None, None, 5, 6], id="driver"),
        pytest.param(True, [1, 2, 3, None, None, 6], id="worker"),
    ],
)
def test_deduplicated_schemas_keep_metadata_in_file_order(
    monkeypatch, tmp_path, ray_start_regular_shared_2_cpus, distributed, expected
):
    monkeypatch.setattr(inference, "_FULL_SCHEMA_INFERENCE_MIN_FILES", 0)
    monkeypatch.setattr(inference, "_FULL_SCHEMA_INFERENCE_BATCH_SIZE", 4)
    monkeypatch.setattr(inference, "_FULL_SCHEMA_INFERENCE_METADATA_BUDGET_BYTES", 160)
    monkeypatch.setattr(
        inference, "_FULL_SCHEMA_INFERENCE_TASK_METADATA_BUDGET_BYTES", 64
    )
    schemas = [[], [], ["late"], [], ["last"], []]
    paths = [tmp_path / f"{i}.parquet" for i in range(len(schemas))]
    for i, (path, extra) in enumerate(zip(paths, schemas)):
        table = pa.table({key: list(range(i + 1)) for key in ["id", *extra]})
        pq.write_table(table, path)
    sample = FileManifest.construct_manifest(
        paths=list(map(str, paths)),
        sizes=[path.stat().st_size for path in paths],
        chunk_metadatas=[None] * len(paths),
    )
    trace = tmp_path / "ordered-reads.jsonl"
    result = inference.infer_full_schema_with_metadata_from_files(
        sample,
        _filesystem(trace),
        format_name="Parquet",
        read_metadata=_read_parquet_metadata,
        ray_remote_args={},
        supports_distributed_reads=distributed,
    )
    assert result is not None
    metadata = result.metadata
    assert metadata is not None
    assert result.schema.names == ["id", "late", "last"]
    assert [value["num_rows"] if value else None for value in metadata.to_pylist()] == (
        expected
    )
    assert all(
        (record["pid"] != os.getpid()) == distributed for record in _records(trace)
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
