"""Full-schema reduction and planning metadata handoff contracts."""

import os
from collections import Counter
from dataclasses import replace
from datetime import datetime

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import pytest
from pyarrow import orc

import ray
from ray.data._internal.datasource_v2.common import schema_inference as inference
from ray.data._internal.datasource_v2.common.file_metadata import (
    get_cached_file_metadata,
)
from ray.data._internal.datasource_v2.common.file_reader import FileFormat
from ray.data._internal.datasource_v2.common.listing_utils import (
    list_files_for_each_block,
    sample_files,
)
from ray.data._internal.datasource_v2.common.non_sampling_file_indexer import (
    NonSamplingFileIndexer,
)
from ray.data._internal.datasource_v2.formats.orc.orc_file_reader import OrcFileReader
from ray.data._internal.datasource_v2.formats.parquet.footer_reader import FooterReader
from ray.data._internal.datasource_v2.formats.parquet.parquet_file_reader import (
    ParquetFileReader,
)
from ray.data._internal.datasource_v2.interfaces.file_indexer import FileInfo
from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    FILE_METADATA_COLUMN_NAME,
    FILE_METADATA_TYPE,
    FileManifest,
    FileMetadata,
)
from ray.data._internal.datasource_v2.interfaces.supports_metadata import MetadataType
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.object_extensions.arrow import ArrowPythonObjectType
from ray.data._internal.tensor_extensions.arrow import ArrowTensorTypeV2
from ray.data._internal.util import unify_schemas_with_validation
from ray.data.context import DataContext
from ray.data.datasource.file_based_datasource import FileShuffleConfig
from ray.data.expressions import col
from ray.data.tests.schema_planning_utils import RecordingLocalFileSystem


def _filesystem(**kwargs):
    backing = RecordingLocalFileSystem(skip_instance_cache=True, **kwargs)
    return pafs.PyFileSystem(pafs.FSSpecHandler(backing)), backing


def _sample(paths, filesystem):
    indexer = NonSamplingFileIndexer(ignore_missing_paths=False)
    return sample_files(indexer, list(map(str, paths)), filesystem, max_files=None)


def _infer(sample, filesystem, **kwargs):
    return inference.infer_full_schema_from_files(
        sample,
        filesystem,
        format_name=kwargs.pop("format_name", "Parquet"),
        supports_distributed_reads=kwargs.pop("distributed", False),
        ray_remote_args=kwargs.pop("remote_args", {}),
        **kwargs,
    )


def _schema_sequences():
    first = pa.schema(
        [pa.field("a", pa.int64(), nullable=False, metadata={b"field": b"first"})],
        metadata={b"schema": b"first"},
    )
    appended = pa.schema(
        [("b", pa.string()), ("a", pa.int64())], metadata={b"schema": b"later"}
    )
    tensor = ArrowTensorTypeV2((2,), pa.float32())
    return [
        [first, appended, first, appended],
        [first, pa.schema([("a", pa.int32())]), first],
        [pa.schema([("a", pa.null())]), first],
        [
            pa.schema([("a", pa.struct([("x", pa.int32())]))]),
            pa.schema([("a", pa.struct([("y", pa.string())]))]),
        ],
        [
            pa.schema([("a", pa.list_(pa.null()))]),
            pa.schema([("a", pa.list_(pa.int64()))]),
        ],
        [pa.schema([("a", pa.map_(pa.string(), pa.int64()))])] * 3,
        [pa.schema([("a", pa.dictionary(pa.int32(), pa.string()))])] * 3,
        [
            pa.schema([("a", pa.int64())]),
            pa.schema([("a", pa.string())]),
            pa.schema([("a", ArrowPythonObjectType())]),
        ],
        [pa.schema([("a", ArrowPythonObjectType())]), pa.schema([("a", tensor)])],
        [pa.schema([("a", tensor)]), pa.schema([("a", pa.null())])],
        [pa.schema([("a", pa.int64()), ("a", pa.int64())])] * 2,
        [pa.schema([], metadata={b"empty": b"first"}), pa.schema([])],
    ]


@pytest.mark.parametrize("schemas", _schema_sequences())
@pytest.mark.parametrize("batch_size", [1, 2, 3])
def test_full_merge_replays_one_shot_semantics(monkeypatch, schemas, batch_size):
    def read_batch(files, *args, **kwargs):
        partial, encoded = inference._encode_schemas(
            [schemas[int(file.path)] for file in files]
        )
        return inference._SchemaBatchSummary(partial, None, 0), encoded

    monkeypatch.setattr(inference, "_SCHEMA_READ_BATCH_SIZE", batch_size)
    monkeypatch.setattr(inference, "_read_schema_batch", read_batch)
    sample = FileManifest.construct_manifest(
        paths=list(map(str, range(len(schemas)))),
        sizes=[1] * len(schemas),
        chunk_metadatas=[None] * len(schemas),
    )
    try:
        expected = unify_schemas_with_validation(schemas)
    except (ValueError, pa.ArrowTypeError, pa.ArrowInvalid) as error:
        with pytest.raises(type(error)):
            _infer(sample, pafs.LocalFileSystem())
    else:
        actual = _infer(sample, pafs.LocalFileSystem()).schema
        assert actual.equals(expected, check_metadata=True)


def test_partial_failure_must_not_reject_global_object_schema():
    schemas = [
        pa.schema([("x", dtype)])
        for dtype in (pa.int64(), pa.string(), ArrowPythonObjectType())
    ]
    with pytest.raises((pa.ArrowTypeError, pa.ArrowInvalid)):
        unify_schemas_with_validation(schemas[:2])
    partial, encoded = inference._encode_schemas(schemas)
    assert partial is None
    merged = unify_schemas_with_validation(encoded.decode())
    assert isinstance(merged, pa.Schema)
    assert merged.field("x").type == ArrowPythonObjectType()


def test_lossless_dictionary_retains_metadata_and_occurrences():
    first = pa.schema([("x", pa.int64())], metadata={b"key": b"one"})
    second = first.with_metadata({b"key": b"two"})
    _, encoded = inference._encode_schemas([first, second, first, second])
    assert len(encoded.schemas) == 2
    assert list(encoded.indices) == [0, 1, 0, 1]
    for actual, expected in zip(encoded.decode(), [first, second, first, second]):
        assert actual.equals(expected, check_metadata=True)


class _CustomType(pa.ExtensionType):
    def __init__(self):
        super().__init__(pa.binary(), "ray.tests.full_schema_custom")

    def __arrow_ext_serialize__(self):
        return b"v1"

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        return cls()


def test_driver_restores_extension_unknown_to_worker():
    dtype = _CustomType()
    original = pa.schema([pa.field("x", dtype, metadata={b"note": b"kept"})])
    # The worker does not have the driver's extension registration.
    unknown = pa.ipc.read_schema(original.serialize())
    assert unknown.field("x").type == pa.binary()
    partial, encoded = inference._encode_schemas([unknown, unknown])
    assert partial is None
    pa.register_extension_type(dtype)
    try:
        for actual in encoded.decode():
            assert actual.equals(original, check_metadata=True)
    finally:
        pa.unregister_extension_type(dtype.extension_name)


@pytest.mark.parametrize(
    "format_name,write", [("Parquet", pq.write_table), ("ORC", orc.write_table)]
)
def test_schema_and_metadata_share_one_open(tmp_path, format_name, write):
    paths = [tmp_path / "empty", tmp_path / "rows"]
    write(pa.table({"id": pa.array([], type=pa.int64())}), paths[0])
    write(pa.table({"id": [1, 2, 3]}), paths[1])
    filesystem, backing = _filesystem()
    sample = _sample(paths, filesystem)
    result = _infer(sample, filesystem, format_name=format_name)
    assert Counter(backing.opens) == {str(path): 1 for path in paths}
    assert result.metadata is not None
    values = result.metadata.to_pylist()
    assert [value["num_rows"] for value in values] == [0, 3]
    if format_name == "Parquet":
        for path, value in zip(paths, values):
            assert pq.read_metadata(pa.BufferReader(value["parquet_footer"])).equals(
                pq.read_metadata(path)
            )
    else:
        assert all(value["parquet_footer"] is None for value in values)


@pytest.mark.parametrize("budget", [0, 31, 32, 96, 4096])
def test_metadata_budget_is_optional_and_bounded(monkeypatch, tmp_path, budget):
    paths = [tmp_path / f"f{i}.parquet" for i in range(3)]
    for path in paths:
        pq.write_table(pa.table({"id": [1]}), path)
    filesystem, _ = _filesystem()
    monkeypatch.setattr(inference, "_SCHEMA_METADATA_BUDGET", budget)
    monkeypatch.setattr(inference, "_SCHEMA_READ_BATCH_SIZE", 2)
    result = _infer(_sample(paths, filesystem), filesystem)
    assert result.schema == pa.schema([("id", pa.int64())])
    if budget < inference._FILE_METADATA_RECORD_BYTES:
        assert result.metadata is None
    else:
        assert result.metadata is not None
        values = [value for value in result.metadata.to_pylist() if value is not None]
        admitted = [value for value in values if value["num_rows"] is not None]
        assert len(admitted) == min(3, budget // inference._FILE_METADATA_RECORD_BYTES)
        retained = sum(len(value["parquet_footer"] or b"") for value in admitted)
        assert (
            retained + len(admitted) * inference._FILE_METADATA_RECORD_BYTES <= budget
        )


@pytest.mark.parametrize("predicate", [None, col("id") >= 30, col("missing") > 0])
@pytest.mark.parametrize("excluded", [False, True])
def test_cached_parquet_footer_keeps_chunking_semantics(tmp_path, predicate, excluded):
    path = tmp_path / "data.parquet"
    pq.write_table(
        pa.table({"id": range(100), "wide": ["x" * 20] * 100}), path, row_group_size=25
    )
    filesystem, backing = _filesystem()
    sample = _sample([path], filesystem)
    result = _infer(sample, filesystem)
    file = FileInfo(str(path), path.stat().st_size)
    kwargs = dict(
        filesystem=filesystem,
        io_concurrency=1,
        filter_expr=predicate,
        projected_cols=["id"],
        coalesce_bytes=500,
        excluded_read_unit_ids={f"{path}#rg0"} if excluded else None,
    )
    reader = FooterReader(**kwargs)
    try:
        expected = reader._read_and_chunk(file)
        backing.fail_open = True
        assert result.metadata is not None
        cached = FileMetadata.from_metadata(result.metadata[0].as_py())
        actual = reader._read_and_chunk(replace(file, metadata=cached))
        assert actual == expected
        assert actual.file.metadata is None
    finally:
        reader.pool.shutdown()


@pytest.mark.parametrize("case", ["nan", "null", "no_statistics", "int96"])
def test_cached_parquet_footer_preserves_statistics_and_int96(tmp_path, case):
    write_options = {}
    if case == "nan":
        table = pa.table({"id": [35.0, 40.0, float("nan")]})
    elif case == "null":
        table = pa.table({"id": pa.array([35, 40, None], type=pa.int64())})
    else:
        table = pa.table({"id": [35, 40, 45]})
        if case == "no_statistics":
            write_options["write_statistics"] = False
        else:
            table = table.append_column(
                "timestamp", pa.array([datetime(2020, 1, 1)] * 3)
            )
            write_options["use_deprecated_int96_timestamps"] = True

    path = tmp_path / "data.parquet"
    pq.write_table(table, path, row_group_size=3, **write_options)
    original_metadata = pq.read_metadata(path)
    if case == "no_statistics":
        assert original_metadata.row_group(0).column(0).statistics is None
    elif case == "int96":
        assert original_metadata.schema.column(1).physical_type == "INT96"

    filesystem, backing = _filesystem()
    result = _infer(_sample([path], filesystem), filesystem)
    assert result.metadata is not None
    cached = FileMetadata.from_metadata(result.metadata[0].as_py())
    assert cached.parquet_footer is not None
    assert pq.read_metadata(pa.BufferReader(cached.parquet_footer)).equals(
        original_metadata
    )
    file = FileInfo(str(path), path.stat().st_size)
    reader = FooterReader(
        filesystem=filesystem,
        io_concurrency=1,
        filter_expr=col("id") >= 30,
        projected_cols=table.column_names,
    )
    try:
        expected = reader._read_and_chunk(file)
        backing.fail_open = True
        actual = reader._read_and_chunk(replace(file, metadata=cached))
        assert actual == expected
        assert actual.file.metadata is None
        assert [group.unit_ids for group in actual.row_groups] == [(0,)]
        assert sum(group.num_rows for group in actual.row_groups) == 3
        assert all(
            group.fully_matched == (case == "int96") for group in actual.row_groups
        )
    finally:
        reader.pool.shutdown()


@pytest.mark.parametrize(
    "format_name,write,reader_class",
    [
        ("Parquet", pq.write_table, ParquetFileReader),
        ("ORC", orc.write_table, OrcFileReader),
    ],
)
def test_count_consumes_zero_and_positive_cached_rows(
    tmp_path, format_name, write, reader_class
):
    paths = [tmp_path / "empty", tmp_path / "rows"]
    write(pa.table({"id": pa.array([], type=pa.int64())}), paths[0])
    write(pa.table({"id": [1, 2, 3]}), paths[1])
    filesystem, backing = _filesystem()
    sample = _sample(paths, filesystem)
    result = _infer(sample, filesystem, format_name=format_name)
    block = sample.as_block()
    assert isinstance(block, pa.Table)
    assert result.metadata is not None
    manifest = FileManifest(
        block.append_column(FILE_METADATA_COLUMN_NAME, result.metadata)
    )
    kwargs = (
        {"format": FileFormat.ORC, "reuse_file_metadata": True}
        if format_name == "ORC"
        else {}
    )
    reader = reader_class(filesystem=filesystem, **kwargs)
    backing.fail_open = True
    assert [metadata.num_rows for metadata in reader.read_metadata(manifest)] == [0, 3]
    assert MetadataType.NUM_ROWS in reader.available_metadata()


def test_orc_default_does_not_add_metadata_count():
    assert OrcFileReader(format=FileFormat.ORC).available_metadata() == set()
    assert (
        OrcFileReader(
            format=FileFormat.ORC, reuse_file_metadata=True, predicate=col("id") > 0
        ).available_metadata()
        == set()
    )


def test_parquet_metadata_count_without_known_file_size(tmp_path):
    path = tmp_path / "data.parquet"
    pq.write_table(pa.table({"id": [1, 2]}), path)
    manifest = FileManifest(
        pa.table(
            {
                "__path": [str(path)],
                "__file_size": pa.array([None], type=pa.int64()),
                "__file_chunk_metadata": [None],
            }
        )
    )
    reader = ParquetFileReader(filesystem=pafs.LocalFileSystem())
    assert [value.num_rows for value in reader.read_metadata(manifest)] == [2]


def test_metadata_cache_checks_existence_size_and_mtime(tmp_path):
    path = tmp_path / "file"
    path.write_bytes(b"a")
    filesystem = pafs.LocalFileSystem()
    info = filesystem.get_file_info(str(path))
    cached = FileMetadata(num_rows=0, mtime_ns=info.mtime_ns)
    file = FileInfo(str(path), info.size, metadata=cached)
    assert get_cached_file_metadata(file, filesystem, []) == cached
    path.write_bytes(b"longer")
    assert get_cached_file_metadata(file, filesystem, []) is None
    path.write_bytes(b"a")
    os.utime(path, ns=(info.mtime_ns + 10**9, info.mtime_ns + 10**9))
    assert get_cached_file_metadata(file, filesystem, []) is None
    assert (
        get_cached_file_metadata(replace(file, path=str(tmp_path)), filesystem, [])
        is None
    )
    with pytest.raises(FileNotFoundError):
        get_cached_file_metadata(
            replace(file, path=str(tmp_path / "missing")), filesystem, []
        )


def test_metadata_stream_survives_whole_file_rebuild_and_shuffle(tmp_path):
    indexer = NonSamplingFileIndexer(ignore_missing_paths=False, max_paths_per_output=1)
    manifest = FileManifest.construct_manifest(
        paths=["a", "b", "a"],
        sizes=[10, 20, 10],
        chunk_metadatas=[None] * 3,
        file_metadatas=[
            FileMetadata(1, b"footer", 10),
            FileMetadata(),
            FileMetadata(2),
        ],
    )
    blocks = list(
        list_files_for_each_block(
            [manifest.as_block()],
            TaskContext(task_idx=0, op_name="test"),
            indexer=indexer.as_whole_file_indexer(),
            filesystem=pafs.LocalFileSystem(),
            prelisted_file_infos=True,
            shuffle_config=FileShuffleConfig(seed=17),
        )
    )
    assert all(
        block.schema.field(FILE_METADATA_COLUMN_NAME).type == FILE_METADATA_TYPE
        for block in blocks
    )
    recovered = FileManifest.concat(list(map(FileManifest, blocks)))
    metadatas = recovered.file_metadatas
    assert metadatas is not None
    values = sorted(
        (str(path), value.num_rows or 0)
        for path, value in zip(recovered.paths, metadatas)
    )
    assert values == [("a", 1), ("a", 2), ("b", 0)]
    assert all(value.parquet_footer is None for value in metadatas)


def test_schema_task_options_preserve_access_constraints():
    remote = {
        "runtime_env": {"env_vars": {"X": "1"}},
        "label_selector": {"zone": "a"},
        "fallback_strategy": [{"label_selector": {"zone": "b"}}],
        "num_gpus": 1,
        "num_cpus": 4,
        "memory": 100,
    }
    options = inference._schema_task_options(remote)
    assert options == {
        key: remote[key]
        for key in ("runtime_env", "label_selector", "fallback_strategy")
    }
    assert inference._schema_task_options({"resources": {"storage": 1}}) is None


@pytest.mark.parametrize("failure", [None, OSError, KeyboardInterrupt])
def test_ordered_task_window_and_failure_cleanup(monkeypatch, tmp_path, failure):
    path = tmp_path / "file.parquet"
    pq.write_table(pa.table({"id": [1]}), path)
    sample = FileManifest.construct_manifest(
        paths=[str(path)] * 7,
        sizes=[path.stat().st_size] * 7,
        chunk_metadatas=[None] * 7,
    )
    monkeypatch.setattr(inference, "_SCHEMA_READ_BATCH_SIZE", 2)
    monkeypatch.setattr(inference, "_SCHEMA_READ_MIN_FILES", 0)
    monkeypatch.setattr(inference, "_SCHEMA_READ_MAX_TASKS", 4)
    monkeypatch.setattr(ray, "cluster_resources", lambda: {"CPU": 8})
    state = {"pending": 0, "peak": 0, "submitted": 0, "original_gets": 0}
    cancelled = []
    options = {}

    class Ref:
        def __init__(self, kind, value, index):
            self.kind, self.value, self.index = kind, value, index

    class Remote:
        def options(self, **kwargs):
            options.update(kwargs)
            return self

        def remote(self, *args):
            result = inference._read_schema_batch(*args)
            index = state["submitted"]
            state["submitted"] += 1
            state["pending"] += 1
            state["peak"] = max(state["peak"], state["pending"])
            return Ref("summary", result[0], index), Ref("original", result[1], index)

    def remote_fn(fn, **kwargs):
        assert fn is inference._read_schema_batch
        assert kwargs == {"num_cpus": 0.5, "max_retries": 2, "num_returns": 2}
        return Remote()

    def get(ref):
        if ref.kind == "summary":
            if failure is not None and ref.index == 0:
                raise failure("test schema task failure")
            state["pending"] -= 1
        else:
            state["original_gets"] += 1
        return ref.value

    monkeypatch.setattr(inference, "cached_remote_fn", remote_fn)
    monkeypatch.setattr(ray, "get", get)
    monkeypatch.setattr(ray, "wait", lambda refs, **kwargs: (refs, []))
    monkeypatch.setattr(
        ray, "cancel", lambda ref, **kwargs: cancelled.append(ref.index)
    )
    if failure is not None:
        with pytest.raises(failure, match="test schema task failure"):
            _infer(sample, pafs.LocalFileSystem(), distributed=True)
        assert cancelled == [0, 1, 2, 3]
        assert state["submitted"] == 4
    else:
        actual = _infer(
            sample,
            pafs.LocalFileSystem(),
            distributed=True,
            remote_args={"num_cpus": 8, "num_gpus": 1},
        )
        assert actual.schema == pa.schema([("id", pa.int64())])
        assert state["submitted"] == 4
        assert state["original_gets"] == 0
        assert not cancelled
    assert state["peak"] == 4
    assert "num_gpus" not in options and "num_cpus" not in options


@pytest.mark.parametrize(
    "distributed,remote_args",
    [
        (False, {}),
        (True, {"resources": {"storage": 1}}),
    ],
)
def test_local_or_unsupported_options_do_not_submit_tasks(
    monkeypatch, tmp_path, distributed, remote_args
):
    path = tmp_path / "file.parquet"
    pq.write_table(pa.table({"id": [1]}), path)
    sample = FileManifest.construct_manifest(
        paths=[str(path)] * 3,
        sizes=[path.stat().st_size] * 3,
        chunk_metadatas=[None] * 3,
    )
    monkeypatch.setattr(inference, "_SCHEMA_READ_BATCH_SIZE", 1)
    monkeypatch.setattr(
        inference,
        "cached_remote_fn",
        lambda *args, **kwargs: pytest.fail(
            "Driver-only path must not create metadata tasks"
        ),
    )
    assert _infer(
        sample, pafs.LocalFileSystem(), distributed=distributed, remote_args=remote_args
    ).schema == pa.schema([("id", pa.int64())])


@pytest.mark.parametrize(
    "format_name,write,reader_class",
    [
        ("Parquet", pq.write_table, ParquetFileReader),
        ("ORC", orc.write_table, OrcFileReader),
    ],
)
def test_count_cache_miss_and_replacement_read_current_footer(
    tmp_path, format_name, write, reader_class
):
    path = tmp_path / "rows"
    write(pa.table({"id": [1]}), path)
    filesystem, backing = _filesystem()
    sample = _sample([path], filesystem)
    result = _infer(sample, filesystem, format_name=format_name)
    block = sample.as_block()
    assert isinstance(block, pa.Table)
    assert result.metadata is not None
    cached = FileManifest(
        block.append_column(FILE_METADATA_COLUMN_NAME, result.metadata)
    )
    kwargs = (
        {"format": FileFormat.ORC, "reuse_file_metadata": True}
        if format_name == "ORC"
        else {}
    )
    reader = reader_class(filesystem=filesystem, **kwargs)
    before = len(backing.opens)
    assert [value.num_rows for value in reader.read_metadata(sample)] == [1]
    assert len(backing.opens) == before + 1
    write(pa.table({"id": list(range(200))}), path)
    assert [value.num_rows for value in reader.read_metadata(cached)] == [200]
    assert len(backing.opens) == before + 2


def test_schema_read_failure_contains_file_and_cause(tmp_path):
    missing = tmp_path / "missing.parquet"
    with pytest.raises(FileNotFoundError, match="missing.parquet") as error:
        inference._read_schema_batch(
            [FileInfo(str(missing), 100)],
            pafs.LocalFileSystem(),
            "Parquet",
            1,
            1000,
            DataContext.get_current(),
        )
    assert isinstance(error.value.__cause__, FileNotFoundError)


@pytest.mark.parametrize("count,cpus", [(1024, None), (2048, None), (2049, 1)])
def test_small_or_cpu_limited_plans_keep_driver_io(monkeypatch, count, cpus):
    schema = pa.schema([("id", pa.int64())])
    sample = FileManifest.construct_manifest(
        paths=[str(i) for i in range(count)],
        sizes=[1] * count,
        chunk_metadatas=[None] * count,
    )

    def read_batch(files, *args, **kwargs):
        partial, encoded = inference._encode_schemas([schema] * len(files))
        return inference._SchemaBatchSummary(partial, None, 0), encoded

    def resources():
        assert cpus is not None, "Small plans should not query cluster resources"
        return {"CPU": cpus}

    monkeypatch.setattr(inference, "_read_schema_batch", read_batch)
    monkeypatch.setattr(ray, "cluster_resources", resources)
    monkeypatch.setattr(
        inference,
        "cached_remote_fn",
        lambda *a, **k: pytest.fail("Unexpected metadata task"),
    )
    assert _infer(sample, pafs.LocalFileSystem(), distributed=True).schema == schema


@pytest.mark.parametrize("offset", [0, 1], ids=["at_threshold", "above_threshold"])
def test_default_schema_task_admission_boundary(monkeypatch, offset):
    count = inference._SCHEMA_READ_MIN_FILES + offset
    paths = list(map(str, range(count)))
    sample = FileManifest.construct_manifest(
        paths=paths, sizes=[1] * count, chunk_metadatas=[None] * count
    )
    schema = pa.schema([("id", pa.int64())])
    read_paths = []
    concurrency = []
    submissions = []

    def read_batch(files, *args, io_concurrency=4):
        read_paths.extend(file.path for file in files)
        concurrency.append(io_concurrency)
        partial, encoded = inference._encode_schemas([schema] * len(files))
        return inference._SchemaBatchSummary(partial, None, 0), encoded

    class Ref:
        def __init__(self, value):
            self.value = value

    class Remote:
        def options(self, **kwargs):
            return self

        def remote(self, *args):
            submissions.append(len(args[0]))
            summary, encoded = read_batch(*args)
            return Ref(summary), encoded

    def remote_fn(fn, **kwargs):
        assert fn is read_batch
        assert kwargs == {"num_cpus": 0.5, "max_retries": 2, "num_returns": 2}
        return Remote()

    monkeypatch.setattr(inference, "_read_schema_batch", read_batch)
    monkeypatch.setattr(inference, "cached_remote_fn", remote_fn)
    monkeypatch.setattr(ray, "cluster_resources", lambda: {"CPU": 2})
    monkeypatch.setattr(ray, "wait", lambda refs, **kwargs: (refs, []))
    monkeypatch.setattr(ray, "get", lambda ref: ref.value)

    assert _infer(sample, pafs.LocalFileSystem(), distributed=True).schema == schema
    assert read_paths == paths
    batches = (
        count + inference._SCHEMA_READ_BATCH_SIZE - 1
    ) // inference._SCHEMA_READ_BATCH_SIZE
    assert concurrency == [4 if offset else 16] * batches
    assert len(submissions) == (batches if offset else 0)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
