"""Full-schema reduction, scheduling and file access contracts."""

from collections import Counter
from copy import deepcopy
from typing import Any, Dict
from unittest.mock import Mock

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import pytest
from pyarrow import orc

import ray
from ray._raylet import PlacementGroupID
from ray.data._internal.datasource_v2.common import schema_inference as inference
from ray.data._internal.datasource_v2.common.listing_utils import (
    sample_files,
)
from ray.data._internal.datasource_v2.common.non_sampling_file_indexer import (
    NonSamplingFileIndexer,
)
from ray.data._internal.datasource_v2.interfaces.file_indexer import FileInfo
from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    FileManifest,
)
from ray.data._internal.object_extensions.arrow import ArrowPythonObjectType
from ray.data._internal.tensor_extensions.arrow import ArrowTensorTypeV2
from ray.data._internal.util import unify_schemas_with_validation
from ray.data.context import DataContext
from ray.data.tests.schema_planning_utils import RecordingLocalFileSystem
from ray.util.placement_group import PlacementGroup
from ray.util.scheduling_strategies import (
    In,
    NodeAffinitySchedulingStrategy,
    NodeLabelSchedulingStrategy,
    PlacementGroupSchedulingStrategy,
)

_SCHEMA_READERS = [("Parquet", pq.write_table), ("ORC", orc.write_table)]


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
        return partial, encoded

    monkeypatch.setattr(
        DataContext.get_current(), "full_schema_inference_batch_size", batch_size
    )
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
        actual = _infer(sample, pafs.LocalFileSystem())
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


@pytest.mark.parametrize("format_name,write", _SCHEMA_READERS)
def test_schema_reads_include_empty_files_once(tmp_path, format_name, write):
    paths = [tmp_path / "empty", tmp_path / "rows"]
    write(pa.table({"id": pa.array([], type=pa.int64())}), paths[0])
    write(pa.table({"late": [1, 2, 3]}), paths[1])
    filesystem, backing = _filesystem()
    sample = _sample(paths, filesystem)
    result = _infer(sample, filesystem, format_name=format_name)
    assert Counter(backing.opens) == {str(path): 1 for path in paths}
    assert result == pa.schema([("id", pa.int64()), ("late", pa.int64())])


@pytest.mark.parametrize(
    "scheduling_options",
    [{}, {"scheduling_strategy": "DEFAULT"}, {"scheduling_strategy": "SPREAD"}],
    ids=["default", "explicit_default", "spread"],
)
def test_schema_task_options_preserve_access_constraints(scheduling_options):
    remote = {
        "runtime_env": {"env_vars": {"X": "1"}},
        "label_selector": {"zone": "a"},
        "fallback_strategy": [{"label_selector": {"zone": "b"}}],
        "num_gpus": 1,
        "num_cpus": 4,
        "memory": 100,
        **scheduling_options,
    }
    options = inference._schema_task_options(remote, 0.5)
    assert (
        options
        == {
            key: remote[key]
            for key in ("runtime_env", "label_selector", "fallback_strategy")
        }
        | scheduling_options
    )
    assert inference._schema_task_options({"resources": {"storage": 1}}, 0.5) is None


@pytest.mark.parametrize(
    "remote_args,allowed",
    [
        pytest.param({}, True, id="default"),
        pytest.param({"num_cpus": None}, False, id="none"),
        pytest.param(
            {"num_cpus": 0, "label_selector": {"pool": "zero"}},
            False,
            id="zero_label",
        ),
        pytest.param({"num_cpus": 0.1}, False, id="fractional"),
        pytest.param(
            {
                "num_cpus": 0.49,
                "label_selector": {"pool": "zero"},
                "fallback_strategy": [{"label_selector": {"pool": "zero"}}],
            },
            False,
            id="restricted_fallback",
        ),
        pytest.param(
            {
                "num_cpus": 0,
                "scheduling_strategy": NodeLabelSchedulingStrategy(
                    {"pool": In("zero")}
                ),
            },
            False,
            id="zero_node_label",
        ),
        pytest.param(
            {"num_cpus": 0.5, "label_selector": {"pool": "worker"}},
            True,
            id="at_metadata_cpu",
        ),
        pytest.param(
            {
                "num_cpus": 1,
                "scheduling_strategy": NodeLabelSchedulingStrategy(
                    {"pool": In("worker")}
                ),
            },
            True,
            id="normal_node_label",
        ),
    ],
)
def test_schema_task_cpu_admission(remote_args, allowed):
    original = remote_args.copy()
    options = inference._schema_task_options(remote_args, 0.5)
    if allowed:
        assert options == {
            key: value for key, value in remote_args.items() if key != "num_cpus"
        }
    else:
        assert options is None
    assert remote_args == original


@pytest.mark.parametrize(
    "context_selector,read_selector,expected",
    [
        (None, {"node": "x"}, {"node": "x"}),
        ({"pool": "train"}, None, {"pool": "train"}),
        (
            {"pool": "train", "zone": "a"},
            {"pool": "eval"},
            {"pool": "eval", "zone": "a"},
        ),
    ],
    ids=["read_only", "context_only", "read_wins"],
)
def test_schema_tasks_merge_context_labels(
    monkeypatch, context_selector, read_selector, expected
):
    context = DataContext.get_current()
    monkeypatch.setattr(context.execution_options, "label_selector", context_selector)
    monkeypatch.setattr(context, "full_schema_inference_min_files", 0)
    schema = pa.schema([("id", pa.int64())])
    partial, encoded = inference._encode_schemas([schema])
    sample = FileManifest.construct_manifest(
        paths=["f"], sizes=[1], chunk_metadatas=[None]
    )
    remote = Mock()
    remote.options.return_value = remote
    remote.remote.return_value = (Mock(), encoded)
    monkeypatch.setattr(inference, "cached_remote_fn", lambda *a, **k: remote)
    monkeypatch.setattr(ray, "cluster_resources", lambda: {"CPU": 2})
    monkeypatch.setattr(ray, "wait", lambda refs, **k: (refs[:1], refs[1:]))
    monkeypatch.setattr(ray, "get", lambda ref: partial)
    read_args = {"label_selector": read_selector} if read_selector is not None else {}
    original_args, original_selector = deepcopy(read_args), deepcopy(context_selector)
    assert (
        _infer(sample, pafs.LocalFileSystem(), distributed=True, remote_args=read_args)
        == schema
    )
    remote.options.assert_called_once_with(label_selector=expected)
    assert read_args == original_args
    assert context.execution_options.label_selector == original_selector


@pytest.mark.parametrize(
    "suffix,value",
    [
        ("min_files", -1),
        ("max_tasks", 0),
        ("batch_size", 0),
        ("io_concurrency", 0),
        ("batch_size", 1.5),
        ("max_tasks", True),
        ("num_cpus", 0),
        ("num_cpus", -1),
        ("num_cpus", float("inf")),
        ("num_cpus", float("nan")),
        ("num_cpus", True),
        ("num_cpus", "0.5"),
    ],
)
def test_full_schema_rejects_invalid_config(monkeypatch, suffix, value):
    name = f"full_schema_inference_{suffix}"
    monkeypatch.setattr(DataContext.get_current(), name, value)
    sample = FileManifest.construct_manifest(paths=[], sizes=[], chunk_metadatas=[])
    with pytest.raises(ValueError, match=name):
        _infer(sample, pafs.LocalFileSystem())


@pytest.mark.parametrize(
    "read_args,io_concurrency,cpus,expected_window",
    [
        ({}, 4, 4, 0),
        ({"num_cpus": None}, 4, 4, 0),
        ({"num_cpus": 2}, 4, 4, 2),
        ({"num_cpus": 2}, 1, 4, 0),
        ({"num_cpus": 2}, 4, 8, 3),
    ],
)
def test_full_schema_config_controls_task_admission(
    monkeypatch, read_args, io_concurrency, cpus, expected_window
):
    context = DataContext.get_current()
    for suffix, value in dict(
        min_files=0,
        max_tasks=3,
        num_cpus=2,
        batch_size=8,
        io_concurrency=io_concurrency,
    ).items():
        monkeypatch.setattr(context, f"full_schema_inference_{suffix}", value)
    schema = pa.schema([("id", pa.int64())])
    sample = FileManifest.construct_manifest(
        paths=list(map(str, range(32))), sizes=[1] * 32, chunk_metadatas=[None] * 32
    )
    reads, windows = [], []

    def read_batch(files, *args, io_concurrency):
        reads.append(([file.path for file in files], io_concurrency))
        return inference._encode_schemas([schema] * len(files))

    class Remote:
        def options(self, **kwargs):
            return self

        def remote(self, *args, **kwargs):
            partial, encoded = read_batch(*args, **kwargs)
            return Mock(value=partial), encoded

    def remote_fn(fn, **kwargs):
        assert fn is read_batch
        assert kwargs == {"num_cpus": 2, "max_retries": 2, "num_returns": 2}
        return Remote()

    def wait(refs, **kwargs):
        windows.append(len(refs))
        return refs[:1], refs[1:]

    monkeypatch.setattr(inference, "_read_schema_batch", read_batch)
    monkeypatch.setattr(inference, "cached_remote_fn", remote_fn)
    monkeypatch.setattr(ray, "cluster_resources", lambda: {"CPU": cpus})
    monkeypatch.setattr(ray, "wait", wait)
    monkeypatch.setattr(ray, "get", lambda ref: ref.value)
    assert (
        _infer(sample, pafs.LocalFileSystem(), distributed=True, remote_args=read_args)
        == schema
    )
    assert reads == [
        (list(map(str, range(i, i + 8))), io_concurrency if expected_window else 16)
        for i in range(0, 32, 8)
    ]
    assert max(windows, default=0) == expected_window


@pytest.mark.parametrize("failure", [None, OSError, KeyboardInterrupt])
def test_ordered_task_window_and_failure_cleanup(monkeypatch, tmp_path, failure):
    path = tmp_path / "file.parquet"
    pq.write_table(pa.table({"id": [1]}), path)
    sample = FileManifest.construct_manifest(
        paths=[str(path)] * 7,
        sizes=[path.stat().st_size] * 7,
        chunk_metadatas=[None] * 7,
    )
    monkeypatch.setattr(
        DataContext.get_current(), "full_schema_inference_batch_size", 2
    )
    monkeypatch.setattr(DataContext.get_current(), "full_schema_inference_min_files", 0)
    monkeypatch.setattr(DataContext.get_current(), "full_schema_inference_max_tasks", 4)
    monkeypatch.setattr(ray, "cluster_resources", lambda: {"CPU": 8})
    state = {"pending": 0, "peak": 0, "submitted": 0, "original_gets": 0}
    cancelled = []
    options = {}

    class Remote:
        def options(self, **kwargs):
            options.update(kwargs)
            return self

        def remote(self, *args, **kwargs):
            result = inference._read_schema_batch(*args, **kwargs)
            index = state["submitted"]
            state["submitted"] += 1
            state["pending"] += 1
            state["peak"] = max(state["peak"], state["pending"])
            return (
                Mock(kind="summary", value=result[0], index=index),
                Mock(kind="original", value=result[1], index=index),
            )

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
    monkeypatch.setattr(ray, "wait", lambda refs, **kwargs: (refs[:1], refs[1:]))
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
        assert actual == pa.schema([("id", pa.int64())])
        assert state["submitted"] == 4
        assert state["original_gets"] == 0
        assert not cancelled
    assert state["peak"] == 4
    assert "num_gpus" not in options and "num_cpus" not in options


@pytest.mark.parametrize("replay", [False, True], ids=["flat", "replay"])
def test_completed_schema_batches_refill_before_slow_head(monkeypatch, replay):
    dtype = pa.list_(pa.int64()) if replay else pa.int64()
    schemas = [
        pa.schema(
            [pa.field(f"f{i}", dtype, metadata={b"position": str(i).encode()})],
            metadata={b"first": str(i).encode()},
        )
        for i in range(12)
    ]
    sample = FileManifest.construct_manifest(
        paths=list(map(str, range(12))), sizes=[1] * 12, chunk_metadatas=[None] * 12
    )
    context = DataContext.get_current()
    monkeypatch.setattr(context, "full_schema_inference_batch_size", 2)
    monkeypatch.setattr(context, "full_schema_inference_min_files", 0)
    monkeypatch.setattr(context, "full_schema_inference_max_tasks", 4)
    monkeypatch.setattr(ray, "cluster_resources", lambda: {"CPU": 2})
    state = {"submitted": 0, "peak": 0}
    active = {}
    finished = set()
    cancelled = []

    class Remote:
        def options(self, **kwargs):
            return self

        def remote(self, files, *args, **kwargs):
            index = state["submitted"]
            state["submitted"] += 1
            partial, encoded = inference._encode_schemas(
                [schemas[int(file.path)] for file in files]
            )
            ref = Mock(kind="summary", value=partial, index=index)
            active[index] = ref
            state["peak"] = max(state["peak"], len(active))
            assert len(active) <= 4
            return ref, Mock(kind="original", value=encoded, index=index)

    def wait(refs, *, num_returns):
        assert num_returns == 1
        assert set(refs) == set(
            active.values()
        ), "Wait must include all unfinished batches"
        candidates = [ref for ref in refs if ref.index != 0 or state["submitted"] > 4]
        assert (
            candidates
        ), "Completed later batches must permit refill before the slow head"
        ref = min(candidates, key=lambda item: item.index)
        active.pop(ref.index)
        finished.add(ref.index)
        return [ref], [item for item in refs if item is not ref]

    def get(ref):
        assert ref.index in finished, "Do not consume unready batch results"
        return ref.value

    monkeypatch.setattr(inference, "cached_remote_fn", lambda *a, **k: Remote())
    monkeypatch.setattr(ray, "wait", wait)
    monkeypatch.setattr(ray, "get", get)
    monkeypatch.setattr(
        ray, "cancel", lambda ref, **kwargs: cancelled.append(ref.index)
    )
    result = _infer(sample, pafs.LocalFileSystem(), distributed=True)
    assert result.equals(unify_schemas_with_validation(schemas), check_metadata=True)
    assert state["submitted"] == 6 and state["peak"] == 4
    assert not active and not cancelled


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
    monkeypatch.setattr(
        DataContext.get_current(), "full_schema_inference_batch_size", 1
    )
    monkeypatch.setattr(DataContext.get_current(), "full_schema_inference_min_files", 0)
    monkeypatch.setattr(ray, "cluster_resources", lambda: {"CPU": 2})
    monkeypatch.setattr(
        inference,
        "cached_remote_fn",
        lambda *args, **kwargs: pytest.fail(
            "Driver-only path must not create metadata tasks"
        ),
    )
    assert _infer(
        sample, pafs.LocalFileSystem(), distributed=distributed, remote_args=remote_args
    ) == pa.schema([("id", pa.int64())])


def test_schema_read_failure_contains_file_and_cause(tmp_path):
    missing = tmp_path / "missing.parquet"
    with pytest.raises(FileNotFoundError, match="missing.parquet") as error:
        inference._read_schema_batch(
            [FileInfo(str(missing), 100)],
            pafs.LocalFileSystem(),
            "Parquet",
            DataContext.get_current(),
            io_concurrency=4,
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
        return partial, encoded

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
    assert _infer(sample, pafs.LocalFileSystem(), distributed=True) == schema


@pytest.mark.parametrize("offset", [0, 1], ids=["at_threshold", "above_threshold"])
@pytest.mark.parametrize(
    "scheduling",
    [
        "default",
        "placement_group",
        "inherit",
        "hard_node_affinity",
        "soft_node_affinity",
        "low_cpu",
    ],
)
def test_default_schema_task_admission_boundary(monkeypatch, offset, scheduling):
    count = DataContext.get_current().full_schema_inference_min_files + offset
    paths = list(map(str, range(count)))
    sample = FileManifest.construct_manifest(
        paths=paths, sizes=[1] * count, chunk_metadatas=[None] * count
    )
    schema = pa.schema([("id", pa.int64())])
    read_paths = []
    concurrency = []
    submissions = []
    remote_args: Dict[str, Any] = {"num_cpus": 0.1 if scheduling == "low_cpu" else 1}
    if scheduling == "placement_group":
        pg = PlacementGroup(PlacementGroupID.from_random(), bundle_cache=[{"CPU": 1}])
        remote_args["scheduling_strategy"] = PlacementGroupSchedulingStrategy(
            pg, placement_group_bundle_index=0
        )
    elif scheduling == "inherit":
        remote_args["scheduling_strategy"] = None
    elif scheduling in ("hard_node_affinity", "soft_node_affinity"):
        remote_args["scheduling_strategy"] = NodeAffinitySchedulingStrategy(
            ray.NodeID.from_random().hex(), soft=scheduling == "soft_node_affinity"
        )
    distributed = bool(offset) and scheduling in ("default", "soft_node_affinity")

    def read_batch(files, *args, io_concurrency=4):
        read_paths.extend(file.path for file in files)
        concurrency.append(io_concurrency)
        partial, encoded = inference._encode_schemas([schema] * len(files))
        return partial, encoded

    class Remote:
        def options(self, **kwargs):
            return self

        def remote(self, *args, **kwargs):
            submissions.append(len(args[0]))
            summary, encoded = read_batch(*args, **kwargs)
            return Mock(value=summary), encoded

    def remote_fn(fn, **kwargs):
        assert fn is read_batch
        assert kwargs == {"num_cpus": 0.5, "max_retries": 2, "num_returns": 2}
        return Remote()

    monkeypatch.setattr(inference, "_read_schema_batch", read_batch)
    monkeypatch.setattr(inference, "cached_remote_fn", remote_fn)
    monkeypatch.setattr(ray, "cluster_resources", lambda: {"CPU": 2})
    monkeypatch.setattr(ray, "wait", lambda refs, **kwargs: (refs[:1], refs[1:]))
    monkeypatch.setattr(ray, "get", lambda ref: ref.value)

    assert (
        _infer(
            sample,
            pafs.LocalFileSystem(),
            distributed=True,
            remote_args=remote_args,
        )
        == schema
    )
    assert read_paths == paths
    batches = (
        count + DataContext.get_current().full_schema_inference_batch_size - 1
    ) // DataContext.get_current().full_schema_inference_batch_size
    assert concurrency == [4 if distributed else 16] * batches
    assert len(submissions) == (batches if distributed else 0)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
