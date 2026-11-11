"""Task admission, ordered schema reads and cancellation contracts."""

import json
import os
import subprocess
import sys
from copy import deepcopy
from typing import Any, Dict
from unittest.mock import Mock, PropertyMock

import pyarrow as pa
import pyarrow.fs as pafs
import pytest

import ray
from ray.data._internal.datasource_v2.common import schema_inference as inference
from ray.data._internal.datasource_v2.formats.orc import orc_datasource_v2
from ray.data._internal.datasource_v2.formats.parquet import parquet_datasource_v2
from ray.data._internal.datasource_v2.formats.parquet.parquet_datasource_v2 import (
    _read_parquet_schema,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.util import unify_schemas_with_validation
from ray.data.context import DataContext
from ray.util.scheduling_strategies import (
    NodeAffinitySchedulingStrategy,
    PlacementGroupSchedulingStrategy,
)


def test_schema_config_reads_environment(monkeypatch):
    settings = {
        "MIN_FILES": "7",
        "MAX_TASKS": "2",
        "NUM_CPUS": "1.25",
        "BATCH_SIZE": "3",
        "IO_CONCURRENCY": "5",
    }
    for suffix, value in settings.items():
        monkeypatch.setenv(f"RAY_DATA_FULL_SCHEMA_INFERENCE_{suffix}", value)
    command = (
        "import json; from ray.data._internal.datasource_v2.common "
        "import schema_inference as s; "
        f"print(json.dumps([getattr(s, '_FULL_SCHEMA_INFERENCE_' + k) for k in {list(settings)!r}]))"
    )
    result = subprocess.check_output([sys.executable, "-c", command], env=os.environ)
    assert json.loads(result) == [7, 2, 1.25, 3, 5]
    assert not hasattr(DataContext(), "full_schema_inference_min_files")


@pytest.mark.parametrize(
    "case_args,allowed",
    [
        pytest.param({}, True, id="default-True"),
        pytest.param({"num_cpus": None}, False, id="none-False"),
        pytest.param({"num_cpus": 0}, False, id="zero-False"),
        pytest.param({"num_cpus": 0.49}, False, id="low-False"),
        pytest.param({"num_cpus": 0.5}, True, id="equal-True"),
        pytest.param(
            {"scheduling_strategy": Mock(PlacementGroupSchedulingStrategy)},
            False,
            id="pg-False",
        ),
        pytest.param({"scheduling_strategy": None}, False, id="inherit-False"),
        pytest.param(
            {"scheduling_strategy": Mock(NodeAffinitySchedulingStrategy, soft=False)},
            False,
            id="hard-False",
        ),
        pytest.param(
            {"scheduling_strategy": Mock(NodeAffinitySchedulingStrategy, soft=True)},
            True,
            id="soft-True",
        ),
        pytest.param({"resources": {"storage": 1}}, False, id="resources-False"),
        pytest.param({"max_retries": -1}, False, id="unsupported-False"),
    ],
)
def test_schema_task_admission(case_args, allowed):
    args: Dict[str, Any] = {
        "runtime_env": {"env_vars": {"X": "1"}},
        "label_selector": {"pool": "read"},
        "fallback_strategy": [{"label_selector": {"pool": "backup"}}],
        **case_args,
    }
    original = args.copy()
    options = inference._schema_task_options(args, 0.5)
    assert args == original
    assert options == (
        {
            key: value
            for key, value in args.items()
            if key
            in {
                "runtime_env",
                "label_selector",
                "fallback_strategy",
                "scheduling_strategy",
            }
        }
        if allowed
        else None
    )


@pytest.mark.parametrize(
    "read_args,cpus,io_concurrency,threshold,window,schema_case",
    [
        ({}, 4, 4, 0, 0, "unique"),
        ({"num_cpus": None}, 4, 4, 0, 0, "unique"),
        ({"num_cpus": 2}, 4, 4, 0, 2, "unique"),
        ({"num_cpus": 2}, 4, 4, 0, 2, "same"),
        ({"num_cpus": 2, "label_selector": {"pool": "read"}}, 8, 4, 0, 3, "varied"),
        ({"num_cpus": 2}, 4, 1, 0, 0, "unique"),
        ({"num_cpus": 2}, 4, 4, 40, 0, "unique"),
    ],
)
def test_ordered_reads_and_task_options(
    monkeypatch, read_args, cpus, io_concurrency, threshold, window, schema_case
):
    context = DataContext.get_current()
    for suffix, value in dict(
        min_files=threshold,
        max_tasks=3,
        num_cpus=2,
        batch_size=8,
        io_concurrency=io_concurrency,
    ).items():
        monkeypatch.setattr(
            inference, f"_FULL_SCHEMA_INFERENCE_{suffix.upper()}", value
        )
    selector = {"pool": "context", "zone": "a"}
    monkeypatch.setattr(context.execution_options, "label_selector", selector)
    args: Dict[str, Any] = {
        "runtime_env": {"env_vars": {"X": "1"}},
        "scheduling_strategy": "SPREAD",
        **read_args,
    }
    original = deepcopy(args)
    paths = [f"field_{i}" for i in range(40)]
    nested = pa.struct(
        [
            ("value", pa.int64()),
            ("items", pa.list_(pa.int64())),
            ("mapping", pa.map_(pa.string(), pa.int64())),
        ]
    )
    schemas = {}
    for index, path in enumerate(paths):
        name, metadata, nullable = path, {b"file": path.encode()}, False
        if schema_case == "same":
            name, metadata = "shared", {b"file": b"same"}
        elif schema_case == "varied":
            # A,A,B,B,A,A,C,C: metadata-only and nullability-only misses,
            # plus a nonconsecutive repetition that must be decoded again.
            phase = index // 2 % 4
            name = f"field_{index // 8}"
            metadata = {b"file": b"changed" if phase == 1 else b"original"}
            nullable = phase == 3
        schemas[path] = pa.schema(
            [pa.field(name, nested, nullable=nullable, metadata=metadata)],
            metadata=metadata,
        )
    expected = unify_schemas_with_validation(list(schemas.values()))
    sample = FileManifest.construct_manifest(
        paths=paths, sizes=list(range(1, 41)), chunk_metadatas=[None] * 40
    )
    remote = Mock()
    remote.options.return_value = remote
    payloads, submitted, widths, timeouts = {}, [], [], []

    def submit(batch, batch_sizes, filesystem, reader, format_name, ctx, concurrency):
        assert batch_sizes == [int(path.removeprefix("field_")) + 1 for path in batch]
        assert concurrency == io_concurrency
        ref = Mock(index=len(submitted))
        unique = {}
        for index, path in enumerate(batch):
            unique.setdefault(schemas[path].serialize().to_pybytes(), index)
        payloads[ref] = [(index, value) for value, index in unique.items()]
        submitted.append(batch)
        return ref

    def wait(refs, **kwargs):
        widths.append(len(refs))
        timeouts.append(kwargs["timeout"])
        choices = [ref for ref in refs if ref.index or len(submitted) > window]
        assert choices, "Completed later batches must allow refill before the slow head"
        ref = max(choices, key=lambda ref: ref.index)
        return [ref], [other for other in refs if other is not ref]

    remote.remote.side_effect = submit
    factory = Mock(return_value=remote)
    merge = Mock(wraps=inference.unify_schemas_with_validation)
    decode = Mock(wraps=pa.ipc.read_schema)
    cancel = Mock()
    monkeypatch.setattr(inference, "cached_remote_fn", factory)
    monkeypatch.setattr(inference, "unify_schemas_with_validation", merge)
    monkeypatch.setattr(pa.ipc, "read_schema", decode)
    monkeypatch.setattr(ray, "cluster_resources", lambda: {"CPU": cpus})
    monkeypatch.setattr(ray, "wait", wait)
    monkeypatch.setattr(ray, "get", lambda ref: payloads[ref])
    monkeypatch.setattr(ray, "cancel", cancel)
    actual = inference.infer_full_schema_from_files(
        sample,
        pafs.LocalFileSystem(),
        format_name="Parquet",
        read_schema=_read_parquet_schema,
        ray_remote_args=args,
        supports_distributed_reads=True,
    )
    if window:
        assert actual is not None and expected is not None
        assert actual.equals(expected, check_metadata=True)
        assert submitted == [paths[i : i + 8] for i in range(0, 40, 8)]
        assert max(widths) == window
        assert timeouts[0] == inference._FULL_SCHEMA_INFERENCE_FIRST_RESULT_TIMEOUT_S
        assert all(timeout is None for timeout in timeouts[1:])
        factory.assert_called_once_with(inference._read_schema_batch, max_retries=2)
        remote.options.assert_called_once_with(
            num_cpus=2,
            runtime_env=args["runtime_env"],
            scheduling_strategy="SPREAD",
            label_selector={**selector, **args.get("label_selector", {})},
        )
        merge.assert_called_once()
        received = merge.call_args.args[0]
        expected_received = []
        for batch in submitted:
            seen = set()
            for path in batch:
                value = schemas[path].serialize().to_pybytes()
                if value not in seen:
                    expected_received.append(schemas[path])
                    seen.add(value)
        assert len(received) == len(expected_received)
        assert all(
            a.equals(b, check_metadata=True)
            for a, b in zip(received, expected_received)
        )
        assert decode.call_count == {"same": 5, "varied": 15, "unique": 40}[schema_case]
        if schema_case == "same":
            decode.reset_mock()
            again = inference.infer_full_schema_from_files(
                sample,
                pafs.LocalFileSystem(),
                format_name="Parquet",
                read_schema=_read_parquet_schema,
                ray_remote_args=args,
                supports_distributed_reads=True,
            )
            assert again is not None and again.equals(expected, check_metadata=True)
            assert decode.call_count == 5
            assert merge.call_count == 2
    else:
        assert actual is None
        factory.assert_not_called()
        merge.assert_not_called()
        decode.assert_not_called()
    cancel.assert_not_called()
    assert args == original and selector == {"pool": "context", "zone": "a"}


def test_first_schema_result_timeout_cancels_and_falls_back(monkeypatch):
    for suffix, value in dict(min_files=0, max_tasks=2, batch_size=1).items():
        monkeypatch.setattr(
            inference, f"_FULL_SCHEMA_INFERENCE_{suffix.upper()}", value
        )
    remote = Mock()
    remote.options.return_value = remote
    refs = [Mock(), Mock()]
    remote.remote.side_effect = refs
    monkeypatch.setattr(inference, "cached_remote_fn", lambda *a, **k: remote)
    monkeypatch.setattr(ray, "cluster_resources", lambda: {"CPU": 1})
    wait = Mock(return_value=([], refs))
    cancel = Mock()
    warning = Mock()
    monkeypatch.setattr(ray, "wait", wait)
    monkeypatch.setattr(ray, "cancel", cancel)
    monkeypatch.setattr(inference.logger, "warning", warning)
    sample = FileManifest.construct_manifest(
        paths=["a", "b"], sizes=[1, 1], chunk_metadatas=[None, None]
    )
    result = inference.infer_full_schema_from_files(
        sample,
        pafs.LocalFileSystem(),
        format_name="Parquet",
        read_schema=_read_parquet_schema,
        ray_remote_args={},
        supports_distributed_reads=True,
    )
    assert result is None
    wait.assert_called_once_with(
        refs,
        num_returns=1,
        timeout=inference._FULL_SCHEMA_INFERENCE_FIRST_RESULT_TIMEOUT_S,
    )
    assert [call.args[0] for call in cancel.call_args_list] == refs
    assert all(call.kwargs == {"force": False} for call in cancel.call_args_list)
    warning.assert_called_once()
    assert "falling back to driver schema reads" in warning.call_args.args[0]


@pytest.mark.parametrize(
    "stage,error",
    [("wait", KeyboardInterrupt), ("get", OSError), ("decode", pa.ArrowInvalid)],
)
def test_schema_failure_cancels_pending_tasks(monkeypatch, stage, error):
    for suffix, value in dict(min_files=0, max_tasks=2, batch_size=1).items():
        monkeypatch.setattr(
            inference, f"_FULL_SCHEMA_INFERENCE_{suffix.upper()}", value
        )
    remote = Mock()
    remote.options.return_value = remote
    refs = [Mock(), Mock()]
    remote.remote.side_effect = refs
    monkeypatch.setattr(inference, "cached_remote_fn", lambda *a, **k: remote)
    monkeypatch.setattr(ray, "cluster_resources", lambda: {"CPU": 1})
    monkeypatch.setattr(ray, "wait", lambda *a, **k: ([refs[0]], [refs[1]]))
    monkeypatch.setattr(ray, "get", lambda ref: [(0, b"invalid")])
    if stage == "decode":
        monkeypatch.setattr(
            pa.ipc, "read_schema", Mock(side_effect=error("schema failure"))
        )
    else:
        monkeypatch.setattr(ray, stage, Mock(side_effect=error("schema failure")))
    cancel = Mock()
    monkeypatch.setattr(ray, "cancel", cancel)
    sample = FileManifest.construct_manifest(
        paths=["a", "b"], sizes=[1, 1], chunk_metadatas=[None, None]
    )
    with pytest.raises(error, match="schema failure"):
        inference.infer_full_schema_from_files(
            sample,
            pafs.LocalFileSystem(),
            format_name="Parquet",
            read_schema=_read_parquet_schema,
            ray_remote_args={},
            supports_distributed_reads=True,
        )
    assert [call.args[0] for call in cancel.call_args_list] == (
        refs if stage == "wait" else refs[1:]
    )
    assert all(call.kwargs == {"force": False} for call in cancel.call_args_list)


def test_worker_returns_first_position_for_exact_payload():
    context = DataContext.get_current()
    values = [
        pa.schema([pa.field("a", pa.int64())], metadata={b"version": b"one"}),
        pa.schema([pa.field("a", pa.int64())], metadata={b"version": b"two"}),
        pa.schema([pa.field("a", pa.int64(), nullable=False)]),
    ]
    ordered = [values[0], values[1], values[0], values[2], values[1]]
    paths = [str(i) for i in range(len(ordered))]

    def read(path, filesystem, size):
        assert size == int(path) + 1
        return ordered[int(path)]

    result = inference._read_schema_batch(
        paths,
        [i + 1 for i in range(len(paths))],
        pafs.LocalFileSystem(),
        read,
        "Parquet",
        context,
        4,
    )
    assert [index for index, _ in result] == [0, 1, 3]
    assert [payload for _, payload in result] == [
        values[i].serialize().to_pybytes() for i in range(3)
    ]


@pytest.mark.parametrize("size", [None, 123])
def test_parquet_schema_reader_uses_listed_size_and_retries(monkeypatch, size):
    import pyarrow.dataset as pds

    expected = pa.schema([("a", pa.int64())])
    metadata = Mock()
    metadata.schema.to_arrow_schema.return_value = expected
    fragment = Mock()
    metadata_property = PropertyMock(
        side_effect=[OSError("retryable footer error"), metadata]
    )
    type(fragment).metadata = metadata_property
    file_format = Mock()
    file_format.make_fragment.return_value = fragment
    monkeypatch.setattr(pds, "ParquetFileFormat", lambda: file_format)
    monkeypatch.setattr(DataContext.get_current(), "retried_io_errors", ["retryable"])
    monkeypatch.setattr("ray._common.retry.time.sleep", lambda _: None)
    filesystem = pafs.LocalFileSystem()

    assert (
        parquet_datasource_v2._read_parquet_schema("test.parquet", filesystem, size)
        == expected
    )
    file_format.make_fragment.assert_called_once_with(
        "test.parquet", filesystem=filesystem, file_size=size
    )
    assert metadata_property.call_count == 2


def test_orc_schema_reader_retries_schema_read(monkeypatch):
    import pyarrow.orc as orc

    expected = pa.schema([("a", pa.int64())])
    reader = Mock(
        side_effect=[OSError("retryable footer error"), Mock(schema=expected)]
    )
    monkeypatch.setattr(orc, "ORCFile", reader)
    monkeypatch.setattr(DataContext.get_current(), "retried_io_errors", ["retryable"])
    monkeypatch.setattr("ray._common.retry.time.sleep", lambda _: None)

    assert orc_datasource_v2._read_orc_schema("test.orc", None, None) == expected
    assert reader.call_count == 2


def test_worker_read_failure_preserves_file_and_cause(tmp_path):
    with pytest.raises(FileNotFoundError, match="missing.parquet") as error:
        inference._read_schema_batch(
            [str(tmp_path / "missing.parquet")],
            [None],
            pafs.LocalFileSystem(),
            _read_parquet_schema,
            "Parquet",
            DataContext.get_current(),
            4,
        )
    assert isinstance(error.value.__cause__, FileNotFoundError)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
