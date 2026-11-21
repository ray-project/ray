"""Task admission, ordered schema reads and cancellation contracts."""

import json
import os
import subprocess
import sys
from copy import deepcopy
from typing import Any, Dict
from unittest.mock import Mock

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import pytest

import ray
from ray.data._internal.datasource_v2.common import schema_inference as inference
from ray.data._internal.datasource_v2.formats.orc import orc_datasource_v2
from ray.data._internal.datasource_v2.formats.parquet import parquet_datasource_v2
from ray.data._internal.datasource_v2.formats.parquet.parquet_datasource_v2 import (
    _read_parquet_schema,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
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
    "module,source_type",
    [
        (parquet_datasource_v2, parquet_datasource_v2.ParquetDatasourceV2),
        (orc_datasource_v2, orc_datasource_v2.OrcDatasourceV2),
    ],
)
def test_full_schema_preserves_subclass_schema_hook(monkeypatch, module, source_type):
    for suffix, value in dict(
        min_files=0, max_tasks=2, num_cpus=0.5, batch_size=2, io_concurrency=4
    ).items():
        monkeypatch.setattr(
            inference, f"_FULL_SCHEMA_INFERENCE_{suffix.upper()}", value
        )
    monkeypatch.setattr(ray, "cluster_resources", lambda: {"CPU": 4})
    monkeypatch.setattr(
        DataContext.get_current().execution_options, "label_selector", None
    )
    remote = Mock()
    remote.options.return_value = remote
    remote.remote.side_effect = RuntimeError("distributed path selected")
    monkeypatch.setattr(inference, "cached_remote_fn", Mock(return_value=remote))
    distributed = Mock(wraps=module.infer_full_schema_from_files)
    monkeypatch.setattr(module, "infer_full_schema_from_files", distributed)
    sample = FileManifest.construct_manifest(
        paths=["unused"], sizes=[1], chunk_metadatas=[None]
    )
    custom_schema = pa.schema([("custom", pa.int64())])
    hook = Mock(return_value=custom_schema)
    custom_type = type("CustomSource", (source_type,), {"infer_schema": hook})
    source = custom_type(["unused"], filesystem=pafs.LocalFileSystem())

    assert source.infer_full_schema(sample, ray_remote_args={}) is custom_schema
    hook.assert_called_once_with(sample)
    distributed.assert_not_called()
    remote.remote.assert_not_called()

    inherited_type = type("InheritedSource", (source_type,), {})
    inherited = inherited_type(["unused"], filesystem=pafs.LocalFileSystem())
    with pytest.raises(RuntimeError, match="distributed path selected"):
        inherited.infer_full_schema(sample, ray_remote_args={})
    distributed.assert_called_once()
    remote.remote.assert_called_once()


def _configure_small_schema_plan(monkeypatch, *, threshold=0, io_concurrency=4):
    for suffix, value in dict(
        min_files=threshold,
        max_tasks=2,
        num_cpus=2,
        batch_size=4,
        io_concurrency=io_concurrency,
    ).items():
        monkeypatch.setattr(
            inference, f"_FULL_SCHEMA_INFERENCE_{suffix.upper()}", value
        )
    monkeypatch.setattr(ray, "cluster_resources", lambda: {"CPU": 4})
    monkeypatch.setattr(
        DataContext.get_current().execution_options, "label_selector", None
    )


def _schema_manifest(paths):
    return FileManifest.construct_manifest(
        paths=paths,
        sizes=list(range(1, len(paths) + 1)),
        chunk_metadatas=[None] * len(paths),
    )


def _plan_schema(paths, read_args):
    return inference.infer_full_schema_from_files(
        _schema_manifest(paths),
        pafs.LocalFileSystem(),
        format_name="Parquet",
        read_schema=_read_parquet_schema,
        ray_remote_args=read_args,
        supports_distributed_reads=True,
    )


@pytest.mark.parametrize(
    "read_args,io_concurrency,threshold",
    [
        ({}, 4, 0),
        ({"num_cpus": None}, 4, 0),
        ({"num_cpus": 2}, 1, 0),
        ({"num_cpus": 2}, 4, 2),
    ],
    ids=["default_cpu", "none_cpu", "low_parallelism", "file_threshold"],
)
def test_full_schema_falls_back_before_scheduling(
    monkeypatch, read_args, io_concurrency, threshold
):
    _configure_small_schema_plan(
        monkeypatch, threshold=threshold, io_concurrency=io_concurrency
    )
    factory = Mock()
    monkeypatch.setattr(inference, "cached_remote_fn", factory)
    original = deepcopy(read_args)
    result = _plan_schema(["a", "b"], read_args)
    assert result is None
    factory.assert_not_called()
    assert read_args == original


@pytest.mark.parametrize("read_selector", [None, {"pool": "read"}])
def test_full_schema_forwards_task_options(monkeypatch, read_selector):
    _configure_small_schema_plan(monkeypatch)
    context = DataContext.get_current()
    selector = {"pool": "context", "zone": "a"}
    monkeypatch.setattr(context.execution_options, "label_selector", selector)
    args: Dict[str, Any] = {
        "num_cpus": 2,
        "runtime_env": {"env_vars": {"X": "1"}},
        "scheduling_strategy": "SPREAD",
        "fallback_strategy": [{"label_selector": {"pool": "backup"}}],
    }
    if read_selector is not None:
        args["label_selector"] = read_selector
    original = deepcopy(args)
    remote = Mock()
    remote.options.return_value = remote
    remote.remote.side_effect = RuntimeError("scheduled")
    factory = Mock(return_value=remote)
    monkeypatch.setattr(inference, "cached_remote_fn", factory)
    with pytest.raises(RuntimeError, match="scheduled"):
        _plan_schema(["a"], args)
    factory.assert_called_once_with(inference._read_schema_batch, max_retries=2)
    remote.options.assert_called_once_with(
        num_cpus=2,
        runtime_env=args["runtime_env"],
        scheduling_strategy="SPREAD",
        fallback_strategy=args["fallback_strategy"],
        label_selector={**selector, **(read_selector or {})},
    )
    remote.remote.assert_called_once_with(
        ["a"],
        [1],
        pafs.LocalFileSystem(),
        _read_parquet_schema,
        "Parquet",
        context,
        4,
        None,
    )
    assert args == original and selector == {"pool": "context", "zone": "a"}


def test_full_schema_orders_bounded_out_of_order_results(monkeypatch):
    _configure_small_schema_plan(monkeypatch)
    metadata = {b"source": b"first"}
    nested = pa.struct(
        [
            ("value", pa.int64()),
            ("items", pa.list_(pa.int64())),
            ("mapping", pa.map_(pa.string(), pa.int64())),
        ]
    )
    field = pa.field("id", nested, nullable=False, metadata=metadata)
    a = pa.schema([field], metadata=metadata)
    # Preserve metadata-only and nullability-only differences in candidate order.
    b = pa.schema(
        [field.with_metadata({b"source": b"other"})],
        metadata={b"source": b"other"},
    )
    c = pa.schema([field.with_nullable(True)], metadata=metadata)
    d = pa.schema([pa.field("extra", pa.string())])
    e = pa.schema([pa.field("tail", pa.int32())])
    refs = [Mock() for _ in range(3)]
    payloads = {
        refs[0]: [(0, a.serialize().to_pybytes(), False)],
        refs[1]: [
            (0, b.serialize().to_pybytes(), False),
            (1, c.serialize().to_pybytes(), False),
            (3, d.serialize().to_pybytes(), False),
        ],
        refs[2]: [(0, e.serialize().to_pybytes(), False)],
    }
    remote = Mock()
    remote.options.return_value = remote
    remote.remote.side_effect = refs
    monkeypatch.setattr(inference, "cached_remote_fn", Mock(return_value=remote))
    wait = Mock(
        side_effect=[
            ([refs[1]], [refs[0]]),
            ([refs[2]], [refs[0]]),
            ([refs[0]], []),
        ]
    )
    monkeypatch.setattr(ray, "wait", wait)
    monkeypatch.setattr(ray, "get", lambda ref: payloads[ref])
    cancel = Mock()
    monkeypatch.setattr(ray, "cancel", cancel)
    merge = Mock(wraps=inference.unify_schemas_with_validation)
    monkeypatch.setattr(inference, "unify_schemas_with_validation", merge)
    paths = [f"f{i}" for i in range(7)]
    actual = _plan_schema(paths, {"num_cpus": 2})
    expected = pa.schema(
        [
            pa.field("id", nested, metadata=metadata),
            pa.field("extra", pa.string()),
            pa.field("tail", pa.int32()),
        ],
        metadata=metadata,
    )
    assert actual is not None and actual.equals(expected, check_metadata=True)
    assert [call.args[0] for call in remote.remote.call_args_list] == [
        paths[:1],
        paths[1:5],
        paths[5:],
    ]
    assert [call.args[1] for call in remote.remote.call_args_list] == [
        [1],
        [2, 3, 4, 5],
        [6, 7],
    ]
    assert [len(call.args[0]) for call in wait.call_args_list] == [2, 2, 1]
    assert [call.kwargs["timeout"] for call in wait.call_args_list] == [
        inference._FULL_SCHEMA_INFERENCE_FIRST_RESULT_TIMEOUT_S,
        None,
        None,
    ]
    merge.assert_called_once()
    received = merge.call_args.args[0]
    assert len(received) == 5
    assert all(
        actual.equals(expected, check_metadata=True)
        for actual, expected in zip(received, [a, b, c, d, e])
    )
    cancel.assert_not_called()


def test_first_schema_result_timeout_cancels_and_falls_back(monkeypatch):
    for suffix, value in dict(min_files=0, max_tasks=2, batch_size=2).items():
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
    result = _plan_schema(["a", "b", "c"], {})
    assert result is None
    assert [call.args[0] for call in remote.remote.call_args_list] == [
        ["a"],
        ["b", "c"],
    ]
    assert [call.args[1] for call in remote.remote.call_args_list] == [[1], [2, 3]]
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
    [
        ("wait", KeyboardInterrupt),
        ("get", OSError),
        ("decode", pa.ArrowInvalid),
        ("parquet_decode", OSError),
    ],
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
    monkeypatch.setattr(
        ray, "get", lambda ref: [(0, b"invalid", stage == "parquet_decode")]
    )
    if stage == "decode":
        monkeypatch.setattr(
            pa.ipc, "read_schema", Mock(side_effect=error("schema failure"))
        )
    elif stage == "parquet_decode":
        monkeypatch.setattr(
            pq, "read_schema", Mock(side_effect=error("schema failure"))
        )
    elif stage in ("wait", "get"):
        monkeypatch.setattr(ray, stage, Mock(side_effect=error("schema failure")))
    cancel = Mock()
    monkeypatch.setattr(ray, "cancel", cancel)
    with pytest.raises(error, match="schema failure"):
        _plan_schema(["a", "b"], {})
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

    read_metadata = Mock()
    result = inference._read_schema_batch(
        paths,
        [i + 1 for i in range(len(paths))],
        pafs.LocalFileSystem(),
        read,
        "Parquet",
        context,
        4,
        read_metadata,
    )
    assert [index for index, _, _ in result] == [0, 1, 3]
    assert [payload for _, payload, _ in result] == [
        values[i].serialize().to_pybytes() for i in range(3)
    ]
    assert all(not parquet_encoded for _, _, parquet_encoded in result)
    read_metadata.assert_not_called()


@pytest.mark.parametrize("encoding_failure", ["unsupported", "lossy"])
def test_worker_uses_original_metadata_only_for_encoding_failure(
    monkeypatch, encoding_failure
):
    schema = Mock()
    schema.serialize.return_value = pa.schema([("a", pa.int64())]).serialize()
    schema.equals.side_effect = (
        [False, True] if encoding_failure == "unsupported" else [False, False, True]
    )
    write_metadata = Mock(
        side_effect=(
            pa.ArrowNotImplementedError("encoding")
            if encoding_failure == "unsupported"
            else None
        )
    )
    monkeypatch.setattr(pq, "write_metadata", write_metadata)
    monkeypatch.setattr(pq, "read_schema", Mock(return_value=pa.schema([])))
    read_metadata = Mock(return_value=b"original metadata")
    filesystem = pafs.LocalFileSystem()
    result = inference._read_schema_batch(
        ["test.parquet"],
        [123],
        filesystem,
        lambda *args: schema,
        "Parquet",
        DataContext.get_current(),
        1,
        read_metadata,
    )
    assert result == [(0, b"original metadata", True)]
    read_metadata.assert_called_once_with("test.parquet", filesystem, 123)


@pytest.mark.parametrize(
    "failure,error,message",
    [
        pytest.param("read", OSError, "footer read failed", id="read_error"),
        pytest.param(
            "changed", pa.ArrowInvalid, "original Parquet metadata", id="changed_schema"
        ),
    ],
)
def test_worker_preserves_metadata_failures(monkeypatch, failure, error, message):
    schema = Mock()
    schema.serialize.return_value = pa.schema([("a", pa.int64())]).serialize()
    schema.equals.return_value = False
    monkeypatch.setattr(
        pq, "write_metadata", Mock(side_effect=pa.ArrowNotImplementedError("encoding"))
    )
    if failure == "read":
        read_metadata = Mock(side_effect=OSError("footer read failed"))
    else:
        monkeypatch.setattr(pq, "read_schema", Mock(return_value=pa.schema([])))
        read_metadata = Mock(return_value=b"different metadata")
    with pytest.raises(error, match=message):
        inference._read_schema_batch(
            ["test.parquet"],
            [123],
            pafs.LocalFileSystem(),
            lambda *args: schema,
            "Parquet",
            DataContext.get_current(),
            1,
            read_metadata,
        )


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
