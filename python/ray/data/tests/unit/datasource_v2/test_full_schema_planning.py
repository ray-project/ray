"""Task admission, ordered schema reads and cancellation contracts."""

from copy import deepcopy
from typing import Any, Dict
from unittest.mock import Mock

import pyarrow as pa
import pyarrow.fs as pafs
import pytest

import ray
from ray.data._internal.datasource_v2.common import schema_inference as inference
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.util import unify_schemas_with_validation
from ray.data.context import DataContext
from ray.util.scheduling_strategies import (
    NodeAffinitySchedulingStrategy,
    PlacementGroupSchedulingStrategy,
)


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
    "suffix,value",
    [
        ("min_files", -1),
        ("max_tasks", 0),
        ("batch_size", 0),
        ("io_concurrency", 0),
        ("batch_size", 1.5),
        ("max_tasks", True),
        ("num_cpus", 0),
        ("num_cpus", float("nan")),
        ("num_cpus", True),
        ("num_cpus", "0.5"),
    ],
)
def test_invalid_schema_config(monkeypatch, suffix, value):
    name = f"full_schema_inference_{suffix}"
    monkeypatch.setattr(DataContext.get_current(), name, value)
    sample = FileManifest.construct_manifest(paths=[], sizes=[], chunk_metadatas=[])
    with pytest.raises(ValueError, match=name):
        inference.infer_full_schema_from_files(
            sample,
            pafs.LocalFileSystem(),
            format_name="Parquet",
            ray_remote_args={},
            supports_distributed_reads=True,
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
        monkeypatch.setattr(context, f"full_schema_inference_{suffix}", value)
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
        paths=paths, sizes=[1] * 40, chunk_metadatas=[None] * 40
    )
    remote = Mock()
    remote.options.return_value = remote
    payloads, submitted, widths = {}, [], []

    def submit(batch, *unused):
        ref = Mock(index=len(submitted))
        payloads[ref] = [schemas[path].serialize().to_pybytes() for path in batch]
        submitted.append(batch)
        return ref

    def wait(refs, **unused):
        widths.append(len(refs))
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
        ray_remote_args=args,
        supports_distributed_reads=True,
    )
    if window:
        assert actual is not None and expected is not None
        assert actual.equals(expected, check_metadata=True)
        assert submitted == [paths[i : i + 8] for i in range(0, 40, 8)]
        assert max(widths) == window
        factory.assert_called_once_with(inference._read_schema_batch, max_retries=2)
        remote.options.assert_called_once_with(
            num_cpus=2,
            runtime_env=args["runtime_env"],
            scheduling_strategy="SPREAD",
            label_selector={**selector, **args.get("label_selector", {})},
        )
        merge.assert_called_once()
        received = merge.call_args.args[0]
        assert len(received) == len(paths)
        assert all(
            a.equals(b, check_metadata=True) for a, b in zip(received, schemas.values())
        )
        assert decode.call_count == {"same": 1, "varied": 20, "unique": 40}[schema_case]
        if schema_case == "same":
            assert all(schema is received[0] for schema in received)
            decode.reset_mock()
            again = inference.infer_full_schema_from_files(
                sample,
                pafs.LocalFileSystem(),
                format_name="Parquet",
                ray_remote_args=args,
                supports_distributed_reads=True,
            )
            assert again is not None and again.equals(expected, check_metadata=True)
            decode.assert_called_once()
            assert merge.call_count == 2
    else:
        assert actual is None
        factory.assert_not_called()
        merge.assert_not_called()
        decode.assert_not_called()
    cancel.assert_not_called()
    assert args == original and selector == {"pool": "context", "zone": "a"}


@pytest.mark.parametrize(
    "stage,error",
    [("wait", KeyboardInterrupt), ("get", OSError), ("decode", pa.ArrowInvalid)],
)
def test_schema_failure_cancels_pending_tasks(monkeypatch, stage, error):
    context = DataContext.get_current()
    for suffix, value in dict(min_files=0, max_tasks=2, batch_size=1).items():
        monkeypatch.setattr(context, f"full_schema_inference_{suffix}", value)
    remote = Mock()
    remote.options.return_value = remote
    refs = [Mock(), Mock()]
    remote.remote.side_effect = refs
    monkeypatch.setattr(inference, "cached_remote_fn", lambda *a, **k: remote)
    monkeypatch.setattr(ray, "cluster_resources", lambda: {"CPU": 1})
    monkeypatch.setattr(ray, "wait", lambda *a, **k: ([refs[0]], [refs[1]]))
    monkeypatch.setattr(ray, "get", lambda ref: [b"invalid"])
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
            ray_remote_args={},
            supports_distributed_reads=True,
        )
    assert [call.args[0] for call in cancel.call_args_list] == (
        refs if stage == "wait" else refs[1:]
    )
    assert all(call.kwargs == {"force": False} for call in cancel.call_args_list)


def test_worker_read_failure_preserves_file_and_cause(tmp_path):
    with pytest.raises(FileNotFoundError, match="missing.parquet") as error:
        inference._read_schema_batch(
            [str(tmp_path / "missing.parquet")],
            pafs.LocalFileSystem(),
            "Parquet",
            DataContext.get_current(),
        )
    assert isinstance(error.value.__cause__, FileNotFoundError)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
