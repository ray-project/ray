"""Unit tests for multi-host TPU batch scheduling.

Mocks ``slice_placement_group`` and placement-group readiness. Covers config
validation, map_batches kwargs, and processor close lifecycle.
"""

from __future__ import annotations

import gc
import inspect
import logging
from typing import Any, Dict, Optional
from unittest.mock import MagicMock

import pytest

import ray
import ray.llm._internal.common.accelerators as accelerators
from ray.data.llm import build_processor, vLLMEngineProcessorConfig
from ray.llm._internal.batch.processor import vllm_engine_proc
from ray.llm._internal.batch.processor.base import Processor
from ray.llm._internal.batch.stages.vllm_engine_stage import vLLMEngineStage
from ray.llm._internal.common.accelerators import (
    DEFAULT_USER_CPU_PER_HOST,
    PARENT_ACTOR_CPU_RESERVE,
    TPUAccelerator,
    TPUConfig,
)
from ray.util.placement_group import PlacementGroup
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy
from ray.util.tpu import SlicePlacementGroup, slice_placement_group

_CPU_FLOOR = PARENT_ACTOR_CPU_RESERVE + DEFAULT_USER_CPU_PER_HOST


def _schedule(
    backend: TPUAccelerator,
    *,
    accelerator_type: str = "TPU-V6E",
    tensor_parallel_size: int,
    placement_group_config: Optional[Dict[str, Any]] = None,
    **engine_overrides,
):
    engine_kwargs = {
        "tensor_parallel_size": tensor_parallel_size,
        "pipeline_parallel_size": 1,
        "data_parallel_size": 1,
        **engine_overrides,
    }
    return backend.build_batch_scheduling_options(
        accelerator_type=accelerator_type,
        engine_kwargs=engine_kwargs,
        placement_group_config=placement_group_config,
    )


@pytest.fixture
def stub_slice_pg(monkeypatch):
    handle = MagicMock(spec=SlicePlacementGroup)
    handle.placement_group = MagicMock(spec=PlacementGroup)
    handle.num_hosts = 4
    handle.num_bundles = 4
    create = MagicMock(return_value=handle)
    monkeypatch.setattr(accelerators, "slice_placement_group", create)
    monkeypatch.setattr(
        handle.placement_group, "ready", MagicMock(return_value=MagicMock())
    )
    monkeypatch.setattr(accelerators.ray, "get", MagicMock())
    monkeypatch.setattr(
        vllm_engine_proc, "download_model_files", lambda *a, **k: "/tmp/mock-model"
    )
    return handle, create


def _topo_config(**kwargs):
    return vLLMEngineProcessorConfig(
        model_source="m",
        accelerator_type="TPU-V6E",
        accelerator_config={"kind": "tpu", "topology": "4x4"},
        **kwargs,
    )


@pytest.mark.parametrize(
    "kwargs, match",
    [
        ({"accelerator_type": "TPU-V6E"}, "requires accelerator_config with topology"),
        (
            {
                "accelerator_type": "TPU-V6E",
                "accelerator_config": {"kind": "tpu", "topology": "4x4"},
                "concurrency": 2,
            },
            "concurrency=1 or \\(1, 1\\)",
        ),
        (
            {
                "accelerator_type": "TPU-V6E",
                "accelerator_config": {"kind": "tpu", "topology": "4x4"},
                "concurrency": (1, 2),
            },
            "concurrency=1 or \\(1, 1\\)",
        ),
        (
            {
                "accelerator_type": "TPU-V6E",
                "accelerator_config": {"kind": "gpu"},
            },
            "GPUConfig cannot be used with TPU accelerator_type",
        ),
        ({"accelerator_config": {"kind": "cpu"}}, "CPUConfig is not supported"),
        ({"accelerator_type": "CPU"}, "Explicit 'CPU' accelerator type"),
    ],
)
def test_rejects_invalid_processor_config(kwargs, match):
    with pytest.raises(ValueError, match=match):
        vLLMEngineProcessorConfig(model_source="m", **kwargs)


@pytest.mark.parametrize(
    "topology, accel, tp, pp, dp, chips_per_vm, strategy, expect_strategy",
    [
        ("4x4", "TPU-V6E", 16, 1, 1, None, None, "SPREAD"),
        ("4x4", "TPU-V6E", 8, 2, 1, None, None, "SPREAD"),
        ("4x4", "TPU-V6E", 8, 1, 2, None, None, "SPREAD"),
        ("2x4", "TPU-V6E", 8, 1, 1, 4, "PACK", "PACK"),
        ("2x4", "TPU-V6E", 8, 1, 1, 4, "SPREAD", "SPREAD"),
        ("2x4", "TPU-V6E", 8, 1, 1, 4, "STRICT_SPREAD", "STRICT_SPREAD"),
    ],
)
def test_slice_pg_kwargs(
    stub_slice_pg, topology, accel, tp, pp, dp, chips_per_vm, strategy, expect_strategy
):
    handle, create = stub_slice_pg
    pg_config = {"bundle_per_worker": {"TPU": 1}}
    if strategy is not None:
        pg_config["strategy"] = strategy
    kwargs, close_fn = _schedule(
        TPUAccelerator(TPUConfig(topology=topology, chips_per_vm=chips_per_vm)),
        accelerator_type=accel,
        tensor_parallel_size=tp,
        pipeline_parallel_size=pp,
        data_parallel_size=dp,
        placement_group_config=pg_config,
    )
    slice_kwargs = create.call_args.kwargs
    inspect.signature(slice_placement_group).bind(**slice_kwargs)
    assert slice_kwargs["topology"] == topology
    assert slice_kwargs["strategy"] == expect_strategy
    assert slice_kwargs.get("chips_per_vm") == chips_per_vm
    assert kwargs["num_cpus"] == _CPU_FLOOR
    assert kwargs["num_cpus"] == slice_kwargs["resources_per_bundle"]["CPU"]
    close_fn()
    handle.shutdown.assert_called_once()


def test_placement_group_names_are_unique(stub_slice_pg):
    _, create = stub_slice_pg
    closes = []
    for _ in range(2):
        _, close_fn = _schedule(
            TPUAccelerator(TPUConfig(topology="4x4")),
            tensor_parallel_size=16,
        )
        closes.append(close_fn)
    names = [c.kwargs["name"] for c in create.call_args_list]
    assert len(names) == 2
    assert names[0] != names[1]
    assert names[0].startswith("ray-data-llm-tpu-4x4-")
    assert names[1].startswith("ray-data-llm-tpu-4x4-")
    for close_fn in closes:
        close_fn()


def test_default_bundle_omits_tpu(stub_slice_pg):
    _, create = stub_slice_pg
    _, close_fn = _schedule(
        TPUAccelerator(TPUConfig(topology="4x4")),
        tensor_parallel_size=16,
    )
    resources = create.call_args.kwargs["resources_per_bundle"]
    assert resources == {"CPU": float(_CPU_FLOOR)}
    assert "TPU" not in resources
    close_fn()


def test_cpu_only_template_omits_tpu(stub_slice_pg):
    _, create = stub_slice_pg
    _, close_fn = _schedule(
        TPUAccelerator(TPUConfig(topology="4x4")),
        tensor_parallel_size=16,
        placement_group_config={"bundle_per_worker": {"CPU": 4}},
    )
    resources = create.call_args.kwargs["resources_per_bundle"]
    assert "TPU" not in resources
    assert resources["CPU"] == 4.0
    close_fn()


def test_cpu_floor_warns_on_override(stub_slice_pg, caplog):
    _, create = stub_slice_pg
    with caplog.at_level(logging.WARNING, logger=accelerators.logger.name):
        _, close_fn = _schedule(
            TPUAccelerator(TPUConfig(topology="4x4")),
            tensor_parallel_size=16,
            placement_group_config={"bundle_per_worker": {"TPU": 1, "CPU": 1}},
        )
    resources = create.call_args.kwargs["resources_per_bundle"]
    assert resources["CPU"] == float(_CPU_FLOOR)
    assert any(
        "Raising placement_group_config CPU" in r.message for r in caplog.records
    )
    close_fn()


def test_default_bundle_does_not_warn_cpu_floor(stub_slice_pg, caplog):
    with caplog.at_level(logging.WARNING, logger=accelerators.logger.name):
        _, close_fn = _schedule(
            TPUAccelerator(TPUConfig(topology="4x4")),
            tensor_parallel_size=16,
        )
    assert not any(
        "Raising placement_group_config CPU" in r.message for r in caplog.records
    )
    close_fn()


def test_defaults_spread_when_strategy_unset(stub_slice_pg):
    handle, create = stub_slice_pg
    cfg = _topo_config(
        engine_kwargs={"tensor_parallel_size": 16},
        placement_group_config={"bundle_per_worker": {"TPU": 1}},
        tokenize=False,
        detokenize=False,
        apply_chat_template=False,
    )
    # Omitted strategy with TPU topology defaults to SPREAD.
    assert cfg.placement_group_config["strategy"] == "SPREAD"
    processor = build_processor(cfg)
    assert create.call_args.kwargs["strategy"] == "SPREAD"
    processor.close()
    handle.shutdown.assert_called_once()


def test_gpu_placement_group_config_unset_strategy():
    """Unset strategy resolves to PACK (PlacementGroupConfig default)."""
    cfg = vLLMEngineProcessorConfig(
        model_source="m",
        placement_group_config={"bundle_per_worker": {"CPU": 1, "GPU": 1}},
        tokenize=False,
        detokenize=False,
        apply_chat_template=False,
    )
    assert cfg.placement_group_config == {
        "bundle_per_worker": {"CPU": 1.0, "GPU": 1.0},
        "bundles": None,
        "strategy": "PACK",
    }


def test_gpu_explicit_strategy_preserved():
    cfg = vLLMEngineProcessorConfig(
        model_source="m",
        placement_group_config={
            "bundles": [{"CPU": 1, "GPU": 1}],
            "strategy": "STRICT_PACK",
        },
        tokenize=False,
        detokenize=False,
        apply_chat_template=False,
    )
    assert cfg.placement_group_config["strategy"] == "STRICT_PACK"


def test_gpu_stage_scheduling_uses_pack_when_unset(monkeypatch):
    captured = {}

    def fake_pg(**kwargs):
        captured.update(kwargs)
        return MagicMock(bundle_specs=kwargs.get("bundles"))

    monkeypatch.setattr(
        "ray.llm._internal.batch.stages.vllm_engine_stage.ray.util.placement_group",
        fake_pg,
    )
    stage = vLLMEngineStage(
        fn_constructor_kwargs=dict(
            model="m",
            engine_kwargs={"tensor_parallel_size": 2},
            task_type="generate",
            placement_group_config={
                "bundle_per_worker": {"CPU": 1.0, "GPU": 1.0},
                "bundles": None,
                "strategy": "PACK",
            },
        ),
        map_batches_kwargs=dict(accelerator_type="A100"),
    )
    stage.map_batches_kwargs["ray_remote_args_fn"]()
    assert captured.get("strategy") == "PACK"
    assert len(captured["bundles"]) == 2


@pytest.mark.parametrize(
    "bundle",
    [
        {"TPU": "4"},
        {"TPU": None},
        {"TPU": True},
        {"TPU": 1.5},
        {"TPU": 0},
        {"TPU": float("nan")},
        {"GPU": 1},
        {"GPU": "1"},
    ],
)
def test_bundle_resource_type_validation(stub_slice_pg, bundle):
    _, create = stub_slice_pg
    with pytest.raises(ValueError):
        _schedule(
            TPUAccelerator(TPUConfig(topology="4x4")),
            tensor_parallel_size=16,
            placement_group_config={"bundle_per_worker": bundle},
        )
    create.assert_not_called()


def test_multi_bundle_list_warns(stub_slice_pg, caplog):
    with caplog.at_level(logging.WARNING, logger=accelerators.logger.name):
        _, close_fn = _schedule(
            TPUAccelerator(TPUConfig(topology="4x4")),
            tensor_parallel_size=16,
            placement_group_config={
                "bundles": [{"TPU": 1, "CPU": 2}, {"TPU": 1, "CPU": 2}]
            },
        )
    assert any("specified 2 bundles" in r.message for r in caplog.records)
    close_fn()


@pytest.mark.parametrize("bad", ["4xx4", "abc", "4x", "-4x4"])
def test_topology_rejects_malformed(bad):
    with pytest.raises(ValueError, match="Invalid TPU topology"):
        TPUConfig(topology=bad)


def test_topology_normalizes_case_and_whitespace():
    assert TPUConfig(topology=" 4X4 ").topology == "4x4"


@pytest.mark.parametrize(
    "option_kwargs, match",
    [
        ({"distributed_executor_backend": "uni"}, "distributed_executor_backend"),
        (
            {"tensor_parallel_size": 8},
            r"tensor_parallel_size \* pipeline_parallel_size \* "
            r"data_parallel_size must be 16",
        ),
        (
            {"tensor_parallel_size": 8, "pipeline_parallel_size": 3},
            r"tensor_parallel_size \* pipeline_parallel_size \* "
            r"data_parallel_size must be 16",
        ),
        (
            {"tensor_parallel_size": 16, "data_parallel_size": 2},
            r"tensor_parallel_size \* pipeline_parallel_size \* "
            r"data_parallel_size must be 16",
        ),
        (
            {"pipeline_parallel_size": 0},
            "pipeline_parallel_size must be a positive integer",
        ),
        ({"data_parallel_size": 0}, "data_parallel_size must be a positive integer"),
        ({"data_parallel_size": True}, "data_parallel_size must be a positive integer"),
        ({"data_parallel_size": "2"}, "data_parallel_size must be a positive integer"),
        (
            {"placement_group_config": {"bundle_per_worker": {"GPU": 1, "TPU": 1}}},
            "GPU resources are not supported",
        ),
        (
            {"placement_group_config": {"strategy": "PACK"}},
            "must specify bundle_per_worker or bundles",
        ),
        (
            {"placement_group_config": {"bundles": []}},
            "must be non-empty",
        ),
    ],
)
def test_rejects_invalid_schedule_inputs(stub_slice_pg, option_kwargs, match):
    _, create = stub_slice_pg
    kwargs = {"tensor_parallel_size": 16, **option_kwargs}
    with pytest.raises(ValueError, match=match):
        _schedule(TPUAccelerator(TPUConfig(topology="4x4")), **kwargs)
    create.assert_not_called()


def test_builder_lifecycle(stub_slice_pg, monkeypatch):
    handle, create = stub_slice_pg
    cfg = _topo_config(
        engine_kwargs={"tensor_parallel_size": 16},
        tokenize=False,
        detokenize=False,
        apply_chat_template=False,
    )
    with build_processor(cfg) as processor:
        assert isinstance(processor, Processor)
        stage = processor.get_stage_by_name("vLLMEngineStage")
        strategy = stage.map_batches_kwargs["scheduling_strategy"]
        assert isinstance(strategy, PlacementGroupSchedulingStrategy)
        assert strategy.placement_group_bundle_index == 0
        assert strategy.placement_group_capture_child_tasks is True
        assert stage.map_batches_kwargs["num_gpus"] == 0
        assert stage.map_batches_kwargs["resources"] == {}
        assert "ray_remote_args_fn" not in stage.map_batches_kwargs
        assert "placement_group_config" not in stage.fn_constructor_kwargs
        rebuilt = vLLMEngineStage(
            fn_constructor_kwargs=dict(stage.fn_constructor_kwargs),
            map_batches_kwargs=dict(stage.map_batches_kwargs),
        )
        assert (
            rebuilt.map_batches_kwargs["scheduling_strategy"]
            is stage.map_batches_kwargs["scheduling_strategy"]
        )
        assert "placement_group_config" not in rebuilt.fn_constructor_kwargs
    handle.shutdown.assert_called_once()

    # Construction failure after slice acquisition releases the slice.
    monkeypatch.setattr(
        ray.data,
        "ActorPoolStrategy",
        MagicMock(side_effect=RuntimeError("bad pool")),
    )
    with pytest.raises(RuntimeError, match="bad pool"):
        build_processor(cfg)
    assert handle.shutdown.call_count == 2


def test_gpu_builder_does_not_create_slice_pg(stub_slice_pg):
    _, create = stub_slice_pg
    cfg = vLLMEngineProcessorConfig(
        model_source="m",
        engine_kwargs={"tensor_parallel_size": 2},
        tokenize=False,
        detokenize=False,
        apply_chat_template=False,
    )
    processor = build_processor(cfg)
    create.assert_not_called()
    stage = processor.get_stage_by_name("vLLMEngineStage")
    assert "ray_remote_args_fn" in stage.map_batches_kwargs
    assert "scheduling_strategy" not in stage.map_batches_kwargs
    processor.close()


def test_close_retry_and_unclosed_finalizer(stub_slice_pg, caplog):
    handle, _ = stub_slice_pg
    cfg = _topo_config(
        engine_kwargs={"tensor_parallel_size": 16},
        tokenize=False,
        detokenize=False,
        apply_chat_template=False,
    )
    handle.shutdown.side_effect = [RuntimeError("boom"), None, None]
    processor = build_processor(cfg)
    with pytest.raises(RuntimeError, match="boom"):
        processor.close()
    assert processor._close_fn is not None
    processor.close()
    assert processor._close_fn is None

    shutdown_before_finalizer = handle.shutdown.call_count
    processor = build_processor(cfg)
    with caplog.at_level(
        logging.WARNING, logger="ray.llm._internal.batch.processor.base"
    ):
        del processor
        gc.collect()
    assert any("garbage-collected without close()" in r.message for r in caplog.records)
    assert handle.shutdown.call_count == shutdown_before_finalizer + 1


def test_eager_timeout_shuts_down(stub_slice_pg, monkeypatch):
    handle, create = stub_slice_pg

    def _timeout(*args, **kwargs):
        raise ray.exceptions.GetTimeoutError("timed out")

    monkeypatch.setattr(accelerators.ray, "get", _timeout)
    with pytest.raises(TimeoutError, match="Timed out"):
        _schedule(TPUAccelerator(TPUConfig(topology="4x4")), tensor_parallel_size=16)
    create.assert_called_once()
    handle.shutdown.assert_called_once()


def test_builder_does_not_mutate_caller_engine_kwargs(stub_slice_pg):
    cfg = _topo_config(
        engine_kwargs={"tensor_parallel_size": 16},
        tokenize=False,
        detokenize=False,
        apply_chat_template=False,
    )
    assert "distributed_executor_backend" not in cfg.engine_kwargs
    processor = build_processor(cfg)
    assert "distributed_executor_backend" not in cfg.engine_kwargs
    processor.close()
