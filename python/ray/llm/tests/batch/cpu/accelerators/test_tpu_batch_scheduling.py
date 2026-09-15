"""Unit and integration tests for multi-host TPU batch scheduling.

Unit tests mock ``slice_placement_group`` (bypassing reservation and PG creation) to cover config
validation, map_batches kwargs, and slice placement strategy.
"""

from __future__ import annotations

import inspect
import logging
import sys
from typing import Any, Dict, Optional
from unittest.mock import MagicMock

import pytest

import ray
import ray.llm._internal.common.accelerators as accelerators
from ray.data.llm import build_processor, vLLMEngineProcessorConfig
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
    kwargs = backend.build_batch_scheduling_options(
        accelerator_type=accelerator_type,
        engine_kwargs=engine_kwargs,
        placement_group_config=placement_group_config,
    )
    if "ray_remote_args_fn" in kwargs:
        kwargs["ray_remote_args_fn"]()
    return kwargs


@pytest.fixture
def stub_slice_pg(monkeypatch):
    handle = MagicMock(spec=SlicePlacementGroup)
    handle.placement_group = MagicMock(spec=PlacementGroup)
    handle.num_hosts = 4
    handle.num_bundles = 4
    create = MagicMock(return_value=handle)
    monkeypatch.setattr(accelerators, "slice_placement_group", create)
    return handle, create


def _topo_config(**kwargs):
    return vLLMEngineProcessorConfig(
        model_source="mock-model",
        accelerator_type="TPU-V6E",
        accelerator_config={"kind": "tpu", "topology": "4x4"},
        chat_template_stage={"enabled": False},
        tokenize_stage={"enabled": False},
        detokenize_stage={"enabled": False},
        **kwargs,
    )


@pytest.mark.parametrize(
    "kwargs, match",
    [
        ({"accelerator_type": "TPU-V6E"}, "requires accelerator_config with topology"),
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
        ("4x4", "TPU-V6E", 16, 1, 1, None, None, "PACK"),
        ("4x4", "TPU-V6E", 8, 2, 1, None, None, "PACK"),
        ("4x4", "TPU-V6E", 8, 1, 2, None, None, "PACK"),
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
    kwargs = _schedule(
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


def test_default_bundle_omits_tpu(stub_slice_pg):
    _, create = stub_slice_pg
    _schedule(
        TPUAccelerator(TPUConfig(topology="4x4")),
        tensor_parallel_size=16,
    )
    resources = create.call_args.kwargs["resources_per_bundle"]
    assert resources == {"CPU": float(_CPU_FLOOR), "accelerator_type:TPU-V6E": 0.001}
    assert "TPU" not in resources


def test_cpu_only_template_omits_tpu(stub_slice_pg):
    _, create = stub_slice_pg
    _schedule(
        TPUAccelerator(TPUConfig(topology="4x4")),
        tensor_parallel_size=16,
        placement_group_config={"bundle_per_worker": {"CPU": 4}},
    )
    resources = create.call_args.kwargs["resources_per_bundle"]
    assert "TPU" not in resources
    assert resources["CPU"] == 4.0
    assert resources["accelerator_type:TPU-V6E"] == 0.001


def test_cpu_floor_warns_on_override(stub_slice_pg, caplog):
    _, create = stub_slice_pg
    with caplog.at_level(logging.WARNING):
        _schedule(
            TPUAccelerator(TPUConfig(topology="4x4")),
            tensor_parallel_size=16,
            placement_group_config={"bundle_per_worker": {"TPU": 1, "CPU": 1}},
        )
    resources = create.call_args.kwargs["resources_per_bundle"]
    assert resources["CPU"] == float(_CPU_FLOOR)
    assert any(
        "Raising placement_group_config CPU" in r.message for r in caplog.records
    )


def test_default_bundle_does_not_warn_cpu_floor(stub_slice_pg, caplog):
    with caplog.at_level(logging.WARNING):
        _schedule(
            TPUAccelerator(TPUConfig(topology="4x4")),
            tensor_parallel_size=16,
        )
    assert not any(
        "Raising placement_group_config CPU" in r.message for r in caplog.records
    )


def test_tpu_backend_strategy_defaults_to_pack_when_unset(stub_slice_pg):
    """Verify TPUAccelerator defaults strategy to PACK when unset in placement_group_config."""
    handle, create = stub_slice_pg
    backend = TPUAccelerator(TPUConfig(topology="4x4"))
    options = backend.build_batch_scheduling_options(
        accelerator_type="TPU-V6E",
        engine_kwargs={"tensor_parallel_size": 16},
        placement_group_config={"bundle_per_worker": {"TPU": 1}},
    )
    options["ray_remote_args_fn"]()
    assert create.call_args.kwargs["strategy"] == "PACK"


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
    with caplog.at_level(logging.WARNING):
        _schedule(
            TPUAccelerator(TPUConfig(topology="4x4")),
            tensor_parallel_size=16,
            placement_group_config={
                "bundles": [{"TPU": 1, "CPU": 2}, {"TPU": 1, "CPU": 2}]
            },
        )
    assert any("specified 2 bundles" in r.message for r in caplog.records)


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


def test_build_processor_reserves_nothing(stub_slice_pg):
    _, create = stub_slice_pg
    cfg = _topo_config(
        engine_kwargs={"tensor_parallel_size": 16},
    )
    processor = build_processor(cfg)
    assert isinstance(processor, Processor)
    create.assert_not_called()


def test_builder_lifecycle(stub_slice_pg):
    handle, create = stub_slice_pg
    cfg = _topo_config(
        engine_kwargs={"tensor_parallel_size": 16},
    )
    processor = build_processor(cfg)
    assert isinstance(processor, Processor)
    stage = processor.get_stage_by_name("vLLMEngineStage")
    assert "scheduling_strategy" not in stage.map_batches_kwargs

    create.assert_not_called()
    remote_args = stage.map_batches_kwargs["ray_remote_args_fn"]()
    create.assert_called_once()

    strategy = remote_args["scheduling_strategy"]
    assert isinstance(strategy, PlacementGroupSchedulingStrategy)
    assert strategy.placement_group_bundle_index == 0
    assert strategy.placement_group_capture_child_tasks is True
    assert stage.map_batches_kwargs["num_gpus"] == 0
    assert stage.map_batches_kwargs["resources"] == {}
    assert "placement_group_config" not in stage.fn_constructor_kwargs
    rebuilt = vLLMEngineStage(
        fn_constructor_kwargs=dict(stage.fn_constructor_kwargs),
        map_batches_kwargs=dict(stage.map_batches_kwargs),
    )
    assert (
        rebuilt.map_batches_kwargs["ray_remote_args_fn"]
        is stage.map_batches_kwargs["ray_remote_args_fn"]
    )
    assert "placement_group_config" not in rebuilt.fn_constructor_kwargs


def test_gpu_builder_does_not_create_slice_pg(stub_slice_pg):
    _, create = stub_slice_pg
    cfg = vLLMEngineProcessorConfig(
        model_source="mock-model",
        engine_kwargs={"tensor_parallel_size": 2},
        chat_template_stage={"enabled": False},
        tokenize_stage={"enabled": False},
        detokenize_stage={"enabled": False},
    )
    processor = build_processor(cfg)
    create.assert_not_called()
    stage = processor.get_stage_by_name("vLLMEngineStage")
    assert "ray_remote_args_fn" in stage.map_batches_kwargs
    assert "scheduling_strategy" not in stage.map_batches_kwargs


def test_scheduling_fn_creates_slice_per_actor_invocation(stub_slice_pg):
    """Each actor replica created in the pool invokes ray_remote_args_fn and gets a distinct slice."""
    _, create = stub_slice_pg
    cfg = _topo_config(engine_kwargs={"tensor_parallel_size": 16})
    processor = build_processor(cfg)
    fn = processor.get_stage_by_name("vLLMEngineStage").map_batches_kwargs[
        "ray_remote_args_fn"
    ]
    res1 = fn()
    res2 = fn()
    assert create.call_count == 2
    assert "scheduling_strategy" in res1
    assert "scheduling_strategy" in res2


def test_builder_does_not_mutate_caller_engine_kwargs(stub_slice_pg):
    cfg = _topo_config(
        engine_kwargs={"tensor_parallel_size": 16},
    )
    assert "distributed_executor_backend" not in cfg.engine_kwargs
    build_processor(cfg)
    assert "distributed_executor_backend" not in cfg.engine_kwargs


def test_tpu_batch_processor_real_slice_placement_group_integration(
    ray_tpu_cluster,
):
    """Integration test verifying real slice_placement_group creation and GCS registration
    on a simulated multi-host TPU cluster.
    """
    config = vLLMEngineProcessorConfig(
        model_source="mock-model",
        accelerator_type="TPU-V6E",
        accelerator_config=TPUConfig(topology="4x4"),
        engine_kwargs={"tensor_parallel_size": 16},
        chat_template_stage={"enabled": False},
        tokenize_stage={"enabled": False},
        detokenize_stage={"enabled": False},
    )
    processor = build_processor(config)
    stage = processor.get_stage_by_name("vLLMEngineStage")

    # Ray Data invokes ray_remote_args_fn to reserve the slice and acquire the real PG.
    remote_args = stage.map_batches_kwargs["ray_remote_args_fn"]()
    strat = remote_args["scheduling_strategy"]
    pg = strat.placement_group
    assert isinstance(pg, PlacementGroup)
    ray.get(pg.ready(), timeout=60)

    # Verify the PG was created in GCS with PACK strategy and 4 bundles (1 per VM host).
    pg_table = ray.util.placement_group_table(pg)
    assert pg_table["state"] == "CREATED"
    assert pg_table["strategy"] == "PACK"
    assert len(pg_table["bundles"]) == 4

    # Remove PG and verify teardown in GCS.
    ray.util.remove_placement_group(pg)
    pg_table_after = ray.util.placement_group_table(pg)
    assert pg_table_after["state"] == "REMOVED"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
