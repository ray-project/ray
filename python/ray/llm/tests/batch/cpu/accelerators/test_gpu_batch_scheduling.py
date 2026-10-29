"""Unit tests for GPU and CPU accelerator batch scheduling."""

from __future__ import annotations

import sys
from unittest.mock import MagicMock

import pytest

from ray.data.llm import vLLMEngineProcessorConfig
from ray.llm._internal.common.accelerators import (
    CPUAccelerator,
    CPUConfig,
    GPUAccelerator,
    GPUConfig,
)


def test_gpu_placement_group_config_unset_strategy():
    """Unset strategy resolves to PACK (PlacementGroupConfig default)."""
    cfg = vLLMEngineProcessorConfig(
        model_source="m",
        placement_group_config={"bundle_per_worker": {"CPU": 1, "GPU": 1}},
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
    )
    assert cfg.placement_group_config["strategy"] == "STRICT_PACK"


def test_gpu_stage_scheduling_uses_pack_when_unset(monkeypatch):
    """Verify GPU scheduling defaults to PACK if no strategy is specified."""
    captured = {}

    def fake_placement_group(*args, **kwargs):
        captured.update(kwargs)
        return MagicMock()

    monkeypatch.setattr(
        "ray.llm._internal.common.accelerators.placement_group",
        fake_placement_group,
    )

    cfg = vLLMEngineProcessorConfig(
        model_source="m",
        engine_kwargs={"tensor_parallel_size": 2},
        placement_group_config={"bundle_per_worker": {"CPU": 1.0, "GPU": 1.0}},
    )
    backend = GPUAccelerator(GPUConfig())
    map_batches_kwargs = backend.build_batch_scheduling_options(
        accelerator_type=cfg.accelerator_type,
        engine_kwargs=dict(cfg.engine_kwargs),
        placement_group_config=cfg.placement_group_config,
    )
    map_batches_kwargs["ray_remote_args_fn"]()
    assert captured["strategy"] == "PACK"


def test_gpu_accelerator_constraint_preserved_with_explicit_pg():
    """Verify that an explicit placement group preserves the accelerator_type and resource counts."""
    cfg = vLLMEngineProcessorConfig(
        model_source="m",
        accelerator_type="A100",
        engine_kwargs={"tensor_parallel_size": 1},
        placement_group_config={"bundle_per_worker": {"CPU": 1.0, "GPU": 1.0}},
    )
    backend = GPUAccelerator(GPUConfig())
    map_batches_kwargs = backend.build_batch_scheduling_options(
        accelerator_type=cfg.accelerator_type,
        engine_kwargs=dict(cfg.engine_kwargs),
        placement_group_config=cfg.placement_group_config,
    )
    assert map_batches_kwargs["accelerator_type"] == "A100"
    assert map_batches_kwargs["num_cpus"] == 1.0
    assert map_batches_kwargs["num_gpus"] == 1.0
    assert "resources" not in map_batches_kwargs


def test_gpu_accelerator_bundle_contents(monkeypatch):
    """Verify GPUAccelerator generates expected bundles for tensor + pipeline parallelism."""
    captured_bundles = []

    def fake_placement_group(bundles, **kwargs):
        captured_bundles.extend(bundles)
        mock_pg = MagicMock()
        mock_pg.bundle_specs = bundles
        return mock_pg

    monkeypatch.setattr(
        "ray.llm._internal.common.accelerators.placement_group",
        fake_placement_group,
    )

    backend = GPUAccelerator(config=GPUConfig())
    map_batches_kwargs = backend.build_batch_scheduling_options(
        accelerator_type="A100",
        engine_kwargs={"tensor_parallel_size": 2, "pipeline_parallel_size": 2},
        placement_group_config=None,
    )
    scheduling_strategy = map_batches_kwargs["ray_remote_args_fn"]()[
        "scheduling_strategy"
    ]
    bundle_specs = scheduling_strategy.placement_group.bundle_specs

    assert len(bundle_specs) == 4
    for bundle_spec in bundle_specs:
        assert bundle_spec["accelerator_type:A100"] == 0.001
        assert bundle_spec["CPU"] == 1.0
        assert bundle_spec["GPU"] == 1.0


def test_cpu_config_raises_not_implemented_error():
    backend = CPUAccelerator(config=CPUConfig())
    with pytest.raises(
        NotImplementedError,
        match="CPUAccelerator does not implement batch scheduling options.",
    ):
        backend.build_batch_scheduling_options(
            accelerator_type=None,
            engine_kwargs={"tensor_parallel_size": 2},
            placement_group_config=None,
        )


def test_get_accelerator_backend_raises_type_error_on_unknown():
    from ray.llm._internal.common.accelerators import get_accelerator_backend

    with pytest.raises(TypeError, match="Unsupported accelerator config"):
        get_accelerator_backend(object())  # type: ignore


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
