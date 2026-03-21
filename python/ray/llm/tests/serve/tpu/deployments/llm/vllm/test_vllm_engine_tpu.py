import sys

import pytest

import ray
from ray.llm._internal.serve.engines.vllm.vllm_engine import VLLMEngine
from ray.serve.llm import LLMConfig, ModelLoadingConfig
from ray.util.placement_group import PlacementGroup
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy


@pytest.mark.asyncio
async def test_vllm_engine_start_with_tpu_topology(
    model_smolvlm_256m,
):
    """vLLM engine starts successfully with TPU v6e 4x4 topology configuration."""
    llm_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(
            model_id="smolvlm-256m",
            model_source=model_smolvlm_256m,
        ),
        accelerator_type="TPU-V6E",
        topology="4x4",
        engine_kwargs=dict(
            use_tqdm_on_load=False,
            enforce_eager=True,
            max_model_len=2048,
        ),
        runtime_env=dict(
            env_vars={
                "VLLM_DISABLE_COMPILE_CACHE": "1",
            },
        ),
    )

    engine_config = llm_config.get_engine_config()
    pg = engine_config.get_or_create_pg()

    strategy = PlacementGroupSchedulingStrategy(
        pg, placement_group_capture_child_tasks=True, placement_group_bundle_index=0
    )

    @ray.remote(num_cpus=1, scheduling_strategy=strategy)
    class Actor:
        def __init__(self):
            self.engine = VLLMEngine(llm_config)

        async def start(self):
            await self.engine.start()

        async def check_health(self):
            await self.engine.check_health()

        async def shutdown(self):
            self.engine.shutdown()

    actor = Actor.remote()
    await actor.start.remote()
    await actor.check_health.remote()
    await actor.shutdown.remote()

    ray.util.remove_placement_group(pg)


def test_tpu_slice_placement_group_creation_default_resources():
    """
    Verifies that requesting a multi-host TPU topology correctly intercepts
    standard PG creation and returns a PACK SlicePlacementGroup.
    """
    llm_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(model_id="test-tpu-model"),
        accelerator_type="TPU-V6E",
        topology="4x4",
        llm_engine="vLLM",
    )

    engine_config = llm_config.get_engine_config()
    pg = engine_config.get_or_create_pg()

    assert isinstance(pg, PlacementGroup)

    assert pg.strategy == "PACK"

    # 4x4 v6e = 16 chips. We default to 1 TPU chip per bundle.
    assert pg.bundle_count == 16
    for bundle in pg.bundle_specs:
        assert "TPU" in bundle
        assert bundle["TPU"] == 1

    ray.util.remove_placement_group(pg)


def test_tpu_slice_placement_group_creation_host_resources():
    """
    Verifies that explicitly providing host-level bundles via
    placement_group_config correctly overrides the 1-chip default.
    """
    llm_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(model_id="test-tpu-model"),
        accelerator_type="TPU-V6E",
        topology="4x4",
        placement_group_config={
            "strategy": "STRICT_SPREAD",
            "bundles": [{"TPU": 4}],
        },
        llm_engine="vLLM",
    )

    engine_config = llm_config.get_engine_config()
    pg = engine_config.get_or_create_pg()

    assert isinstance(pg, PlacementGroup)

    assert pg.strategy == "STRICT_SPREAD"
    # We should provision 4 host-level bundles instead of the default 16 chip-level bundles.
    assert pg.bundle_count == 4
    for bundle in pg.bundle_specs:
        assert "TPU" in bundle
        assert bundle["TPU"] == 4

    ray.util.remove_placement_group(pg)


def test_single_tpu_fallback():
    """
    Verifies that requesting a TPU without a topology gracefully
    falls back to standard single-host bundle packing.
    """
    llm_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(model_id="test-tpu-model"),
        accelerator_type="TPU-V6E",
        llm_engine="vLLM",
    )

    engine_config = llm_config.get_engine_config()
    pg = engine_config.get_or_create_pg()

    # It should fall back to the default PACK strategy for 1 GPU/TPU
    assert pg.bundle_count == 1
    assert pg.strategy == "PACK"

    ray.util.remove_placement_group(pg)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
