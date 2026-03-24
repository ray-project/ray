import sys

import pytest

import ray
from ray.serve.llm import LLMConfig, ModelLoadingConfig
from ray.util.placement_group import PlacementGroup


def test_tpu_slice_placement_group_creation_default_resources(ray_start_regular_shared):
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

    if engine_config._tpu_slice_pg_wrapper:
        engine_config._tpu_slice_pg_wrapper.shutdown()
    else:
        ray.util.remove_placement_group(pg)


def test_tpu_slice_placement_group_creation_host_resources(ray_start_regular_shared):
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

    if engine_config._tpu_slice_pg_wrapper:
        engine_config._tpu_slice_pg_wrapper.shutdown()
    else:
        ray.util.remove_placement_group(pg)


def test_single_tpu_fallback(ray_start_regular_shared):
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

    if engine_config._tpu_slice_pg_wrapper:
        engine_config._tpu_slice_pg_wrapper.shutdown()
    else:
        ray.util.remove_placement_group(pg)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
