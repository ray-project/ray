import sys

import pytest

import ray
from ray import serve
from ray.llm._internal.serve.core.configs.accelerators import (
    CPUAccelerator,
    CPUConfig,
    GPUAccelerator,
    GPUConfig,
    TPUAccelerator,
    TPUConfig,
)
from ray.llm._internal.serve.core.server.llm_server import LLMServer
from ray.llm.tests.serve.mocks.mock_vllm_engine import PGCreationMockEngine
from ray.serve.llm import LLMConfig, ModelLoadingConfig
from ray.util.placement_group import PlacementGroup, placement_group_table


def test_tpu_slice_placement_group_creation_default_resources(ray_tpu_cluster):
    """
    Verifies that requesting a multi-host TPU topology correctly intercepts
    standard PG creation and returns a PACK SlicePlacementGroup.
    """
    llm_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(model_id="test-tpu-model"),
        accelerator_type="TPU-V6E",
        accelerator_config={"kind": "tpu", "topology": "4x4"},
    )

    engine_config = llm_config.get_engine_config()
    pg = engine_config.get_or_create_pg()

    assert isinstance(pg, PlacementGroup)

    pg_table = placement_group_table(pg)
    assert pg_table["strategy"] == "PACK"

    # 4x4 v6e = 16 chips. We default to 1 TPU chip per bundle.
    assert len(pg_table["bundles"]) == 16
    for bundle in pg_table["bundles"].values():
        assert "TPU" in bundle
        assert bundle["TPU"] == 1

    # Let the backend tear down its own resources if it has any
    engine_config.accelerator.shutdown()
    try:
        ray.util.remove_placement_group(pg)
    except Exception:
        pass  # Already cleaned up by the wrapper


def test_tpu_slice_placement_group_creation_host_resources(ray_tpu_cluster):
    """
    Verifies that explicitly providing host-level bundles via
    placement_group_config correctly overrides the 1-chip default.
    """
    llm_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(model_id="test-tpu-model"),
        accelerator_type="TPU-V6E",
        accelerator_config={"kind": "tpu", "topology": "4x4"},
        placement_group_config={
            "strategy": "STRICT_SPREAD",
            "bundles": [{"TPU": 4}],
        },
    )

    engine_config = llm_config.get_engine_config()
    pg = engine_config.get_or_create_pg()

    assert isinstance(pg, PlacementGroup)

    pg_table = placement_group_table(pg)
    assert pg_table["strategy"] == "STRICT_SPREAD"
    # We should provision 4 host-level bundles instead of the default 16 chip-level bundles.
    assert len(pg_table["bundles"]) == 4
    for bundle in pg_table["bundles"].values():
        assert "TPU" in bundle
        assert bundle["TPU"] == 4

    # Let the backend tear down its own resources if it has any
    engine_config.accelerator.shutdown()
    try:
        ray.util.remove_placement_group(pg)
    except Exception:
        pass  # Already cleaned up by the wrapper


def test_single_tpu_fallback(ray_tpu_cluster):
    """
    Verifies that requesting a TPU without a topology gracefully
    falls back to standard single-host bundle packing.
    """
    llm_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(model_id="test-tpu-model"),
        accelerator_type="TPU-V6E",
    )

    engine_config = llm_config.get_engine_config()
    pg = engine_config.get_or_create_pg()

    pg_table = placement_group_table(pg)

    # Verify it falls back to the default PACK strategy for 1 GPU/TPU
    assert len(pg_table["bundles"]) == 1
    assert pg_table["strategy"] == "PACK"

    # Let the backend tear down its own resources if it has any
    engine_config.accelerator.shutdown()
    try:
        ray.util.remove_placement_group(pg)
    except Exception:
        pass  # Already cleaned up by the wrapper


def test_tpu_slice_placement_group_creation_bundle_per_worker(ray_tpu_cluster):
    """
    Verifies that specifying bundle_per_worker correctly expands to bundles,
    includes the accelerator hint for TPU, and correctly identifies TPU usage.
    """
    llm_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(model_id="test-tpu-model"),
        accelerator_type="TPU-V6E",
        accelerator_config={"kind": "tpu", "topology": "4x4"},
        placement_group_config={
            "bundle_per_worker": {"TPU": 1},
        },
        engine_kwargs={
            "tensor_parallel_size": 2,
        },
    )

    engine_config = llm_config.get_engine_config()

    # Validate the accelerator backend was correctly inferred
    assert isinstance(engine_config.accelerator, TPUAccelerator)

    bundles = engine_config.placement_bundles
    assert len(bundles) == 2
    for bundle in bundles:
        assert bundle["TPU"] == 1
        assert "accelerator_type:TPU-V6E" in bundle
        assert bundle["accelerator_type:TPU-V6E"] == 0.001


def test_accelerator_inference_logic():
    """
    Verifies that LLMConfig correctly infers the accelerator config
    when no explicit accelerator_config is provided, and passes it
    correctly to the engine.
    """
    # TPU string correctly infers TPUConfig and TPUAccelerator
    cfg1 = LLMConfig(
        model_loading_config={"model_id": "test"},
        accelerator_type="TPU-V6E",
    )
    assert isinstance(cfg1.accelerator_config, TPUConfig)
    assert isinstance(cfg1.get_engine_config().accelerator, TPUAccelerator)

    # GPU string falls back to GPUConfig and GPUAccelerator
    cfg2 = LLMConfig(
        model_loading_config={"model_id": "test"},
        accelerator_type="A10G",
    )
    assert isinstance(cfg2.accelerator_config, GPUConfig)
    assert isinstance(cfg2.get_engine_config().accelerator, GPUAccelerator)

    # No accelerator hints falls back to GPU by default
    cfg3 = LLMConfig(model_loading_config={"model_id": "test"})
    assert isinstance(cfg3.accelerator_config, GPUConfig)
    assert isinstance(cfg3.get_engine_config().accelerator, GPUAccelerator)

    # Explicit CPU config correctly yields CPUAccelerator
    cfg4 = LLMConfig(
        model_loading_config={"model_id": "test"},
        accelerator_config={"kind": "cpu"},
    )
    assert isinstance(cfg4.accelerator_config, CPUConfig)
    assert isinstance(cfg4.get_engine_config().accelerator, CPUAccelerator)


def test_tpu_slice_placement_group_creation_heterogeneous_tpu_bundles_fail():
    """
    Verifies that a ValueError is raised when heterogeneous TPU bundles are provided.
    """
    accelerator = TPUAccelerator(TPUConfig(kind="tpu", topology="4x4"))

    with pytest.raises(ValueError, match="Heterogeneous TPU bundles are not supported"):
        accelerator.create_placement_group(
            bundles=[{"TPU": 4}, {"TPU": 2}],
            strategy="PACK",
            name="test-pg",
            accelerator_type_str="TPU-V6E",
        )


def test_tpu_slice_placement_group_creation_cpu_driver_homogeneous_tpu_bundles_pass(
    ray_tpu_cluster,
):
    """
    Verifies that CPU-only driver bundles are ignored and do not trigger an error
    if subsequent TPU bundles are homogeneous.
    """
    accelerator = TPUAccelerator(TPUConfig(kind="tpu", topology="4x4"))

    pg = accelerator.create_placement_group(
        bundles=[{"CPU": 2}, {"TPU": 4}, {"TPU": 4}],
        strategy="PACK",
        name="test-pg",
        accelerator_type_str="TPU-V6E",
    )

    # Verify valid PG creation
    assert isinstance(pg, PlacementGroup)

    accelerator.shutdown()
    try:
        ray.util.remove_placement_group(pg)
    except Exception:
        pass


def test_tpu_serve_deployment_default_chip_level_bundles(ray_tpu_cluster):
    """
    Verifies that a Serve deployment created for a multi-host TPU slice defaults
    to chip-level bundles when no placement_group_config is specified.
    """
    llm_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(model_id="test-tpu-model"),
        accelerator_type="TPU-V6E",
        accelerator_config={"kind": "tpu", "topology": "4x4"},
    )

    app = serve.deployment(LLMServer).bind(llm_config, engine_cls=PGCreationMockEngine)
    serve.run(app)

    pg_table = ray.util.placement_group_table()
    active_pgs = list(
        {k: v for k, v in pg_table.items() if v["state"] == "CREATED"}.values()
    )

    assert (
        len(active_pgs) == 2
    ), "Expected 2 PGs - one for TPU Head, one for worker bundles"

    tpu_head_resource = "TPU-v6e-16-head"
    head_pgs = [
        pg
        for pg in active_pgs
        if len(pg["bundles"]) == 1
        and tpu_head_resource in list(pg["bundles"].values())[0]
    ]
    assert len(head_pgs) == 1

    worker_pg = [pg for pg in active_pgs if pg not in head_pgs][0]

    assert worker_pg["strategy"] == "PACK"
    # 4x4 topology = 16 chips. Default is 16 bundles of 1 TPU.
    assert len(worker_pg["bundles"]) == 16
    for bundle in worker_pg["bundles"].values():
        assert bundle.get("TPU", 0) == 1

    serve.shutdown()


def test_tpu_serve_deployment_explicit_host_level_bundles(ray_tpu_cluster):
    """
    Verifies that a user can explicitly request host-level bundles (4 TPUs per bundle)
    for a Serve deployment via placement_group_config.
    """
    llm_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(model_id="test-tpu-model"),
        accelerator_type="TPU-V6E",
        accelerator_config={"kind": "tpu", "topology": "4x4"},
        placement_group_config={"bundle_per_worker": {"TPU": 4}},
    )

    app = serve.deployment(LLMServer).bind(llm_config, engine_cls=PGCreationMockEngine)
    serve.run(app)

    pg_table = ray.util.placement_group_table()
    active_pgs = list(
        {k: v for k, v in pg_table.items() if v["state"] == "CREATED"}.values()
    )

    assert (
        len(active_pgs) == 2
    ), "Expected 2 PGs - one for TPU Head, one for worker bundles"

    tpu_head_resource = "TPU-v6e-16-head"
    head_pgs = [
        pg
        for pg in active_pgs
        if len(pg["bundles"]) == 1
        and tpu_head_resource in list(pg["bundles"].values())[0]
    ]
    assert len(head_pgs) == 1

    worker_pg = [pg for pg in active_pgs if pg not in head_pgs][0]

    assert worker_pg["strategy"] == "PACK"
    # 4x4 topology = 16 chips. With 4 TPUs per bundle, expect exactly 4 bundles.
    assert len(worker_pg["bundles"]) == 4
    for bundle in worker_pg["bundles"].values():
        assert bundle.get("TPU", 0) == 4

    serve.shutdown()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
