import sys

import pytest

import ray
from ray.llm._internal.serve.core.configs.accelerators import (
    CPUAccelerator,
    GPUAccelerator,
    TPUAccelerator,
    TPUConfig,
)
from ray.llm._internal.serve.engines.vllm.vllm_models import VLLMEngineConfig
from ray.serve.llm import LLMConfig, ModelLoadingConfig
from ray.tests.conftest import _ray_start_cluster
from ray.util.placement_group import PlacementGroup, placement_group_table


@pytest.fixture(scope="module")
def ray_tpu_cluster():
    """
    Simulates a Ray cluster with a multi-host TPU v6e-16 slice (4x4 topology).
    """
    pod_type = "v6e-16"
    topology = "4x4"

    with _ray_start_cluster() as cluster:
        # A 4x4 v6e slice has 16 chips. We simulate 4 hosts with 4 chips each.
        for i in range(4):
            env_vars = {
                "TPU_NAME": "test-slice",
                "TPU_WORKER_ID": str(i),
                "TPU_ACCELERATOR_TYPE": pod_type,
                "TPU_TOPOLOGY": topology,
            }
            labels = {
                "ray.io/tpu-slice-name": "test-slice",
                "ray.io/tpu-worker-id": str(i),
                "ray.io/tpu-pod-type": pod_type,
            }
            resources = {"TPU": 4, "accelerator_type:TPU-V6E": 4}

            # The first node is the "head" of the slice
            if i == 0:
                resources[f"TPU-{pod_type}-head"] = 1

            cluster.add_node(
                num_cpus=8,
                resources=resources,
                labels=labels,
                env_vars=env_vars,
            )

        ray.init(address=cluster.address)
        yield cluster
        ray.shutdown()


def test_tpu_slice_placement_group_creation_default_resources(ray_tpu_cluster):
    """
    Verifies that requesting a multi-host TPU topology correctly intercepts
    standard PG creation and returns a PACK SlicePlacementGroup.
    """
    llm_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(model_id="test-tpu-model"),
        accelerator_type="TPU-V6E",
        accelerator_config={"kind": "tpu", "topology": "4x4"},
        llm_engine="vLLM",
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
        llm_engine="vLLM",
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
        llm_engine="vLLM",
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
        llm_engine="vLLM",
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
    Verifies that VLLMEngineConfig correctly infers the accelerator backend
    when no explicit accelerator_config is provided.
    """
    # TPU string correctly infers TPUAccelerator
    cfg1 = VLLMEngineConfig(model_id="test", accelerator_type="TPU-V6E")
    assert isinstance(cfg1.accelerator, TPUAccelerator)

    # GPU string (like A10G) falls back to GPUAccelerator
    cfg2 = VLLMEngineConfig(model_id="test", accelerator_type="A10G")
    assert isinstance(cfg2.accelerator, GPUAccelerator)

    # No accelerator hints falls back to GPU by default
    cfg3 = VLLMEngineConfig(model_id="test")
    assert isinstance(cfg3.accelerator, GPUAccelerator)

    # Explicit CPU config correctly yields CPUAccelerator
    cfg4 = VLLMEngineConfig(model_id="test", accelerator_config={"kind": "cpu"})
    assert isinstance(cfg4.accelerator, CPUAccelerator)


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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
