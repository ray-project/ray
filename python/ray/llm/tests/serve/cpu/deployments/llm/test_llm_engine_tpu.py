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
from ray.serve._private.utils import resolve_tpu_slice_kwargs
from ray.serve.llm import LLMConfig, ModelLoadingConfig


def test_tpu_serve_deployment_single_tpu_fallback(ray_tpu_cluster):
    """
    Verifies that requesting a TPU without a topology gracefully
    falls back to standard single-host bundle packing without
    triggering the slice placement group interception.
    """
    llm_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(model_id="test-tpu-model"),
        accelerator_type="TPU-V6E",
        # Explicitly omit topology
        accelerator_config={"kind": "tpu"},
        engine_kwargs={"tensor_parallel_size": 1},
    )

    app = serve.deployment(LLMServer).bind(llm_config, engine_cls=PGCreationMockEngine)

    serve.start(http_options={"port": 0})
    try:
        serve.run(app, name="single_tpu_app", route_prefix="/single_tpu")
        pg_table = ray.util.placement_group_table()
        active_pgs = list(
            {k: v for k, v in pg_table.items() if v["state"] == "CREATED"}.values()
        )

        # Ensure we only have standard PGs, no TPU Head PGs
        tpu_head_resource = "TPU-v6e-16-head"
        head_pgs = [
            pg
            for pg in active_pgs
            if len(pg["bundles"]) == 1
            and tpu_head_resource in list(pg["bundles"].values())[0]
        ]
        assert len(head_pgs) == 0

        # Verify the deployment PG has the default PACK strategy and 1 TPU
        deployment_pgs = [
            pg
            for pg in active_pgs
            if any("TPU" in bundle for bundle in pg["bundles"].values())
        ]
        assert len(deployment_pgs) >= 1

        target_pg = deployment_pgs[0]
        assert target_pg["strategy"] == "PACK"

        # Verify it allocated 1 TPU per bundle
        for bundle in target_pg["bundles"].values():
            if "TPU" in bundle:
                assert bundle["TPU"] == 1

    finally:
        serve.shutdown()


def test_tpu_accelerator_remote_options_scheduling(ray_tpu_cluster):
    """
    Verifies that TPUAccelerator.get_remote_options returns a label_selector,
    and successfully schedules a task without causing Ray Core validation errors.
    """
    llm_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(model_id="test-tpu-model"),
        accelerator_type="TPU-V6E",
        accelerator_config={"kind": "tpu"},
    )
    engine_config = llm_config.get_engine_config()

    options = engine_config.accelerator.get_remote_options("TPU-V6E")

    # Ensure it returns the expected options (i.e. label_selector only)
    assert options == {
        "resources": {},
        "label_selector": {"ray.io/accelerator-type": "TPU-V6E"},
    }

    @ray.remote(**options)
    def probe_metadata():
        return True

    assert ray.get(probe_metadata.remote()) is True


def test_accelerator_inference_logic():
    """
    Verifies that LLMConfig correctly infers the accelerator config
    when no explicit accelerator_config is provided, and passes it
    correctly to the engine.
    """
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


def test_tpu_deployment_options_bundle_selector_injection():
    """
    Verifies that LLMServer.get_deployment_options correctly injects TPU topology
    labels into the placement group bundle selectors and creates the expected bundles.
    """
    llm_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(model_id="test-tpu-model"),
        accelerator_type="TPU-V6E",
        accelerator_config={"kind": "tpu", "topology": "4x4"},
        engine_kwargs={"tensor_parallel_size": 16},
    )

    options = LLMServer.get_deployment_options(llm_config)

    # Ensure PG creation is no longer deferred
    assert "placement_group_bundles" in options
    assert "placement_group_bundle_label_selector" in options

    pg_bundles = options["placement_group_bundles"]
    selectors = options["placement_group_bundle_label_selector"]

    # 4x4 topology = 16 chips
    # Default is 4 bundles of 4 TPUs
    assert len(pg_bundles) == 4
    assert len(selectors) == 4

    # The first bundle should contain the TPU allocation and the replica actor's CPU
    assert pg_bundles[0].get("TPU") == 4
    assert pg_bundles[0].get("CPU", 0) >= 1

    # The remaining worker bundles should strictly be for the TPU host
    for i in range(1, 4):
        assert pg_bundles[i].get("TPU") == 4
        assert pg_bundles[i].get("CPU", 0) == 0

    for selector in selectors:
        assert selector["ray.io/tpu-topology"] == "4x4"
        assert selector["ray.io/accelerator-type"] == "TPU-V6E"


def test_tpu_slice_kwargs_ignores_cpu_driver_bundle():
    """
    Verifies that resolve_tpu_slice_kwargs correctly ignores the merged CPU
    resources from the replica actor in the first bundle, and extracts the
    TPU requirement from the remaining bundles.
    """
    labels = [{"ray.io/tpu-topology": "4x4", "ray.io/accelerator-type": "TPU-V6E"}]
    bundles = [{"TPU": 4, "CPU": 1}, {"TPU": 4}, {"TPU": 4}, {"TPU": 4}]

    topology, version, worker_bundle = resolve_tpu_slice_kwargs(labels, bundles)

    assert topology == "4x4"
    assert version == "v6e"
    assert worker_bundle == {"TPU": 4}
    assert "CPU" not in worker_bundle


def test_tpu_slice_kwargs_rejects_heterogeneous_bundles():
    """
    Verifies that a ValueError is raised when heterogeneous TPU bundles are provided
    to the Serve gang-scheduler helper.
    """
    labels = [{"ray.io/tpu-topology": "4x4", "ray.io/accelerator-type": "TPU-V6E"}]
    bundles = [{"TPU": 4}, {"TPU": 2}]

    with pytest.raises(ValueError, match="Heterogeneous TPU bundles are not supported"):
        resolve_tpu_slice_kwargs(labels, bundles)


def test_tpu_serve_deployment_default_host_level_bundles(ray_tpu_cluster):
    """
    Verifies that a Serve deployment created for a multi-host TPU slice intercepts
    the default creation and provisions the SlicePlacementGroup correctly.
    """
    llm_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(model_id="test-tpu-model"),
        accelerator_type="TPU-V6E",
        accelerator_config={"kind": "tpu", "topology": "4x4"},
        engine_kwargs={"tensor_parallel_size": 16},
    )

    app = serve.deployment(LLMServer).bind(llm_config, engine_cls=PGCreationMockEngine)

    serve.start(http_options={"port": 0})
    try:
        serve.run(app, name="default_host_app", route_prefix="/default_host")
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

        # 4x4 topology = 16 chips. Default is 4 bundles of 4 TPUs.
        assert len(worker_pg["bundles"]) == 4
        for bundle in worker_pg["bundles"].values():
            assert bundle.get("TPU", 0) == 4
    finally:
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
        engine_kwargs={"tensor_parallel_size": 4},
    )

    app = serve.deployment(LLMServer).bind(llm_config, engine_cls=PGCreationMockEngine)

    serve.start(http_options={"port": 0})
    try:
        serve.run(app, name="explicit_host_app", route_prefix="/explicit_host")
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
        assert len(worker_pg["bundles"]) == 4
        for bundle in worker_pg["bundles"].values():
            assert bundle.get("TPU", 0) == 4
    finally:
        serve.shutdown()


def test_tpu_slice_kwargs_rejects_missing_accelerator_type():
    """
    Verifies that a ValueError is raised when a TPU topology is requested
    but the accelerator type label is missing from the bundle labels.
    """
    labels = [{"ray.io/tpu-topology": "4x4"}]
    bundles = [{"TPU": 4}]

    with pytest.raises(
        ValueError,
        match="A TPU topology was requested, but 'ray.io/accelerator-type' was not found",
    ):
        resolve_tpu_slice_kwargs(labels, bundles)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
