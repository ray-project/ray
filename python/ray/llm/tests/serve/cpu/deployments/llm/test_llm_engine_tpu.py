import asyncio
import copy
import logging
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import ray
from ray import serve
from ray.llm._internal.serve.core.configs import accelerators as accelerators_mod
from ray.llm._internal.serve.core.configs.accelerators import (
    CPUAccelerator,
    CPUConfig,
    GPUAccelerator,
    GPUConfig,
    TPUAccelerator,
    TPUConfig,
    _resolve_bundle_label_selector,
    prepare_tpu_bundles_and_label_selector,
)
from ray.llm._internal.serve.core.server.llm_server import LLMServer
from ray.llm._internal.serve.serving_patterns.data_parallel.dp_server import DPServer
from ray.llm._internal.serve.utils.node_initialization_utils import initialize_node
from ray.llm.tests.serve.mocks.mock_vllm_engine import PGCreationMockEngine
from ray.serve._private.config import ReplicaConfig
from ray.serve.llm import LLMConfig, ModelLoadingConfig
from ray.serve.schema import DeploymentSchema
from ray.util.placement_group import PlacementGroup, placement_group_table


def test_tpu_slice_placement_group_creation_default_resources(ray_tpu_cluster):
    """
    Verifies that requesting a multi-host TPU topology correctly intercepts
    standard PG creation and returns a PACK SlicePlacementGroup.
    """
    llm_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(model_id="test-tpu-model"),
        accelerator_type="TPU-V6E",
        accelerator_config=TPUConfig(kind="tpu", topology="4x4"),
        engine_kwargs={"tensor_parallel_size": 16},
    )

    engine_config = llm_config.get_engine_config()

    pg = None
    try:
        pg = engine_config.get_or_create_pg()

        assert isinstance(pg, PlacementGroup)

        pg_table = placement_group_table(pg)
        assert pg_table["strategy"] == "PACK"

        # 4x4 v6e = 16 chips. We default to 4 TPU chips per bundle (per-host).
        assert len(pg_table["bundles"]) == 4
        for bundle in pg_table["bundles"].values():
            assert "TPU" in bundle
            assert bundle["TPU"] == 4.0
    finally:
        # Let the backend tear down its own resources if it has any
        engine_config.accelerator.shutdown()
        if pg is not None:
            try:
                ray.util.remove_placement_group(pg)
            except Exception:
                pass


def test_tpu_slice_placement_group_creation_host_resources(
    ray_tpu_cluster, caplog, monkeypatch
):
    """
    Verifies that explicitly providing host-level bundles via
    placement_group_config configures 4 host bundles and warns when ignoring a
    custom placement strategy on the SlicePlacementGroup path.
    """
    monkeypatch.setattr(logging.getLogger("ray"), "propagate", True)
    monkeypatch.setattr(logging.getLogger("ray.serve"), "propagate", True)
    llm_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(model_id="test-tpu-model"),
        accelerator_type="TPU-V6E",
        accelerator_config=TPUConfig(kind="tpu", topology="4x4"),
        placement_group_config={
            "strategy": "STRICT_SPREAD",
            "bundles": [{"TPU": 4}] * 4,
        },
    )

    engine_config = llm_config.get_engine_config()

    pg = None
    try:
        with caplog.at_level(logging.WARNING):
            pg = engine_config.get_or_create_pg()

        assert isinstance(pg, PlacementGroup)
        assert "Ignoring placement group strategy 'STRICT_SPREAD'" in caplog.text

        pg_table = placement_group_table(pg)
        assert pg_table["strategy"] == "PACK"
        assert len(pg_table["bundles"]) == 4
        for bundle in pg_table["bundles"].values():
            assert "TPU" in bundle
            assert bundle["TPU"] == 4
    finally:
        # Let the backend tear down its own resources if it has any
        engine_config.accelerator.shutdown()
        if pg is not None:
            try:
                ray.util.remove_placement_group(pg)
            except Exception:
                pass


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

    serve_options = LLMServer.get_deployment_options(llm_config)
    assert serve_options["placement_group_bundles"] == [{"CPU": 1, "GPU": 0, "TPU": 1}]
    assert serve_options["placement_group_bundle_label_selector"] == [
        {"ray.io/accelerator-type": "TPU-V6E"}
    ]

    # Let the backend tear down its own resources if it has any
    engine_config.accelerator.shutdown()
    try:
        ray.util.remove_placement_group(pg)
    except Exception:
        pass  # Already cleaned up by the wrapper


def test_tpu_label_selector_merge_and_bundle_extraction(ray_tpu_cluster):
    """Verify multi-bundle selector broadcast/merging and bundle constraint extraction."""
    # Single-element list selector broadcasts across both TPU bundles and merges with pin.
    llm_config_broadcast = LLMConfig(
        model_loading_config=ModelLoadingConfig(model_id="test-tpu-model"),
        accelerator_type="TPU-V6E",
        engine_kwargs={"tensor_parallel_size": 2},
        deployment_config={
            "placement_group_bundle_label_selector": [{"ray.io/market-type": "spot"}]
        },
    )
    broadcast_options = LLMServer.get_deployment_options(llm_config_broadcast)
    assert broadcast_options["placement_group_bundle_label_selector"] == [
        {"ray.io/accelerator-type": "TPU-V6E", "ray.io/market-type": "spot"},
        {"ray.io/accelerator-type": "TPU-V6E", "ray.io/market-type": "spot"},
    ]

    # Single-element tuple selector also broadcasts and retains the TPU generation pin.
    llm_config_tuple_broadcast = LLMConfig(
        model_loading_config=ModelLoadingConfig(model_id="test-tpu-model"),
        accelerator_type="TPU-V6E",
        engine_kwargs={"tensor_parallel_size": 2},
        deployment_config={
            "placement_group_bundle_label_selector": ({"ray.io/market-type": "spot"},)
        },
    )
    tuple_options = LLMServer.get_deployment_options(llm_config_tuple_broadcast)
    assert tuple_options["placement_group_bundle_label_selector"] == [
        {"ray.io/accelerator-type": "TPU-V6E", "ray.io/market-type": "spot"},
        {"ray.io/accelerator-type": "TPU-V6E", "ray.io/market-type": "spot"},
    ]

    # Bundle-level accelerator_type:* constraints are preserved as label selectors when
    # top-level accelerator_type_str is None.
    assert prepare_tpu_bundles_and_label_selector(
        [{"CPU": 1, "TPU": 1, "accelerator_type:TPU-V6E": 0.001}],
        accelerator_type_str=None,
    ) == ([{"CPU": 1, "TPU": 1}], [{"ray.io/accelerator-type": "TPU-V6E"}])


def test_initialize_node_awaits_placement_group_ready(monkeypatch):
    """initialize_node awaits ctx.placement_group.ready() before reading bundles_to_node_id."""
    events = []

    async def _ready():
        events.append("ready")

    fake_pg = SimpleNamespace(ready=_ready)
    fake_ctx = SimpleNamespace(placement_group=fake_pg, runtime_env={})
    fake_engine_cfg = SimpleNamespace(hf_model_id=None)
    fake_llm_cfg = SimpleNamespace(
        get_or_create_callback=lambda: SimpleNamespace(ctx=fake_ctx),
        get_engine_config=lambda: fake_engine_cfg,
    )

    def _fake_pg_table(pg):
        assert pg is fake_pg
        events.append("table")
        return {"bundles_to_node_id": {0: "1" * 56, 1: "2" * 56}}

    async def _remote_result():
        return "/tmp/downloaded-model"

    fake_remote_fn = MagicMock()
    fake_remote_fn.options.return_value = fake_remote_fn
    fake_remote_fn.remote.side_effect = lambda _cfg: _remote_result()

    monkeypatch.setattr(ray.util, "placement_group_table", _fake_pg_table)
    monkeypatch.setattr(ray, "remote", lambda _fn: fake_remote_fn)

    asyncio.run(initialize_node(fake_llm_cfg))

    assert events == ["ready", "table"]
    assert fake_engine_cfg.hf_model_id == "/tmp/downloaded-model"


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


def test_tpu_serve_deployment_default_host_level_bundles(ray_tpu_cluster):
    """
    Verifies that a Serve deployment created for a multi-host TPU slice defaults
    to host-level bundles when no placement_group_config is specified.
    """
    llm_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(model_id="test-tpu-model"),
        accelerator_type="TPU-V6E",
        accelerator_config=TPUConfig(kind="tpu", topology="4x4"),
        engine_kwargs={"tensor_parallel_size": 16},
    )

    app = serve.deployment(LLMServer).bind(llm_config, engine_cls=PGCreationMockEngine)
    serve.run(app)

    pg_table = ray.util.placement_group_table()
    active_pgs = list(
        {k: v for k, v in pg_table.items() if v["state"] == "CREATED"}.values()
    )

    assert len(active_pgs) == 1, "Expected 1 topology-aware PG for the TPU slice"
    worker_pg = active_pgs[0]

    assert worker_pg["strategy"] == "PACK"
    # 4x4 topology = 16 chips. Default is 4 bundles of 4 TPUs (per-host).
    assert len(worker_pg["bundles"]) == 4
    for bundle in worker_pg["bundles"].values():
        assert bundle.get("TPU", 0) == 4
        assert "accelerator_type:TPU-V6E" not in bundle

    serve.shutdown()


def test_tpu_serve_deployment_explicit_host_level_bundles(ray_tpu_cluster):
    """
    Verifies that a user can explicitly request host-level bundles (4 TPUs per bundle)
    for a Serve deployment via placement_group_config.
    """
    llm_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(model_id="test-tpu-model"),
        accelerator_type="TPU-V6E",
        accelerator_config=TPUConfig(kind="tpu", topology="4x4"),
        placement_group_config={"bundle_per_worker": {"TPU": 4}},
        engine_kwargs={"tensor_parallel_size": 16},
    )

    app = serve.deployment(LLMServer).bind(llm_config, engine_cls=PGCreationMockEngine)
    serve.run(app)

    pg_table = ray.util.placement_group_table()
    active_pgs = list(
        {k: v for k, v in pg_table.items() if v["state"] == "CREATED"}.values()
    )

    assert len(active_pgs) == 1, "Expected 1 topology-aware PG for the TPU slice"
    worker_pg = active_pgs[0]

    assert worker_pg["strategy"] == "PACK"
    # 4x4 topology = 16 chips. With 4 TPUs per bundle, expect exactly 4 bundles.
    assert len(worker_pg["bundles"]) == 4
    for bundle in worker_pg["bundles"].values():
        assert bundle.get("TPU", 0) == 4
        assert "accelerator_type:TPU-V6E" not in bundle

    serve.shutdown()


def test_tpu_serve_deployment_explicit_per_chip_bundles(ray_tpu_cluster):
    """
    Verifies that a user can explicitly request chip-level bundles (1 TPU per bundle)
    for a full multi-host TPU slice via placement_group_config.
    """
    llm_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(model_id="test-tpu-model"),
        accelerator_type="TPU-V6E",
        accelerator_config=TPUConfig(kind="tpu", topology="4x4"),
        placement_group_config={"bundle_per_worker": {"TPU": 1}},
        engine_kwargs={"tensor_parallel_size": 16},
    )

    app = serve.deployment(LLMServer).bind(llm_config, engine_cls=PGCreationMockEngine)
    serve.run(app)

    pg_table = ray.util.placement_group_table()
    active_pgs = list(
        {k: v for k, v in pg_table.items() if v["state"] == "CREATED"}.values()
    )

    assert len(active_pgs) == 1, "Expected 1 topology-aware PG for the TPU slice"
    worker_pg = active_pgs[0]

    assert worker_pg["strategy"] == "PACK"
    # 4x4 topology = 16 chips. Explicitly requested 16 bundles of 1 TPU.
    assert len(worker_pg["bundles"]) == 16
    for bundle in worker_pg["bundles"].values():
        assert bundle.get("TPU", 0) == 1.0
        assert "accelerator_type:TPU-V6E" not in bundle

    serve.shutdown()


_REPLICA_CONFIG_KEYS = (
    "ray_actor_options",
    "placement_group_bundles",
    "placement_group_strategy",
    "placement_group_bundle_label_selector",
    "max_replicas_per_node",
)


def _validate_serve_deployment_options(
    opts: dict,
    name: str = "test-tpu",
    server_cls=LLMServer,
) -> None:
    """Validate deployment options against DeploymentSchema, ReplicaConfig, and serve.deployment."""
    DeploymentSchema(name=name, **copy.deepcopy(opts))
    replica_kwargs = {
        k: copy.deepcopy(v) for k, v in opts.items() if k in _REPLICA_CONFIG_KEYS
    }
    replica_config = ReplicaConfig.create(server_cls, **replica_kwargs)
    roundtripped = ReplicaConfig.from_proto(replica_config.to_proto())
    assert (
        roundtripped.placement_group_bundle_label_selector
        == replica_config.placement_group_bundle_label_selector
    )
    serve.deployment(server_cls, **copy.deepcopy(opts))
    serve.deployment(server_cls).options(**copy.deepcopy(opts))


@pytest.mark.parametrize(
    "selector,expected_normalized",
    [
        ([{"ray.io/market-type": "spot"}], [{"ray.io/market-type": "spot"}]),
        (({"ray.io/market-type": "spot"},), [{"ray.io/market-type": "spot"}]),
        ([{"pool": "a"}] * 4, [{"pool": "a"}] * 4),
        ([], None),
        ((), None),
        (None, None),
    ],
)
def test_tpu_deferred_pg_strips_bundle_label_selector_for_serve_validation(
    selector, expected_normalized
):
    """Deferred TPU PG strips placement_group_bundle_label_selector from Serve options without mutating LLMConfig."""
    llm_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(model_id="test-tpu-model"),
        accelerator_type="TPU-V6E",
        accelerator_config=TPUConfig(kind="tpu", topology="4x4"),
        engine_kwargs={"tensor_parallel_size": 16},
        deployment_config={"placement_group_bundle_label_selector": selector},
    )

    opts = LLMServer.get_deployment_options(llm_config)
    assert "placement_group_bundles" not in opts
    assert "placement_group_bundle_label_selector" not in opts

    _validate_serve_deployment_options(opts, name="test-tpu-deferred")

    # LLMConfig.deployment_config is not mutated and repeated calls are idempotent.
    assert (
        llm_config.deployment_config["placement_group_bundle_label_selector"]
        == selector
    )
    assert LLMServer.get_deployment_options(llm_config) == opts
    assert (
        llm_config.get_engine_config().placement_group_bundle_label_selector
        == expected_normalized
    )


def test_tpu_deferred_pg_forwards_bundle_label_selector_to_slice_pg(monkeypatch):
    """Deferred TPU PG forwards normalized bundle_label_selector through VLLMEngineConfig into SlicePlacementGroup."""
    pg_calls = []

    def _fake_placement_group(*args, **kwargs):
        pg_calls.append(kwargs)
        return MagicMock(spec=PlacementGroup, name=f"pg_{len(pg_calls) - 1}")

    monkeypatch.setattr("ray._private.client_mode_hook.auto_init_ray", lambda: None)
    monkeypatch.setattr("ray.util.tpu.placement_group", _fake_placement_group)
    monkeypatch.setattr(
        "ray.llm._internal.serve.engines.vllm.vllm_models.get_current_placement_group",
        lambda: None,
    )
    monkeypatch.setattr(
        "ray.llm._internal.serve.engines.vllm.vllm_models.placement_group_table",
        lambda _pg: {},
    )

    # 1-element list and 1-element tuple broadcast across all 4 host bundles.
    for selector in (
        [{"ray.io/market-type": "spot"}],
        ({"ray.io/market-type": "spot"},),
    ):
        pg_calls.clear()
        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test-tpu-model"),
            accelerator_type="TPU-V6E",
            accelerator_config=TPUConfig(kind="tpu", topology="4x4"),
            engine_kwargs={"tensor_parallel_size": 16},
            deployment_config={"placement_group_bundle_label_selector": selector},
        )
        engine_config = llm_config.get_engine_config()
        engine_config.get_or_create_pg()
        assert len(pg_calls) == 1
        assert pg_calls[0]["bundle_label_selector"] == [
            {
                "ray.io/tpu-topology": "4x4",
                "ray.io/accelerator-type": "TPU-V6E",
                "ray.io/tpu-worker-id": str(i),
                "ray.io/market-type": "spot",
            }
            for i in range(4)
        ]

    # User override precedence on ray.io/accelerator-type.
    pg_calls.clear()
    override_cfg = LLMConfig(
        model_loading_config=ModelLoadingConfig(model_id="test-tpu-model"),
        accelerator_type="TPU-V6E",
        accelerator_config=TPUConfig(kind="tpu", topology="4x4"),
        engine_kwargs={"tensor_parallel_size": 16},
        deployment_config={
            "placement_group_bundle_label_selector": [
                {"ray.io/accelerator-type": "TPU-V5P"}
            ]
        },
    )
    override_cfg.get_engine_config().get_or_create_pg()
    assert [
        d["ray.io/accelerator-type"] for d in pg_calls[0]["bundle_label_selector"]
    ] == ["TPU-V5P"] * 4

    # N-element list and tuple matching SlicePlacementGroup bundle count (4).
    per_bundle = [{"tier": f"t{i}"} for i in range(4)]
    for selector in (per_bundle, tuple(per_bundle)):
        pg_calls.clear()
        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test-tpu-model"),
            accelerator_type="TPU-V6E",
            accelerator_config=TPUConfig(kind="tpu", topology="4x4"),
            engine_kwargs={"tensor_parallel_size": 16},
            deployment_config={"placement_group_bundle_label_selector": selector},
        )
        llm_config.get_engine_config().get_or_create_pg()
        assert [d["tier"] for d in pg_calls[0]["bundle_label_selector"]] == [
            "t0",
            "t1",
            "t2",
            "t3",
        ]

    # Topology bundle count (16) differs from len(placement_bundles) (2):
    # 16-element selector succeeds while 2-element selector fails in SlicePlacementGroup.
    pg_calls.clear()
    per_chip_16_cfg = LLMConfig(
        model_loading_config=ModelLoadingConfig(model_id="test-tpu-model"),
        accelerator_type="TPU-V6E",
        accelerator_config=TPUConfig(kind="tpu", topology="4x4"),
        placement_group_config={"bundle_per_worker": {"TPU": 1}},
        engine_kwargs={"tensor_parallel_size": 2},
        deployment_config={
            "placement_group_bundle_label_selector": [
                {"chip": str(i)} for i in range(16)
            ]
        },
    )
    engine_16 = per_chip_16_cfg.get_engine_config()
    assert len(engine_16.placement_bundles) == 2
    engine_16.get_or_create_pg()
    assert len(pg_calls[0]["bundle_label_selector"]) == 16

    per_chip_2_cfg = LLMConfig(
        model_loading_config=ModelLoadingConfig(model_id="test-tpu-model"),
        accelerator_type="TPU-V6E",
        accelerator_config=TPUConfig(kind="tpu", topology="4x4"),
        placement_group_config={"bundle_per_worker": {"TPU": 1}},
        engine_kwargs={"tensor_parallel_size": 2},
        deployment_config={
            "placement_group_bundle_label_selector": [{"chip": "0"}, {"chip": "1"}]
        },
    )
    with pytest.raises(ValueError, match="bundle_label_selector length"):
        per_chip_2_cfg.get_engine_config().get_or_create_pg()

    # CPU driver + homogeneous TPU bundles with 4-element selector succeeds.
    pg_calls.clear()
    acc = TPUAccelerator(TPUConfig(kind="tpu", topology="4x4"))
    acc.create_placement_group(
        bundles=[{"CPU": 2}, {"TPU": 4}, {"TPU": 4}],
        strategy="PACK",
        name="test-pg",
        accelerator_type_str="TPU-V6E",
        bundle_label_selector=[{"tier": f"t{i}"} for i in range(4)],
    )
    assert len(pg_calls[0]["bundle_label_selector"]) == 4

    # Invalid selector shapes raise ValueError in VLLMEngineConfig and do NOT shut down
    # an existing _slice_pg_wrapper in TPUAccelerator.create_placement_group.
    for bad_selector in ({"a": "1"}, {}, "", "bad", [123], [None], [{"a": 123}]):
        bad_cfg = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test-tpu-model"),
            accelerator_type="TPU-V6E",
            accelerator_config=TPUConfig(kind="tpu", topology="4x4"),
            engine_kwargs={"tensor_parallel_size": 16},
            deployment_config={"placement_group_bundle_label_selector": bad_selector},
        )
        with pytest.raises(ValueError):
            bad_cfg.get_engine_config()

    existing_wrapper = MagicMock()
    acc_existing = TPUAccelerator(TPUConfig(kind="tpu", topology="4x4"))
    acc_existing._slice_pg_wrapper = existing_wrapper
    with pytest.raises(ValueError, match="list or tuple"):
        acc_existing.create_placement_group(
            bundles=[{"TPU": 4}] * 4,
            strategy="PACK",
            name="test-pg",
            accelerator_type_str="TPU-V6E",
            bundle_label_selector={"a": "1"},
        )
    existing_wrapper.shutdown.assert_not_called()
    assert acc_existing._slice_pg_wrapper is existing_wrapper


def test_tpu_deferred_pg_serve_run_with_bundle_label_selector(ray_tpu_cluster):
    """End-to-end serve.run using LLMServer.get_deployment_options with deferred TPU bundle_label_selector."""
    llm_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(model_id="test-tpu-model"),
        accelerator_type="TPU-V6E",
        accelerator_config=TPUConfig(kind="tpu", topology="4x4"),
        engine_kwargs={"tensor_parallel_size": 16},
        deployment_config={
            "placement_group_bundle_label_selector": [{"ray.io/tpu-pod-type": "v6e-16"}]
        },
    )
    opts = LLMServer.get_deployment_options(llm_config)
    assert "placement_group_bundles" not in opts
    assert "placement_group_bundle_label_selector" not in opts

    app = (
        serve.deployment(LLMServer)
        .options(**opts)
        .bind(llm_config, engine_cls=PGCreationMockEngine)
    )
    try:
        serve.run(app)
        pg_table = ray.util.placement_group_table()
        active_pgs = [v for v in pg_table.values() if v["state"] == "CREATED"]
        assert len(active_pgs) == 1
        assert len(active_pgs[0]["bundles"]) == 4
    finally:
        serve.shutdown()


def test_tpu_non_topology_create_placement_group_bundle_label_selector(monkeypatch):
    """Non-topology TPUAccelerator.create_placement_group normalizes, broadcasts, and merges selectors."""
    captured = []

    def _fake_pg(**kwargs):
        captured.append(kwargs)
        return MagicMock(spec=PlacementGroup)

    monkeypatch.setattr(accelerators_mod, "placement_group", _fake_pg)
    acc = TPUAccelerator(TPUConfig(kind="tpu", topology=None))
    two_bundles = [{"TPU": 1}, {"TPU": 1}]

    # None, [], () -> default accelerator selector only.
    for empty_sel in (None, [], ()):
        captured.clear()
        acc.create_placement_group(
            bundles=two_bundles,
            strategy="PACK",
            name="pg",
            accelerator_type_str="TPU-V6E",
            bundle_label_selector=empty_sel,
        )
        assert captured[0]["bundle_label_selector"] == [
            {"ray.io/accelerator-type": "TPU-V6E"},
            {"ray.io/accelerator-type": "TPU-V6E"},
        ]

    # 1-element list/tuple broadcasts to distinct dicts and does not alias caller dict.
    caller_sel = {"pool": "a"}
    for single_sel in ([caller_sel], (caller_sel,)):
        captured.clear()
        acc.create_placement_group(
            bundles=two_bundles,
            strategy="PACK",
            name="pg",
            accelerator_type_str="TPU-V6E",
            bundle_label_selector=single_sel,
        )
        out = captured[0]["bundle_label_selector"]
        assert out == [
            {"ray.io/accelerator-type": "TPU-V6E", "pool": "a"},
            {"ray.io/accelerator-type": "TPU-V6E", "pool": "a"},
        ]
        assert out[0] is not out[1]
        caller_sel["pool"] = "mutated"
        assert out[0]["pool"] == "a"
        caller_sel["pool"] = "a"

    # 2-element list/tuple merges element-wise.
    for pair_sel in ([{"pool": "a"}, {"pool": "b"}], ({"pool": "a"}, {"pool": "b"})):
        captured.clear()
        acc.create_placement_group(
            bundles=two_bundles,
            strategy="PACK",
            name="pg",
            accelerator_type_str="TPU-V6E",
            bundle_label_selector=pair_sel,
        )
        assert captured[0]["bundle_label_selector"] == [
            {"ray.io/accelerator-type": "TPU-V6E", "pool": "a"},
            {"ray.io/accelerator-type": "TPU-V6E", "pool": "b"},
        ]

    # User override takes precedence over TPU-V6E.
    captured.clear()
    acc.create_placement_group(
        bundles=two_bundles,
        strategy="PACK",
        name="pg",
        accelerator_type_str="TPU-V6E",
        bundle_label_selector=[{"ray.io/accelerator-type": "TPU-V5P"}],
    )
    assert captured[0]["bundle_label_selector"] == [
        {"ray.io/accelerator-type": "TPU-V5P"},
        {"ray.io/accelerator-type": "TPU-V5P"},
    ]

    # accelerator_type_str=None still broadcasts user selector.
    captured.clear()
    acc.create_placement_group(
        bundles=two_bundles,
        strategy="PACK",
        name="pg",
        accelerator_type_str=None,
        bundle_label_selector=[{"pool": "a"}],
    )
    assert captured[0]["bundle_label_selector"] == [{"pool": "a"}, {"pool": "a"}]

    # Mismatched length raises ValueError.
    with pytest.raises(ValueError, match="bundle_label_selector length"):
        acc.create_placement_group(
            bundles=[{"TPU": 1}, {"TPU": 1}, {"TPU": 1}],
            strategy="PACK",
            name="pg",
            accelerator_type_str="TPU-V6E",
            bundle_label_selector=[{"pool": "a"}, {"pool": "b"}],
        )

    # Zero-valued accelerator_type:* constraint is stripped without generating a selector.
    assert prepare_tpu_bundles_and_label_selector(
        [{"TPU": 1, "accelerator_type:TPU-V5P": 0}],
        accelerator_type_str=None,
    ) == ([{"TPU": 1}], None)


@pytest.mark.parametrize(
    "tp_size,selector,expected",
    [
        (1, None, [{"ray.io/accelerator-type": "TPU-V6E"}]),
        (1, [], [{"ray.io/accelerator-type": "TPU-V6E"}]),
        (1, (), [{"ray.io/accelerator-type": "TPU-V6E"}]),
        (1, [{}], [{"ray.io/accelerator-type": "TPU-V6E"}]),
        (1, ({},), [{"ray.io/accelerator-type": "TPU-V6E"}]),
        (1, [{"pool": "a"}], [{"ray.io/accelerator-type": "TPU-V6E", "pool": "a"}]),
        (1, ({"pool": "a"},), [{"ray.io/accelerator-type": "TPU-V6E", "pool": "a"}]),
        (
            1,
            ({"ray.io/accelerator-type": "TPU-V5P"},),
            [{"ray.io/accelerator-type": "TPU-V5P"}],
        ),
        (
            2,
            None,
            [
                {"ray.io/accelerator-type": "TPU-V6E"},
                {"ray.io/accelerator-type": "TPU-V6E"},
            ],
        ),
        (
            2,
            [],
            [
                {"ray.io/accelerator-type": "TPU-V6E"},
                {"ray.io/accelerator-type": "TPU-V6E"},
            ],
        ),
        (
            2,
            (),
            [
                {"ray.io/accelerator-type": "TPU-V6E"},
                {"ray.io/accelerator-type": "TPU-V6E"},
            ],
        ),
        (
            2,
            [{"pool": "a"}],
            [
                {"ray.io/accelerator-type": "TPU-V6E", "pool": "a"},
                {"ray.io/accelerator-type": "TPU-V6E", "pool": "a"},
            ],
        ),
        (
            2,
            ({"pool": "a"},),
            [
                {"ray.io/accelerator-type": "TPU-V6E", "pool": "a"},
                {"ray.io/accelerator-type": "TPU-V6E", "pool": "a"},
            ],
        ),
        (
            2,
            [{"pool": "a"}, {"pool": "b"}],
            [
                {"ray.io/accelerator-type": "TPU-V6E", "pool": "a"},
                {"ray.io/accelerator-type": "TPU-V6E", "pool": "b"},
            ],
        ),
        (
            2,
            ({"pool": "a"}, {"pool": "b"}),
            [
                {"ray.io/accelerator-type": "TPU-V6E", "pool": "a"},
                {"ray.io/accelerator-type": "TPU-V6E", "pool": "b"},
            ],
        ),
    ],
)
def test_tpu_non_deferred_label_selector_normalization_matrix(
    tp_size, selector, expected
):
    """Non-deferred TPU get_deployment_options normalizes all list/tuple forms and passes Serve validation."""
    llm_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(model_id="test-tpu-model"),
        accelerator_type="TPU-V6E",
        engine_kwargs={"tensor_parallel_size": tp_size},
        deployment_config={"placement_group_bundle_label_selector": selector},
    )
    opts = LLMServer.get_deployment_options(llm_config)
    actual = opts["placement_group_bundle_label_selector"]
    assert isinstance(actual, list)
    assert actual == expected
    if len(actual) > 1:
        assert actual[0] is not actual[1]
    assert (
        llm_config.deployment_config["placement_group_bundle_label_selector"]
        == selector
    )
    _validate_serve_deployment_options(opts, name=f"test-tpu-tp{tp_size}")


@pytest.mark.parametrize(
    "cfg_id,server_cls,accel_kwargs,extra_engine_kwargs",
    [
        ("gpu_l4", LLMServer, {"accelerator_type": "L4"}, {}),
        ("gpu_default", LLMServer, {}, {}),
        ("cpu", LLMServer, {"accelerator_config": CPUConfig(kind="cpu")}, {}),
        ("tpu_unpinned", LLMServer, {"accelerator_config": TPUConfig(kind="tpu")}, {}),
        ("dp_gpu_l4", DPServer, {"accelerator_type": "L4"}, {"data_parallel_size": 2}),
        (
            "dp_cpu",
            DPServer,
            {"accelerator_config": CPUConfig(kind="cpu")},
            {"data_parallel_size": 2},
        ),
        (
            "dp_tpu_unpinned",
            DPServer,
            {"accelerator_config": TPUConfig(kind="tpu")},
            {"data_parallel_size": 2},
        ),
    ],
)
@pytest.mark.parametrize("tp_size", [1, 2])
@pytest.mark.parametrize("selector", [None, [], ()])
def test_non_deferred_empty_and_null_selector_popped_across_hardware(
    cfg_id, server_cls, accel_kwargs, extra_engine_kwargs, tp_size, selector
):
    """Non-deferred GPU/CPU/unpinned TPU and DPServer pop None/[]/() selectors and pass all Serve validators."""
    llm_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(model_id="test-model"),
        engine_kwargs={"tensor_parallel_size": tp_size, **extra_engine_kwargs},
        deployment_config={"placement_group_bundle_label_selector": selector},
        **accel_kwargs,
    )
    opts = server_cls.get_deployment_options(llm_config)
    assert "placement_group_bundle_label_selector" not in opts
    stored = llm_config.deployment_config["placement_group_bundle_label_selector"]
    assert stored == selector
    assert type(stored) is type(selector)
    assert server_cls.get_deployment_options(llm_config) == opts
    _validate_serve_deployment_options(
        opts,
        name=f"test-{cfg_id.replace('_', '-')}-tp{tp_size}",
        server_cls=server_cls,
    )

    # DPServer on pinned TPU retains and merges ray.io/accelerator-type across empty and single selectors.
    if cfg_id == "dp_tpu_unpinned" and selector is None:
        for dp_sel, expected_sel in (
            (None, [{"ray.io/accelerator-type": "TPU-V6E"}] * tp_size),
            ([], [{"ray.io/accelerator-type": "TPU-V6E"}] * tp_size),
            ((), [{"ray.io/accelerator-type": "TPU-V6E"}] * tp_size),
            (
                [{"pool": "a"}],
                [{"ray.io/accelerator-type": "TPU-V6E", "pool": "a"}] * tp_size,
            ),
            (
                ({"pool": "a"},),
                [{"ray.io/accelerator-type": "TPU-V6E", "pool": "a"}] * tp_size,
            ),
        ):
            dp_pinned_cfg = LLMConfig(
                model_loading_config=ModelLoadingConfig(model_id="test-tpu-model"),
                accelerator_type="TPU-V6E",
                engine_kwargs={
                    "tensor_parallel_size": tp_size,
                    "data_parallel_size": 2,
                },
                deployment_config={"placement_group_bundle_label_selector": dp_sel},
            )
            dp_opts = DPServer.get_deployment_options(dp_pinned_cfg)
            assert dp_opts["placement_group_bundle_label_selector"] == expected_sel
            _validate_serve_deployment_options(
                dp_opts,
                name=f"test-dp-tpu-pinned-tp{tp_size}",
                server_cls=DPServer,
            )


@pytest.mark.parametrize(
    "cfg_id,server_cls,accel_kwargs,extra_engine_kwargs,pinned_tpu",
    [
        ("gpu_l4", LLMServer, {"accelerator_type": "L4"}, {}, False),
        ("gpu_default", LLMServer, {}, {}, False),
        ("cpu", LLMServer, {"accelerator_config": CPUConfig(kind="cpu")}, {}, False),
        (
            "tpu_unpinned",
            LLMServer,
            {"accelerator_config": TPUConfig(kind="tpu")},
            {},
            False,
        ),
        ("tpu_pinned", LLMServer, {"accelerator_type": "TPU-V6E"}, {}, True),
        (
            "dp_gpu_l4",
            DPServer,
            {"accelerator_type": "L4"},
            {"data_parallel_size": 2},
            False,
        ),
        (
            "dp_tpu_pinned",
            DPServer,
            {"accelerator_type": "TPU-V6E"},
            {"data_parallel_size": 2},
            True,
        ),
    ],
)
def test_non_deferred_selector_tuple_normalization_and_unaliasing(
    cfg_id, server_cls, accel_kwargs, extra_engine_kwargs, pinned_tpu
):
    """Non-deferred paths convert tuples to lists and shallow-copy aliased caller dicts across hardware."""
    base = {"ray.io/accelerator-type": "TPU-V6E"} if pinned_tpu else {}
    shared = {"pool": "shared"}
    cases = [
        ([{}], [{**base}] * (2 if pinned_tpu else 1), False),
        (({},), [{**base}] * (2 if pinned_tpu else 1), False),
        ([{"pool": "a"}], [{**base, "pool": "a"}] * (2 if pinned_tpu else 1), False),
        (({"pool": "a"},), [{**base, "pool": "a"}] * (2 if pinned_tpu else 1), False),
        ([shared, shared], [{**base, "pool": "shared"}] * 2, True),
        ((shared, shared), [{**base, "pool": "shared"}] * 2, True),
    ]
    for selector, expected, check_unaliased in cases:
        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test-model"),
            engine_kwargs={"tensor_parallel_size": 2, **extra_engine_kwargs},
            deployment_config={"placement_group_bundle_label_selector": selector},
            **accel_kwargs,
        )
        opts = server_cls.get_deployment_options(llm_config)
        actual = opts["placement_group_bundle_label_selector"]
        assert isinstance(actual, list)
        assert actual == expected
        _validate_serve_deployment_options(
            opts,
            name=f"test-{cfg_id.replace('_', '-')}-norm",
            server_cls=server_cls,
        )

        # Verify non-mutation of input config and idempotency across calls.
        stored = llm_config.deployment_config["placement_group_bundle_label_selector"]
        assert stored == selector
        assert type(stored) is type(selector)
        assert server_cls.get_deployment_options(llm_config) == opts

        if check_unaliased:
            assert actual[0] is not actual[1]
            assert actual[0] is not shared
            assert actual[1] is not shared
            actual[0]["mut"] = "1"
            assert "mut" not in actual[1]
            assert "mut" not in shared
            assert "mut" not in stored[0]
            assert (
                server_cls.get_deployment_options(llm_config)[
                    "placement_group_bundle_label_selector"
                ]
                == expected
            )


def test_tpu_non_deferred_label_selector_negative_cases(monkeypatch):
    """Mismatched selector lengths fail all Serve validators and invalid shapes always raise ValueError."""
    hardware_configs = [
        ("gpu_l4", {"accelerator_type": "L4"}, GPUAccelerator(), [{"GPU": 1}]),
        ("gpu_default", {}, GPUAccelerator(), [{"GPU": 1}]),
        (
            "cpu",
            {"accelerator_config": CPUConfig(kind="cpu")},
            CPUAccelerator(),
            [{"CPU": 1}],
        ),
        (
            "tpu_unpinned",
            {"accelerator_config": TPUConfig(kind="tpu")},
            TPUAccelerator(TPUConfig(kind="tpu", topology=None)),
            [{"TPU": 1}],
        ),
        (
            "tpu_pinned",
            {"accelerator_type": "TPU-V6E"},
            TPUAccelerator(TPUConfig(kind="tpu", topology=None)),
            [{"TPU": 1}],
        ),
    ]

    # Wrong-length selectors (3-element list or tuple for tp=2) normalize to a 3-element
    # list and are rejected by all 4 Serve entrypoints across hardware and DPServer.
    three_list = [{"pool": "a"}, {"pool": "b"}, {"pool": "c"}]
    three_tuple = tuple(three_list)
    for cfg_id, accel_kwargs, _, _ in hardware_configs:
        for server_cls, dp_kwargs in (
            (LLMServer, {}),
            (DPServer, {"data_parallel_size": 2}),
        ):
            for bad_len_sel in (three_list, three_tuple):
                mismatch_cfg = LLMConfig(
                    model_loading_config=ModelLoadingConfig(model_id="test-model"),
                    engine_kwargs={"tensor_parallel_size": 2, **dp_kwargs},
                    deployment_config={
                        "placement_group_bundle_label_selector": bad_len_sel
                    },
                    **accel_kwargs,
                )
                if cfg_id == "tpu_pinned":
                    with pytest.raises(
                        ValueError,
                        match="placement_group_bundle_label_selector length",
                    ):
                        server_cls.get_deployment_options(mismatch_cfg)
                    continue
                opts = server_cls.get_deployment_options(mismatch_cfg)
                assert isinstance(opts["placement_group_bundle_label_selector"], list)
                assert len(opts["placement_group_bundle_label_selector"]) == 3
                with pytest.raises(ValueError, match="Got 3 selectors for 2 bundles"):
                    DeploymentSchema(
                        name=f"test-mismatch-{cfg_id.replace('_', '-')}",
                        **copy.deepcopy(opts),
                    )
                replica_kwargs = {
                    k: copy.deepcopy(v)
                    for k, v in opts.items()
                    if k in _REPLICA_CONFIG_KEYS
                }
                with pytest.raises(ValueError, match="length"):
                    ReplicaConfig.create(server_cls, **replica_kwargs)
                with pytest.raises(ValueError):
                    serve.deployment(server_cls, **copy.deepcopy(opts))
                with pytest.raises(ValueError):
                    serve.deployment(server_cls).options(**copy.deepcopy(opts))

    # Invalid selector shapes raise ValueError via VLLMEngineConfig AND directly inside
    # LLMServer.get_deployment_options even when get_engine_config is stubbed/cached.
    invalid_shapes = (
        {},
        {"a": "1"},
        "",
        "abc",
        [123],
        [None],
        [{"a": 123}],
        [{"a": True}],
        [{1: "a"}],
    )
    for _, accel_kwargs, fake_accel, fake_bundles in hardware_configs:
        fake_engine_cfg = SimpleNamespace(
            accelerator=fake_accel,
            placement_bundles=fake_bundles,
            placement_strategy="PACK",
            get_runtime_env_with_local_env_vars=lambda: {},
        )
        for bad_shape in invalid_shapes:
            bad_cfg = LLMConfig(
                model_loading_config=ModelLoadingConfig(model_id="test-model"),
                deployment_config={"placement_group_bundle_label_selector": bad_shape},
                **accel_kwargs,
            )
            with pytest.raises(ValueError):
                LLMServer.get_deployment_options(bad_cfg)

            with monkeypatch.context() as mp:
                mp.setattr(LLMConfig, "get_engine_config", lambda self: fake_engine_cfg)
                with pytest.raises(ValueError):
                    LLMServer.get_deployment_options(bad_cfg)


@pytest.mark.parametrize(
    "accelerator",
    [CPUAccelerator(), GPUAccelerator()],
    ids=["cpu", "gpu"],
)
def test_cpu_and_gpu_accelerator_create_placement_group_bundle_label_selector(
    accelerator, monkeypatch
):
    """CPUAccelerator and GPUAccelerator normalize, broadcast, and validate bundle_label_selector."""
    captured = []

    def _fake_pg(**kwargs):
        captured.append(kwargs)
        return MagicMock(spec=PlacementGroup)

    monkeypatch.setattr(accelerators_mod, "placement_group", _fake_pg)
    two_bundles = [{"CPU": 1}, {"CPU": 1}]

    for empty_sel in (None, [], ()):
        captured.clear()
        accelerator.create_placement_group(
            bundles=two_bundles,
            strategy="PACK",
            name="pg",
            bundle_label_selector=empty_sel,
        )
        assert captured[0]["bundle_label_selector"] is None

    for single_sel in ([{"pool": "a"}], ({"pool": "a"},)):
        captured.clear()
        accelerator.create_placement_group(
            bundles=two_bundles,
            strategy="PACK",
            name="pg",
            bundle_label_selector=single_sel,
        )
        out = captured[0]["bundle_label_selector"]
        assert out == [{"pool": "a"}, {"pool": "a"}]
        assert out[0] is not out[1]

    for pair_sel in ([{"pool": "a"}, {"pool": "b"}], ({"pool": "a"}, {"pool": "b"})):
        captured.clear()
        accelerator.create_placement_group(
            bundles=two_bundles,
            strategy="PACK",
            name="pg",
            bundle_label_selector=pair_sel,
        )
        assert captured[0]["bundle_label_selector"] == [{"pool": "a"}, {"pool": "b"}]

    with pytest.raises(ValueError, match="bundle_label_selector length"):
        accelerator.create_placement_group(
            bundles=[{"CPU": 1}, {"CPU": 1}, {"CPU": 1}],
            strategy="PACK",
            name="pg",
            bundle_label_selector=[{"pool": "a"}, {"pool": "b"}],
        )

    for bad_sel in ({"a": "1"}, [123], [{"a": 123}]):
        with pytest.raises(ValueError):
            accelerator.create_placement_group(
                bundles=two_bundles,
                strategy="PACK",
                name="pg",
                bundle_label_selector=bad_sel,
            )


@pytest.mark.parametrize(
    "topology,bundles,accelerator_type,selector,error_match",
    [
        # Mismatched selector length vs 4 host bundles on 4x4.
        (
            "4x4",
            [{"TPU": 4}] * 4,
            "TPU-V6E",
            [{"pool": "a"}, {"pool": "b"}],
            "bundle_label_selector length",
        ),
        # Mismatched selector length vs 16 per-chip bundles on 4x4.
        (
            "4x4",
            [{"TPU": 1}, {"TPU": 1}],
            "TPU-V6E",
            [{"pool": "a"}, {"pool": "b"}],
            "bundle_label_selector length",
        ),
        # Mismatched selector length vs 1 host bundle on single-host 2x4.
        (
            "2x4",
            [{"TPU": 8}],
            "TPU-V6E",
            [{"pool": "a"}, {"pool": "b"}],
            "bundle_label_selector length",
        ),
        # Non-divisible TPU resource per worker.
        ("4x4", [{"TPU": 3}], "TPU-V6E", [{"pool": "a"}], "not divisible"),
        # TPU resource per worker exceeding chips per host (4 on v6e 4x4).
        ("4x4", [{"TPU": 8}], "TPU-V6E", [{"pool": "a"}], "evenly divide"),
        # Invalid topology (not divisible and divisible-but-unsupported).
        (
            "99x99",
            [{"TPU": 4}],
            "TPU-V6E",
            [{"pool": "a"}],
            "Invalid accelerator topology",
        ),
        (
            "100x100",
            [{"TPU": 4}],
            "TPU-V6E",
            [{"pool": "a"}],
            "Invalid accelerator topology",
        ),
        (
            "   ",
            [{"TPU": 4}],
            "TPU-V6E",
            [{"pool": "a"}],
            "Invalid accelerator topology",
        ),
        # Invalid Kubernetes label selector key or value syntax.
        (
            "4x4",
            [{"TPU": 4}] * 4,
            "TPU-V6E",
            [{"-bad-key": "a"}],
            "Invalid label selector",
        ),
        (
            "4x4",
            [{"TPU": 4}] * 4,
            "TPU-V6E",
            [{"pool": "!!!bad"}],
            "Invalid label selector",
        ),
        # Invalid extra resource value type in worker bundle.
        (
            "4x4",
            [{"TPU": 4, "custom": "bad"}],
            "TPU-V6E",
            None,
            "resource dictionaries",
        ),
        # Invalid accelerator_type and heterogeneous bundles.
        ("4x4", [{"TPU": 4}], "GPU-L4", None, "Invalid accelerator_type"),
        (
            "4x4",
            [{"TPU": 4}, {"TPU": 2}],
            "TPU-V6E",
            None,
            "Heterogeneous TPU bundles",
        ),
    ],
)
def test_tpu_create_pg_preserves_existing_slice_on_validation_error(
    topology, bundles, accelerator_type, selector, error_match
):
    """Validation failures must raise before shutting down an existing live _slice_pg_wrapper."""
    existing = MagicMock()
    acc = TPUAccelerator(TPUConfig(kind="tpu", topology=topology))
    acc._slice_pg_wrapper = existing

    with pytest.raises(ValueError, match=error_match):
        acc.create_placement_group(
            bundles=bundles,
            strategy="PACK",
            name="probe-pg",
            accelerator_type_str=accelerator_type,
            bundle_label_selector=selector,
        )

    existing.shutdown.assert_not_called()
    assert acc._slice_pg_wrapper is existing


def test_tpu_create_pg_replaces_existing_slice_on_valid_reconfiguration(monkeypatch):
    """Valid reconfiguration shuts down the previous _slice_pg_wrapper and creates a new slice PG."""
    pg_calls = []
    created_pg = MagicMock(spec=PlacementGroup, name="replacement_pg")

    def _fake_placement_group(*args, **kwargs):
        pg_calls.append(kwargs)
        return created_pg

    monkeypatch.setattr("ray._private.client_mode_hook.auto_init_ray", lambda: None)
    monkeypatch.setattr("ray.util.tpu.placement_group", _fake_placement_group)

    existing = MagicMock()
    acc = TPUAccelerator(TPUConfig(kind="tpu", topology="4x4"))
    acc._slice_pg_wrapper = existing

    result = acc.create_placement_group(
        bundles=[{"TPU": 4}] * 4,
        strategy="PACK",
        name="probe-pg",
        accelerator_type_str="TPU-V6E",
        bundle_label_selector=[{"pool": "a"}],
    )

    existing.shutdown.assert_called_once()
    assert acc._slice_pg_wrapper is not existing
    assert result is created_pg
    assert len(pg_calls) == 1
    assert len(pg_calls[0]["bundle_label_selector"]) == 4


def test_prepare_tpu_bundles_empty_accel_and_bool_values():
    """Empty/whitespace accelerator_type_str falls back to bundle constraints and bools are not positive numbers."""
    # Empty or whitespace-only accelerator_type_str falls back to bundle-level constraint.
    for empty_accel in ("", "   "):
        assert prepare_tpu_bundles_and_label_selector(
            [{"TPU": 1, "accelerator_type:TPU-V5P": 0.001}],
            accelerator_type_str=empty_accel,
        ) == ([{"TPU": 1}], [{"ray.io/accelerator-type": "TPU-V5P"}])

    # Whitespace-only constraint suffix is stripped without emitting an invalid label.
    assert prepare_tpu_bundles_and_label_selector(
        [{"TPU": 1, "accelerator_type:   ": 0.001}],
        accelerator_type_str=None,
    ) == ([{"TPU": 1}], None)

    # Boolean resource values are not treated as positive numeric resource counts.
    assert prepare_tpu_bundles_and_label_selector(
        [{"TPU": 1, "accelerator_type:TPU-V5P": True}],
        accelerator_type_str=None,
    ) == ([{"TPU": 1}], None)
    assert prepare_tpu_bundles_and_label_selector(
        [{"TPU": True}],
        accelerator_type_str="TPU-V6E",
    ) == ([{"TPU": True}], None)
    assert prepare_tpu_bundles_and_label_selector(
        [{"TPU": False}],
        accelerator_type_str="TPU-V6E",
    ) == ([{"TPU": False}], None)
    assert prepare_tpu_bundles_and_label_selector(
        [{"TPU": 1, "accelerator_type:TPU-V5P": float("nan")}],
        accelerator_type_str=None,
    ) == ([{"TPU": 1}], None)


def test_resolve_bundle_label_selector_non_positive_num_bundles():
    """_resolve_bundle_label_selector rejects negative num_bundles and non-empty selectors when num_bundles is 0."""
    # Zero bundles with no user selector returns None cleanly (or [] if base_selectors=[]).
    for empty_sel in (None, [], ()):
        assert _resolve_bundle_label_selector(0, empty_sel, None) is None
    assert _resolve_bundle_label_selector(0, None, []) == []

    # Zero bundles with any non-empty selector (including 1-element) raises ValueError.
    for non_empty_sel in ([{"a": "1"}], ({"a": "1"},), [{"a": "1"}, {"b": "2"}]):
        with pytest.raises(ValueError, match="bundle_label_selector length"):
            _resolve_bundle_label_selector(0, non_empty_sel, None)

    # Negative num_bundles always raises ValueError.
    for user_sel in (None, [], [{"a": "1"}], [{"a": "1"}, {"b": "2"}]):
        with pytest.raises(ValueError, match="non-negative"):
            _resolve_bundle_label_selector(-1, user_sel, None)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
