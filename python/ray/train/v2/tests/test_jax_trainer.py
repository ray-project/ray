import pytest

import ray
from ray.tests.conftest import _ray_start_cluster
from ray.train import RunConfig, ScalingConfig
from ray.train.v2._internal.constants import (
    HEALTH_CHECK_INTERVAL_S_ENV_VAR,
    is_v2_enabled,
)
from ray.train.v2.jax import JaxTrainer

assert is_v2_enabled()


@pytest.fixture
def ray_tpu_single_host(monkeypatch):
    """Start a mock single-host TPU Ray cluster with 2x4 v6e (8 chips per host)."""
    with _ray_start_cluster() as cluster:
        monkeypatch.setenv("TPU_ACCELERATOR_TYPE", "v6e-8")

        # Simulate one node with 8 TPU chips.
        cluster.add_node(
            num_cpus=4,
            resources={"TPU": 8},
        )

        ray.init(address=cluster.address)

        yield cluster
        ray.shutdown()


@pytest.fixture
def ray_tpu_multi_host(monkeypatch):
    """Start a simulated multi-host TPU Ray cluster."""
    with _ray_start_cluster() as cluster:
        monkeypatch.setenv("TPU_NAME", "test-slice-1")
        monkeypatch.setenv("TPU_WORKER_ID", "0")
        monkeypatch.setenv("TPU_ACCELERATOR_TYPE", "v4-8")
        monkeypatch.setenv("TPU_TOPOLOGY", "2x2x2")

        cluster.add_node(
            num_cpus=2,
            resources={"TPU": 4, "TPU-v4-8-head": 1},
        )
        monkeypatch.setenv("TPU_WORKER_ID", "1")
        cluster.add_node(
            num_cpus=2,
            resources={"TPU": 4},
        )

        ray.init(address=cluster.address)

        yield cluster
        ray.shutdown()


@pytest.fixture(autouse=True)
def reduce_health_check_interval(monkeypatch):
    monkeypatch.setenv(HEALTH_CHECK_INTERVAL_S_ENV_VAR, "0.2")
    yield


def train_func():
    import jax

    from ray import train

    devices = jax.devices()
    print(f"Devices on this worker: {devices}")
    train.report({"result": [str(d) for d in devices]})


def train_func_check_env():
    """Training function to verify JAX_PLATFORMS env var is set."""
    import os

    from ray import train

    jax_platforms = os.environ.get("JAX_PLATFORMS", "")
    train.report({"jax_platforms": jax_platforms})


def test_minimal_singlehost(ray_tpu_single_host, tmp_path):
    trainer = JaxTrainer(
        train_loop_per_worker=train_func,
        # Topology can be omitted for single-host.
        scaling_config=ScalingConfig(
            num_workers=1,
            resources_per_worker={"TPU": 8},
            use_tpu=True,
            accelerator_type="TPU-V6E",
        ),
        run_config=RunConfig(
            storage_path=str(tmp_path),
        ),
    )
    result = trainer.fit()
    assert result.error is None

    # Check that exactly 1 TPU node was used.
    nodes = ray.nodes()
    labeled_nodes = [
        node for node in nodes if node["Alive"] and node["Resources"].get("TPU") == 8
    ]
    assert len(labeled_nodes) == 1


def test_minimal_multihost(ray_tpu_multi_host, tmp_path):
    trainer = JaxTrainer(
        train_loop_per_worker=train_func,
        scaling_config=ScalingConfig(
            num_workers=2,
            resources_per_worker={"TPU": 4},
            use_tpu=True,
            topology="2x2x2",
            accelerator_type="TPU-V4",
        ),
        run_config=RunConfig(
            storage_path=str(tmp_path),
        ),
    )
    result = trainer.fit()
    assert result.error is None

    # Check that multi-host slice was scheduled atomically.
    nodes = ray.nodes()
    slice_label = "test-slice-1"
    labeled_nodes = [
        node
        for node in nodes
        if node["Alive"] and node["Labels"].get("ray.io/tpu-slice-name") == slice_label
    ]
    assert len(labeled_nodes) == 2


def test_jax_platforms_env_var_no_existing(ray_tpu_single_host, tmp_path, monkeypatch):
    """Test that JAX_PLATFORMS is set to 'tpu' when use_tpu=True and no existing value."""
    # Ensure JAX_PLATFORMS is not set
    monkeypatch.delenv("JAX_PLATFORMS", raising=False)

    trainer = JaxTrainer(
        train_loop_per_worker=train_func_check_env,
        scaling_config=ScalingConfig(
            num_workers=1,
            resources_per_worker={"TPU": 8},
            use_tpu=True,
            accelerator_type="TPU-V6E",
        ),
        run_config=RunConfig(
            storage_path=str(tmp_path),
        ),
    )
    result = trainer.fit()
    assert result.error is None

    # Check that JAX_PLATFORMS was set to "tpu"
    jax_platforms = result.metrics["jax_platforms"]
    assert jax_platforms == "tpu"


def test_jax_platforms_env_var_with_existing_tpu(
    ray_tpu_single_host, tmp_path, monkeypatch
):
    """Test that JAX_PLATFORMS is not modified when 'tpu' is already present."""
    monkeypatch.setenv("JAX_PLATFORMS", "tpu,cpu")

    trainer = JaxTrainer(
        train_loop_per_worker=train_func_check_env,
        scaling_config=ScalingConfig(
            num_workers=1,
            resources_per_worker={"TPU": 8},
            use_tpu=True,
            accelerator_type="TPU-V6E",
        ),
        run_config=RunConfig(
            storage_path=str(tmp_path),
        ),
    )
    result = trainer.fit()
    assert result.error is None

    # Check that JAX_PLATFORMS was not modified
    jax_platforms = result.metrics["jax_platforms"]
    assert jax_platforms == "tpu,cpu"


def test_jax_platforms_env_var_with_existing_other(
    ray_tpu_single_host, tmp_path, monkeypatch
):
    """Test that 'tpu' is prepended when JAX_PLATFORMS contains other platforms."""
    monkeypatch.setenv("JAX_PLATFORMS", "cpu")

    trainer = JaxTrainer(
        train_loop_per_worker=train_func_check_env,
        scaling_config=ScalingConfig(
            num_workers=1,
            resources_per_worker={"TPU": 8},
            use_tpu=True,
            accelerator_type="TPU-V6E",
        ),
        run_config=RunConfig(
            storage_path=str(tmp_path),
        ),
    )
    result = trainer.fit()
    assert result.error is None

    # Check that 'tpu' was prepended to existing platforms
    jax_platforms = result.metrics["jax_platforms"]
    assert jax_platforms == "tpu,cpu"


def test_jax_platforms_env_var_no_tpu_flag(ray_tpu_single_host, tmp_path, monkeypatch):
    """Test that JAX_PLATFORMS is not set when use_tpu=False."""
    monkeypatch.delenv("JAX_PLATFORMS", raising=False)

    trainer = JaxTrainer(
        train_loop_per_worker=train_func_check_env,
        scaling_config=ScalingConfig(
            num_workers=1,
            resources_per_worker={"TPU": 8},
            use_tpu=False,  # Not using TPU
            accelerator_type="TPU-V6E",
        ),
        run_config=RunConfig(
            storage_path=str(tmp_path),
        ),
    )
    result = trainer.fit()
    assert result.error is None

    # Check that JAX_PLATFORMS was not set
    jax_platforms = result.metrics["jax_platforms"]
    assert jax_platforms == ""


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
