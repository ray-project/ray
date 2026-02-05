import os
import sys

import jax
import jax.numpy as jnp
import numpy as np
import pytest
from jax.sharding import Mesh, NamedSharding, PartitionSpec

import ray
from ray import train
from ray.tests.conftest import _ray_start_cluster
from ray.train import CheckpointConfig, RunConfig, ScalingConfig, UserCallback
from ray.train.v2._internal.constants import (
    HEALTH_CHECK_INTERVAL_S_ENV_VAR,
    is_v2_enabled,
)
from ray.train.v2.jax import JaxTrainer
from ray.train.v2.jax.checkpoint import JaxCheckpointManager

assert is_v2_enabled()


@pytest.fixture(scope="module")
def _ray_tpu_single_host_cluster():
    """Start a mock single-host TPU Ray cluster with 2x4 v6e (8 chips per host)."""
    with _ray_start_cluster() as cluster:
        # Simulate one node with 8 TPU chips.
        cluster.add_node(
            num_cpus=4,
            resources={"TPU": 8, "accelerator_type:TPU-V6E": 1},
            env_vars={"TPU_ACCELERATOR_TYPE": "v6e-8"},
        )
        yield cluster


@pytest.fixture
def ray_tpu_single_host(_ray_tpu_single_host_cluster):
    """
    Initialize Ray with the single-host TPU cluster.
    Each test gets a fresh Ray client connection for isolation.
    """
    ray.init(address=_ray_tpu_single_host_cluster.address)
    yield _ray_tpu_single_host_cluster
    ray.shutdown()


@pytest.fixture(scope="module")
def _ray_tpu_multi_host_cluster():
    """
    Simulates a Ray cluster with two multi-host TPU v4-16 slices.
    """

    pod_type = "v4-16"
    topology = "2x2x2"

    with _ray_start_cluster() as cluster:
        # First TPU slice - v4-16 with 2 hosts and 4 chips/host.
        slice_a_head_env = {
            "TPU_NAME": "slice-A",
            "TPU_WORKER_ID": "0",
            "TPU_ACCELERATOR_TYPE": pod_type,
            "TPU_TOPOLOGY": topology,
        }
        slice_a_head_labels = {
            "ray.io/tpu-slice-name": "slice-A",
            "ray.io/tpu-worker-id": "0",
            "ray.io/tpu-pod-type": pod_type,
        }
        slice_a_worker_env = {
            "TPU_NAME": "slice-A",
            "TPU_WORKER_ID": "1",
            "TPU_ACCELERATOR_TYPE": pod_type,
            "TPU_TOPOLOGY": topology,
        }
        slice_a_worker_labels = {
            "ray.io/tpu-slice-name": "slice-A",
            "ray.io/tpu-worker-id": "1",
            "ray.io/tpu-pod-type": pod_type,
        }
        cluster.add_node(
            num_cpus=8,
            resources={
                "TPU": 4,
                f"TPU-{pod_type}-head": 1,
                "accelerator_type:TPU-V4": 1,
            },
            env_vars=slice_a_head_env,
            labels=slice_a_head_labels,
        )
        cluster.add_node(
            num_cpus=8,
            resources={"TPU": 4, "accelerator_type:TPU-V4": 1},
            env_vars=slice_a_worker_env,
            labels=slice_a_worker_labels,
        )
        # Second TPU slice - v4-16 with 2 hosts and 4 chips/host.
        slice_b_head_env = {
            "TPU_NAME": "slice-B",
            "TPU_WORKER_ID": "0",
            "TPU_ACCELERATOR_TYPE": pod_type,
            "TPU_TOPOLOGY": topology,
        }
        slice_b_head_labels = {
            "ray.io/tpu-slice-name": "slice-B",
            "ray.io/tpu-worker-id": "0",
            "ray.io/tpu-pod-type": pod_type,
        }
        slice_b_worker_env = {
            "TPU_NAME": "slice-B",
            "TPU_WORKER_ID": "1",
            "TPU_ACCELERATOR_TYPE": pod_type,
            "TPU_TOPOLOGY": topology,
        }
        slice_b_worker_labels = {
            "ray.io/tpu-slice-name": "slice-B",
            "ray.io/tpu-worker-id": "1",
            "ray.io/tpu-pod-type": pod_type,
        }
        cluster.add_node(
            num_cpus=8,
            resources={
                "TPU": 4,
                f"TPU-{pod_type}-head": 1,
                "accelerator_type:TPU-V4": 1,
            },
            env_vars=slice_b_head_env,
            labels=slice_b_head_labels,
        )
        cluster.add_node(
            num_cpus=8,
            resources={"TPU": 4, "accelerator_type:TPU-V4": 1},
            env_vars=slice_b_worker_env,
            labels=slice_b_worker_labels,
        )

        yield cluster


@pytest.fixture
def ray_tpu_multi_host(_ray_tpu_multi_host_cluster):
    """
    Initialize Ray with the multi-host TPU cluster.
    Each test gets a fresh Ray client connection for isolation.
    """
    ray.init(address=_ray_tpu_multi_host_cluster.address)
    yield _ray_tpu_multi_host_cluster
    ray.shutdown()


@pytest.fixture(autouse=True)
def reduce_health_check_interval(monkeypatch):
    monkeypatch.setenv(HEALTH_CHECK_INTERVAL_S_ENV_VAR, "0.2")
    yield


def train_func():
    train_ctx = train.get_context()
    rank = train_ctx.get_world_rank()

    devices = jax.devices()
    node_labels = ray.get_runtime_context().get_node_labels()
    slice_name = node_labels.get("ray.io/tpu-slice-name")
    current_ip = ray.util.get_node_ip_address()

    megascale_vars = {
        "MEGASCALE_SLICE_ID": os.environ.get("MEGASCALE_SLICE_ID"),
        "MEGASCALE_NUM_SLICES": os.environ.get("MEGASCALE_NUM_SLICES"),
        "MEGASCALE_COORDINATOR_ADDRESS": os.environ.get(
            "MEGASCALE_COORDINATOR_ADDRESS"
        ),
    }

    train.report(
        {
            "worker_id": rank,
            "slice_name": slice_name,
            "node_ip": current_ip,
            "devices": [str(d) for d in devices],
            **megascale_vars,
        }
    )


class CustomMetricsCallback(UserCallback):
    """
    In Ray Train V2, reporting metrics from all workers is a no-op, so we
    utilize this callback to access the results in our tests.
    """

    def __init__(self, actor_name: str):
        self.actor_name = actor_name

    def after_report(self, run_context, metrics, checkpoint):
        # Connect to the specific verify actor for this test.
        sink = ray.get_actor(self.actor_name)
        sink.log.remote(metrics)


@ray.remote
class VerificationActor:
    """
    This Actor is called from the custom metrics callback and saves
    the reported metrics from each test.
    """

    def __init__(self):
        self.reports = []

    def log(self, metrics):
        self.reports.extend(metrics)

    def get_reports(self):
        return self.reports


@pytest.mark.skipif(
    sys.version_info >= (3, 12),
    reason="Current jax version (0.4.13) is not supported in python 3.12+",
)
def test_tpu_single_host(ray_tpu_single_host, tmp_path):
    """
    Tests single-host scheduling with no topology value. In this case, the
    JaxTrainer should skip the multi-host slice scheduling logic and setup
    with a single TPU worker.
    """
    actor_name = "test_tpu_single_host"
    verify_actor = VerificationActor.options(name=actor_name).remote()

    trainer = JaxTrainer(
        train_loop_per_worker=train_func,
        scaling_config=ScalingConfig(
            use_tpu=True,
            num_workers=1,
            resources_per_worker={"TPU": 8},
            accelerator_type="TPU-V6E",
        ),
        run_config=RunConfig(
            storage_path=str(tmp_path),
            callbacks=[CustomMetricsCallback(actor_name)],
            worker_runtime_env={"env_vars": {"JAX_PLATFORMS": "cpu"}},
        ),
    )
    result = trainer.fit()
    assert result.error is None

    # Fetch metrics result using the verification actor.
    reports = ray.get(verify_actor.get_reports.remote())

    # The train func should have ran on one single-host TPU.
    assert len(reports) == 1, f"Expected 1 report, got {len(reports)}"
    report = reports[0]
    assert report["worker_id"] == 0

    # Validate we do not automatically set megascale vars for single-slice.
    for r in reports:
        assert r.get("MEGASCALE_SLICE_ID") is None
        assert r.get("MEGASCALE_NUM_SLICES") is None
        assert r.get("MEGASCALE_COORDINATOR_ADDRESS") is None


@pytest.mark.skipif(
    sys.version_info >= (3, 12),
    reason="Current jax version (0.4.13) is not supported in python 3.12+",
)
def test_tpu_single_slice_multi_host(ray_tpu_multi_host, tmp_path):
    """
    Tests scheduling on a single multi-host slice. The number of workers
    is set by the user to match the number of hosts in the slice, with each
    worker consuming the full resources on that host.
    """
    actor_name = "test_tpu_single_slice_multi_host"
    verify_actor = VerificationActor.options(name=actor_name).remote()

    trainer = JaxTrainer(
        train_loop_per_worker=train_func,
        scaling_config=ScalingConfig(
            use_tpu=True,
            accelerator_type="TPU-V4",
            topology="2x2x2",
            num_workers=2,
        ),
        run_config=RunConfig(
            storage_path=str(tmp_path),
            callbacks=[CustomMetricsCallback(actor_name)],
            worker_runtime_env={"env_vars": {"JAX_PLATFORMS": "cpu"}},
        ),
    )
    result = trainer.fit()
    assert result.error is None

    # Fetch metrics result from each worker using the verification actor.
    reports = ray.get(verify_actor.get_reports.remote())

    # Verify two TPU workers on the same slice ran the training func.
    assert (
        len(reports) == 2
    ), f"Expected 2 workers to report metrics, got {len(reports)}"
    worker_ids = {r["worker_id"] for r in reports}
    assert worker_ids == {0, 1}, "Expected unique worker IDs from 0 to N-1."
    slices_used = {r["slice_name"] for r in reports}
    assert len(slices_used) == 1, "Expected workers to be scheduled to 1 slice."
    assert next(iter(slices_used)) in ("slice-A", "slice-B")

    # Validate we do not automatically set megascale vars for single-slice.
    for r in reports:
        assert r.get("MEGASCALE_SLICE_ID") is None
        assert r.get("MEGASCALE_NUM_SLICES") is None
        assert r.get("MEGASCALE_COORDINATOR_ADDRESS") is None


@pytest.mark.skipif(
    sys.version_info >= (3, 12),
    reason="Current jax version (0.4.13) is not supported in python 3.12+",
)
def test_tpu_multi_slice_multi_host(ray_tpu_multi_host, tmp_path):
    """
    Tests execution of TPU workers across multiple multi-host slices. The
    user specifies num_workers equal to the total number of hosts across all
    slices.
    """
    actor_name = "test_tpu_multi_slice_multi_host"
    verify_actor = VerificationActor.options(name=actor_name).remote()

    trainer = JaxTrainer(
        train_loop_per_worker=train_func,
        scaling_config=ScalingConfig(
            use_tpu=True,
            accelerator_type="TPU-V4",
            topology="2x2x2",
            num_workers=4,
        ),
        run_config=RunConfig(
            storage_path=str(tmp_path),
            callbacks=[CustomMetricsCallback(actor_name)],
            worker_runtime_env={"env_vars": {"JAX_PLATFORMS": "cpu"}},
        ),
    )
    result = trainer.fit()
    assert result.error is None

    # Fetch metrics result from each worker using the verification actor.
    reports = ray.get(verify_actor.get_reports.remote())

    # Verify execution of all 4 TPU workers across both slices.
    assert (
        len(reports) == 4
    ), f"Expected 4 workers to report metrics, got {len(reports)}"
    worker_ids = {r["worker_id"] for r in reports}
    assert worker_ids == {0, 1, 2, 3}, "Expected unique worker IDs from 0 to N-1."
    slices_used = {r["slice_name"] for r in reports}
    assert len(slices_used) == 2, "Expected workers to schedule across 2 slices."
    assert "slice-A" in slices_used
    assert "slice-B" in slices_used

    # Verify megascale coordinator address set to IP of worker 0.
    worker_0_report = next(r for r in reports if r["worker_id"] == 0)
    expected_coordinator_ip = worker_0_report["node_ip"]

    for r in reports:
        assert r["MEGASCALE_COORDINATOR_ADDRESS"] == expected_coordinator_ip
        assert r["MEGASCALE_NUM_SLICES"] == "2"

    # Validate MEGASCALE_SLICE_ID set based on indexed TPU Pod name.
    slice_a_reports = [r for r in reports if r["slice_name"] == "slice-A"]
    slice_b_reports = [r for r in reports if r["slice_name"] == "slice-B"]

    assert list({r["MEGASCALE_SLICE_ID"] for r in slice_a_reports}) == ["0"]
    assert list({r["MEGASCALE_SLICE_ID"] for r in slice_b_reports}) == ["1"]


@pytest.mark.skipif(
    sys.version_info >= (3, 12),
    reason="Current jax version (0.4.13) is not supported in python 3.12+",
)
def test_multi_slice_manual_resources(ray_tpu_multi_host, tmp_path):
    """
    Tests execution of TPU workers across multiple multi-host slices when
    `resources_per_worker` is specified. The JaxTrainer should execute across
    both slices with num_workers workers of the specified resources.
    """
    actor_name = "test_multi_slice_manual_resources"
    verify_actor = VerificationActor.options(name=actor_name).remote()

    trainer = JaxTrainer(
        train_loop_per_worker=train_func,
        scaling_config=ScalingConfig(
            use_tpu=True,
            accelerator_type="TPU-V4",
            topology="2x2x2",
            resources_per_worker={"TPU": 1},  # 1 CPU added by default per-bundle.
            num_workers=16,
        ),
        run_config=RunConfig(
            storage_path=str(tmp_path),
            callbacks=[CustomMetricsCallback(actor_name)],
            worker_runtime_env={"env_vars": {"JAX_PLATFORMS": "cpu"}},
        ),
    )
    result = trainer.fit()
    assert result.error is None

    # Fetch metrics result from each worker using the verification actor.
    reports = ray.get(verify_actor.get_reports.remote())

    # Verify execution of all 16 TPU workers across both v4-16 slices.
    assert (
        len(reports) == 16
    ), f"Expected 16 workers to report metrics, got {len(reports)}"
    worker_ids = {r["worker_id"] for r in reports}
    assert worker_ids == set(range(16)), "Expected unique worker IDs from 0 to N-1."
    slices_used = {r["slice_name"] for r in reports}
    assert len(slices_used) == 2, "Expected workers to span 2 slices."
    assert "slice-A" in slices_used
    assert "slice-B" in slices_used

    # Verify megascale coordinator address set to IP of worker 0.
    worker_0_report = next(r for r in reports if r["worker_id"] == 0)
    expected_coordinator_ip = worker_0_report["node_ip"]

    for r in reports:
        assert r["MEGASCALE_COORDINATOR_ADDRESS"] == expected_coordinator_ip
        assert r["MEGASCALE_NUM_SLICES"] == "2"

    # Validate MEGASCALE_SLICE_ID set based on indexed TPU Pod name.
    slice_a_reports = [r for r in reports if r["slice_name"] == "slice-A"]
    slice_b_reports = [r for r in reports if r["slice_name"] == "slice-B"]

    assert list({r["MEGASCALE_SLICE_ID"] for r in slice_a_reports}) == ["0"]
    assert list({r["MEGASCALE_SLICE_ID"] for r in slice_b_reports}) == ["1"]


@pytest.mark.skipif(
    sys.version_info >= (3, 12),
    reason="Current jax version (0.4.13) is not supported in python 3.12+",
)
def test_tpu_multi_slice_uneven_workers(ray_tpu_multi_host, tmp_path):
    """
    Tests that ScalingConfig raises a ValueError if the requested num_workers
    does not divide evenly across TPU slices of the requested topology.
    """
    # Default resources (1 worker per host).
    with pytest.raises(ValueError, match="must be a multiple of"):
        ScalingConfig(
            use_tpu=True,
            accelerator_type="TPU-V4",
            topology="2x2x2",
            num_workers=3,  # Expect a multiple of 2.
        )
    # Explicit resources (1 TPU chip per worker).
    with pytest.raises(ValueError, match="must be a multiple of"):
        ScalingConfig(
            use_tpu=True,
            accelerator_type="TPU-V4",
            topology="2x2x1",
            resources_per_worker={"TPU": 1},
            num_workers=6,  # Expect a multiple of 4.
        )


def test_scaling_config_validation():
    with pytest.raises(
        ValueError, match="Cannot set `label_selector` when `use_tpu=True`"
    ):
        ScalingConfig(
            num_workers=2,
            use_tpu=True,
            topology="2x2x2",
            accelerator_type="TPU-V4",
            label_selector={"subcluster": "my_subcluster"},
        )


def _generate_array_with_sharding(mesh, sharding, shape, value=None):
    """Helper function to generate a sharded array."""
    if value is None:
        data = np.arange(np.prod(shape)).reshape(shape)
    else:
        data = np.full(shape, value)

    indices_map = sharding.addressable_devices_indices_map(shape)
    local_devices = jax.local_devices()
    local_arrays = []
    for d in local_devices:
        idx = indices_map[d]
        local_arrays.append(jax.device_put(data[idx], d))

    return jax.make_array_from_single_device_arrays(shape, sharding, local_arrays)


@pytest.mark.skipif(
    sys.version_info >= (3, 12),
    reason="Current jax version (0.4.13) is not supported in python 3.12+",
)
def test_tpu_checkpointing_single_host(ray_tpu_single_host, tmp_path):
    """
    Tests that the JaxTrainer correctly handles sharded checkpoints using
    JaxCheckpointManager.
    """
    actor_name = "test_tpu_checkpointing_single_host"
    verify_actor = VerificationActor.options(name=actor_name).remote()

    def train_func_checkpointing():

        # Setup mesh
        devices = jax.devices()
        mesh = Mesh(devices, axis_names=("x",))

        # Create sharded array
        sharding = NamedSharding(mesh, PartitionSpec("x"))
        shape = (8, 8)
        w = _generate_array_with_sharding(mesh, sharding, shape, value=0)
        train_state = {"w": w}

        storage_context = train.get_context().get_storage()

        # Config to keep only the best checkpoint (min loss)
        checkpoint_config = CheckpointConfig(
            num_to_keep=1,
            checkpoint_score_attribute="loss",
            checkpoint_score_order="min",
        )

        manager = JaxCheckpointManager(
            storage_context=storage_context,
            checkpoint_config=checkpoint_config,
        )

        # 3 steps:
        # Step 0: loss 0.5
        # Step 1: loss 0.1 (Best)
        # Step 2: loss 0.3
        losses = [0.5, 0.1, 0.3]

        for step, loss in enumerate(losses):
            # Create a new w for each step so we can verify the best one is restored.
            # Step 0: all 0s
            # Step 1: all 1s (Best)
            # Step 2: all 2s
            w = _generate_array_with_sharding(mesh, sharding, shape, value=step)
            train_state = {"w": w}
            metrics = {"loss": loss}
            manager.save(step, train_state, metrics)

        manager.wait_until_finished()

        # Step 1 should be the one kept
        best_step = manager.best_step()
        assert best_step == 1
        expected_metrics = {"loss": 0.1}

        stored_metrics = manager.orbax_manager.metrics(best_step)
        assert stored_metrics == expected_metrics

        # Also verify we can restore the best step
        restore_target = {
            "w": _generate_array_with_sharding(mesh, sharding, shape, value=-1)
        }

        restored, _ = manager.restore(restore_target, best_step)

        # Verify restored value matches step 1 (all 1s)
        expected_w = _generate_array_with_sharding(
            mesh, sharding, shape, value=best_step
        )

        is_equal = bool(jnp.array_equal(restored["w"], expected_w))
        train.report({"is_equal": is_equal})

    trainer = JaxTrainer(
        train_loop_per_worker=train_func_checkpointing,
        scaling_config=ScalingConfig(
            use_tpu=True,
            num_workers=1,
            resources_per_worker={"TPU": 8},
            accelerator_type="TPU-V6E",
        ),
        run_config=RunConfig(
            storage_path=str(tmp_path),
            callbacks=[CustomMetricsCallback(actor_name)],
            worker_runtime_env={"env_vars": {"JAX_PLATFORMS": "cpu"}},
        ),
    )

    result = trainer.fit()
    assert result.error is None

    # Verify reports
    reports = ray.get(verify_actor.get_reports.remote())
    assert len(reports) == 1
    assert reports[0]["is_equal"] is True


def _mock_multi_host_sync(tmp_path):
    """
    Mock multi-host sync for CPU multi-process simulation.
    """
    import pickle
    import time

    import jax.experimental.multihost_utils as mhu

    barrier_dir = os.path.join(tmp_path, "barriers")
    os.makedirs(barrier_dir, exist_ok=True)

    def mock_sync(name, **kwargs):
        token = name.replace("/", "_").replace(":", "_")
        worker_token = f"{token}_{jax.process_index()}"
        with open(os.path.join(barrier_dir, worker_token), "w") as f:
            f.write("1")

        while True:
            files = os.listdir(barrier_dir)
            relevant = [f for f in files if f.startswith(token + "_")]
            if len(relevant) >= 2:  # num_workers
                break
            time.sleep(0.1)

    def mock_broadcast(x, **kwargs):
        mock_broadcast.counter = getattr(mock_broadcast, "counter", 0) + 1
        token = f"broadcast_{mock_broadcast.counter}"

        if jax.process_index() == 0:
            with open(os.path.join(barrier_dir, token), "wb") as f:
                pickle.dump(x, f)
            mock_sync(token)
            return x
        else:
            mock_sync(token)
            with open(os.path.join(barrier_dir, token), "rb") as f:
                return pickle.load(f)

    mhu.sync_global_devices = mock_sync
    mhu.broadcast_one_to_all = mock_broadcast
    mhu.assert_equal = lambda x, **kwargs: None


@pytest.mark.skipif(
    sys.version_info >= (3, 12),
    reason="Current jax version (0.4.13) is not supported in python 3.12+",
)
def test_tpu_checkpointing_multi_host(ray_tpu_multi_host, tmp_path):
    """
    Tests that the JaxTrainer correctly handles sharded checkpoints using
    JaxCheckpointManager.
    """
    actor_name = "test_tpu_checkpointing_multi_host"
    verify_actor = VerificationActor.options(name=actor_name).remote()

    def train_func_checkpointing():

        # Setup mesh
        devices = jax.devices()

        if devices[0].platform == "cpu":
            # JAX's CPU backend does not support multiprocess collective operations in jax 0.4.*
            _mock_multi_host_sync(tmp_path)

        devices = np.array(devices).reshape((4, 2))
        mesh = Mesh(devices, axis_names=("x", "y"))
        shape = (8, 8)

        # Create sharded array
        sharding_save = NamedSharding(mesh, PartitionSpec("x", "y"))
        sharding_restore = NamedSharding(mesh, PartitionSpec("y", "x"))

        w_save = _generate_array_with_sharding(mesh, sharding_save, shape)

        train_state = {"w": w_save}

        storage_context = train.get_context().get_storage()

        manager = JaxCheckpointManager(
            storage_context=storage_context,
        )

        # single step
        manager.save(0, train_state)

        manager.wait_until_finished()

        restore_target = {
            "w": _generate_array_with_sharding(mesh, sharding_restore, shape, value=-1)
        }

        restored, _ = manager.restore(restore_target)

        # Verify values are correct
        # Use a manual check for equality to avoid JAX collectives on the CPU backend
        def check_equal(jax_arr, expected_np_arr):
            for shard in jax_arr.addressable_shards:
                local_data = np.array(shard.data)
                if not np.array_equal(local_data, expected_np_arr[shard.index]):
                    return False
            return True

        expected_data = np.arange(np.prod(shape)).reshape(shape)
        is_equal = check_equal(restored["w"], expected_data)

        # Verify sharding is correct (should match sharding_restore)
        is_resharded = restored["w"].sharding == sharding_restore

        train.report({"is_equal": is_equal, "is_resharded": is_resharded})

    trainer = JaxTrainer(
        train_loop_per_worker=train_func_checkpointing,
        scaling_config=ScalingConfig(
            use_tpu=True,
            accelerator_type="TPU-V4",
            topology="2x2x2",
            num_workers=2,
            resources_per_worker={"TPU": 4},
        ),
        # By default, when running on the CPU backend, JAX only provides 1 virtual device per host.
        # Setting xla_force_host_platform_device_count=4 simulates 4 devices per worker on CPU
        # to match the 4 TPU chips per host in a v4-16 slice.
        run_config=RunConfig(
            storage_path=str(tmp_path),
            callbacks=[CustomMetricsCallback(actor_name)],
            worker_runtime_env={
                "env_vars": {
                    "JAX_PLATFORMS": "cpu",
                    "XLA_FLAGS": "--xla_force_host_platform_device_count=4",
                }
            },
        ),
    )

    result = trainer.fit()
    assert result.error is None

    # Verify reports
    reports = ray.get(verify_actor.get_reports.remote())
    assert len(reports) == 2
    for report in reports:
        assert report["is_equal"] is True
        assert report["is_resharded"] is True


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
