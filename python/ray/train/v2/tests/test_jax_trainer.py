import os
import sys

import pytest

import ray
from ray.tests.conftest import _ray_start_cluster
from ray.train import RunConfig, ScalingConfig, UserCallback
from ray.train.v2._internal.constants import (
    HEALTH_CHECK_INTERVAL_S_ENV_VAR,
    is_v2_enabled,
)
from ray.train.v2.jax import JaxTrainer

assert is_v2_enabled()


@pytest.fixture
def ray_tpu_single_host():
    """Start a mock single-host TPU Ray cluster with 2x4 v6e (8 chips per host)."""
    with _ray_start_cluster() as cluster:
        # Simulate one node with 8 TPU chips.
        cluster.add_node(
            num_cpus=4,
            resources={"TPU": 8, "accelerator_type:TPU-V6E": 1},
            env_vars={"TPU_ACCELERATOR_TYPE": "v6e-8"},
        )

        ray.init(address=cluster.address)

        yield cluster
        ray.shutdown()


@pytest.fixture
def ray_tpu_multi_host():
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

        ray.init(address=cluster.address)
        yield cluster
        ray.shutdown()


@pytest.fixture(autouse=True)
def reduce_health_check_interval(monkeypatch):
    monkeypatch.setenv(HEALTH_CHECK_INTERVAL_S_ENV_VAR, "0.2")
    yield


def train_func():
    import jax

    import ray
    from ray import train

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
        "MEGASCALE_PORT": os.environ.get("MEGASCALE_PORT"),
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
        # When the user does not set MEGASCALE_PORT in their pod spec, Ray
        # Train falls back to the default port from
        # ``get_tpu_coordinator_env_vars``.
        assert r["MEGASCALE_PORT"] == "8081"

    # Validate MEGASCALE_SLICE_ID set based on indexed TPU Pod name.
    slice_a_reports = [r for r in reports if r["slice_name"] == "slice-A"]
    slice_b_reports = [r for r in reports if r["slice_name"] == "slice-B"]

    assert list({r["MEGASCALE_SLICE_ID"] for r in slice_a_reports}) == ["0"]
    assert list({r["MEGASCALE_SLICE_ID"] for r in slice_b_reports}) == ["1"]


def test_tpu_multi_slice_overrides_stale_megascale_env(ray_tpu_multi_host, tmp_path):
    """
    Tests JaxTrainer's MEGASCALE_* env var handling when the underlying TPU
    node provider has already baked values into the pod environment.

    This simulates the multi-slice fault tolerance scenario where one slice was
    preempted and the replacement pods were provisioned by the TPU node
    provider with env vars reflecting a different slice id / total slice count
    than the worker group Ray Train is currently scheduling. Without an
    override, libtpu's megascale topology coordinator would wait for slices
    that no longer exist and hang TPU initialization.
    """
    actor_name = "test_tpu_multi_slice_overrides_stale_megascale_env"
    verify_actor = VerificationActor.options(name=actor_name).remote()

    user_port_override = "9999"

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
            worker_runtime_env={
                "env_vars": {
                    "JAX_PLATFORMS": "cpu",
                    # Stale values, as the TPU node provider would inject on
                    # a freshly-provisioned replacement slice. These should
                    # all be overridden by Ray Train.
                    "MEGASCALE_COORDINATOR_ADDRESS": "stale-coordinator:9999",
                    "MEGASCALE_NUM_SLICES": "3",
                    "MEGASCALE_SLICE_ID": "2",
                    # User-customized port (e.g. set in pod spec to avoid a
                    # conflict with another process). This should be
                    # preserved by Ray Train, NOT overridden.
                    "MEGASCALE_PORT": user_port_override,
                }
            },
        ),
    )
    result = trainer.fit()
    assert result.error is None

    reports = ray.get(verify_actor.get_reports.remote())
    assert (
        len(reports) == 4
    ), f"Expected 4 workers to report metrics, got {len(reports)}"

    # All workers should see the controller-computed coordinator address (the
    # IP of worker 0), NOT the stale "stale-coordinator:9999" value.
    worker_0_report = next(r for r in reports if r["worker_id"] == 0)
    expected_coordinator_ip = worker_0_report["node_ip"]

    for r in reports:
        assert r["MEGASCALE_COORDINATOR_ADDRESS"] == expected_coordinator_ip, (
            "Expected MEGASCALE_COORDINATOR_ADDRESS to be overridden by Ray "
            f"Train, but worker {r['worker_id']} still has stale value "
            f"{r['MEGASCALE_COORDINATOR_ADDRESS']}"
        )
        assert r["MEGASCALE_NUM_SLICES"] == "2", (
            "Expected MEGASCALE_NUM_SLICES to be overridden to the actual "
            f"slice count (2), but worker {r['worker_id']} still has stale "
            f"value {r['MEGASCALE_NUM_SLICES']}"
        )
        assert r["MEGASCALE_PORT"] == user_port_override, (
            "Expected MEGASCALE_PORT to preserve the user-provided pod-spec "
            f"value '{user_port_override}', but worker {r['worker_id']} has "
            f"value '{r['MEGASCALE_PORT']}'. Ray Train should not override "
            "user-customized ports."
        )

    # Validate MEGASCALE_SLICE_ID was overridden per slice (0 / 1) rather than
    # the stale "2" the provider injected on every worker.
    slice_a_reports = [r for r in reports if r["slice_name"] == "slice-A"]
    slice_b_reports = [r for r in reports if r["slice_name"] == "slice-B"]
    assert list({r["MEGASCALE_SLICE_ID"] for r in slice_a_reports}) == ["0"]
    assert list({r["MEGASCALE_SLICE_ID"] for r in slice_b_reports}) == ["1"]


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


def train_func_with_data_multi_host():
    import unittest.mock

    import jax

    import ray
    from ray import train

    train_ctx = train.get_context()
    rank = train_ctx.get_world_rank()

    devices = jax.devices()
    local_devices = jax.local_devices()
    node_labels = ray.get_runtime_context().get_node_labels()
    slice_name = node_labels.get("ray.io/tpu-slice-name")

    ds_shard = train.get_dataset_shard("train")

    batches = []
    # Mock process_allgather because the JAX CPU backend does not support multiprocess communication.
    # Since our mock test distributes exact same data lengths uniformly, all workers report the identical
    # array `[has_batch, local_batch_size]`. We simulate the gather by stacking the local array identically.
    def mock_process_allgather(arr):
        import jax.numpy as jnp

        return jnp.stack([arr] * jax.process_count())

    with unittest.mock.patch(
        "jax.experimental.multihost_utils.process_allgather",
        side_effect=mock_process_allgather,
    ):
        # Each batch has 4 rows. iter_jax_batches should combine the batches from all workers and return a batch of size 4 * num_workers.
        for batch in ds_shard.iter_jax_batches(batch_size=4, synchronize_batches=True):
            batches.append(batch["id"].shape)

    train.report(
        {
            "worker_id": rank,
            "slice_name": slice_name,
            "devices": len(devices),
            "local_devices": len(local_devices),
            "batches": batches,
        }
    )


def test_multi_host_data_iterator(ray_tpu_multi_host, tmp_path):
    actor_name = "test_multi_host_data_iterator"
    verify_actor = VerificationActor.options(name=actor_name).remote()

    import ray

    # Create 32 rows, batch size 4.
    # We have 4 workers. Each custom shard will have 8 rows (2 batches).
    ds = ray.data.range(32)

    trainer = JaxTrainer(
        train_loop_per_worker=train_func_with_data_multi_host,
        scaling_config=ScalingConfig(
            use_tpu=True,
            accelerator_type="TPU-V4",
            topology="2x2x2",
            num_workers=4,
        ),
        datasets={"train": ds},
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

    # Fetch metrics result from each worker using the verification actor.
    reports = ray.get(verify_actor.get_reports.remote())

    # Verify execution of all 4 TPU workers across both slices.
    assert (
        len(reports) == 4
    ), f"Expected 4 workers to report metrics, got {len(reports)}"

    worker_ids = {r["worker_id"] for r in reports}
    assert worker_ids == {0, 1, 2, 3}, "Expected unique worker IDs from 0 to N-1."

    for r in reports:
        # With batch_size=4 and 8 rows per shard, each worker sees 2 batches.
        # jax.devices() will return 16 global devices (CPU).
        assert r["devices"] == 16
        assert r["local_devices"] == 4
        assert len(r["batches"]) == 2

        # Each batch should have global batch size of 16.
        for batch_shape in r["batches"]:
            assert batch_shape == (16,)


def test_single_host_data_iterator_padding(ray_tpu_single_host, tmp_path):
    actor_name = "test_single_host_data_iterator_padding"
    verify_actor = VerificationActor.options(name=actor_name).remote()

    import numpy as np

    import ray

    # Create 44 rows. Batch size 16.
    # 44 / 16 = 2 batches of 16, and 1 batch of 12.
    # With paddings, we expect 3 batches of 16.
    ds = ray.data.from_items([{"features": np.ones((8,))} for _ in range(44)])

    def train_func():
        from ray import train

        ds_shard = train.get_dataset_shard("train")
        batches = []
        for batch in ds_shard.iter_jax_batches(
            batch_size=16,
            paddings=-1,
        ):
            batches.append(batch["features"].shape)
        train.report({"batches": batches})

    trainer = JaxTrainer(
        train_loop_per_worker=train_func,
        scaling_config=ScalingConfig(
            use_tpu=True,
            num_workers=1,
            resources_per_worker={"TPU": 8},
            accelerator_type="TPU-V6E",
        ),
        datasets={"train": ds},
        run_config=RunConfig(
            storage_path=str(tmp_path),
            callbacks=[CustomMetricsCallback(actor_name)],
            worker_runtime_env={
                "env_vars": {
                    "JAX_PLATFORMS": "cpu",
                    "XLA_FLAGS": "--xla_force_host_platform_device_count=8",
                }
            },
        ),
    )
    result = trainer.fit()
    assert result.error is None

    reports = ray.get(verify_actor.get_reports.remote())
    assert len(reports) == 1
    assert len(reports[0]["batches"]) == 3
    for shape in reports[0]["batches"]:
        assert shape == (16, 8)


def test_single_host_data_iterator_dtypes(ray_tpu_single_host, tmp_path):
    actor_name = "test_single_host_data_iterator_dtypes"
    verify_actor = VerificationActor.options(name=actor_name).remote()

    import numpy as np

    import ray

    ds = ray.data.from_items([{"features": np.ones((8,))} for _ in range(32)])

    def train_func():
        import jax.numpy as jnp

        from ray import train

        ds_shard = train.get_dataset_shard("train")
        batches = []
        for batch in ds_shard.iter_jax_batches(
            batch_size=16,
            dtypes=jnp.float16,
        ):
            assert batch["features"].dtype == jnp.float16
            batches.append(batch["features"].shape)
        train.report({"batches": batches})

    trainer = JaxTrainer(
        train_loop_per_worker=train_func,
        scaling_config=ScalingConfig(
            use_tpu=True,
            num_workers=1,
            resources_per_worker={"TPU": 8},
            accelerator_type="TPU-V6E",
        ),
        datasets={"train": ds},
        run_config=RunConfig(
            storage_path=str(tmp_path),
            callbacks=[CustomMetricsCallback(actor_name)],
            worker_runtime_env={
                "env_vars": {
                    "JAX_PLATFORMS": "cpu",
                    "XLA_FLAGS": "--xla_force_host_platform_device_count=8",
                }
            },
        ),
    )
    result = trainer.fit()
    assert result.error is None

    reports = ray.get(verify_actor.get_reports.remote())
    assert len(reports) == 1
    assert len(reports[0]["batches"]) == 2
    for shape in reports[0]["batches"]:
        assert shape == (16, 8)


def train_func_with_data_single_host(config):
    import jax
    import numpy as np
    from jax.sharding import Mesh, NamedSharding, PartitionSpec as P

    from ray import train

    devices = jax.devices()
    local_devices = jax.local_devices()

    # We requested 8 TPU resources per worker, so XLA_FLAGS should expose 8 mock local devices
    assert len(devices) == 8
    assert len(local_devices) == 8

    # 8 local devices across a 2x4 mesh
    mesh = Mesh(np.array(local_devices).reshape(2, 4), ("x", "y"))
    named_sharding = NamedSharding(mesh, P("x", "y"))

    ds_shard = train.get_dataset_shard("train")

    batches = []
    drop_last = config.get("drop_last", False) if config else False
    # Local batch size must be evenly divisible by 8 (num_local_devices)
    for batch in ds_shard.iter_jax_batches(
        batch_size=16,
        drop_last=drop_last,
    ):
        arr = jax.device_put(batch["features"], named_sharding)
        assert arr.sharding == named_sharding
        batches.append(arr.shape)

    train.report(
        {
            "devices": len(devices),
            "local_devices": len(local_devices),
            "batches": batches,
        }
    )


def test_single_host_data_iterator_2d(ray_tpu_single_host, tmp_path):
    actor_name = "test_single_host_data_iterator_2d"
    verify_actor = VerificationActor.options(name=actor_name).remote()

    import numpy as np

    import ray

    # Create 48 rows. Each row is {"features": np.ones((8,))}
    # We have 1 worker. The worker processes 48 rows in batches of 16.
    # The first 3 batches are (16, 8) in shape.
    ds = ray.data.from_items([{"features": np.ones((8,))} for _ in range(48)])

    trainer = JaxTrainer(
        train_loop_per_worker=train_func_with_data_single_host,
        scaling_config=ScalingConfig(
            use_tpu=True,
            num_workers=1,
            resources_per_worker={"TPU": 8},
            accelerator_type="TPU-V6E",
        ),
        datasets={"train": ds},
        run_config=RunConfig(
            storage_path=str(tmp_path),
            callbacks=[CustomMetricsCallback(actor_name)],
            # Mock 8 local CPU devices exclusively for this worker process.
            # This enables jax.device_put to successfully execute local 2D resharding
            # completely in-memory because jax.process_count() defaults to 1.
            worker_runtime_env={
                "env_vars": {
                    "JAX_PLATFORMS": "cpu",
                    "XLA_FLAGS": "--xla_force_host_platform_device_count=8",
                }
            },
        ),
    )
    result = trainer.fit()
    assert result.error is None

    reports = ray.get(verify_actor.get_reports.remote())
    assert len(reports) == 1

    for r in reports:
        assert r["devices"] == 8
        assert r["local_devices"] == 8
        assert len(r["batches"]) == 3

        for batch_shape in r["batches"]:
            assert batch_shape == (16, 8)


def test_single_host_data_iterator_2d_truncation(ray_tpu_single_host, tmp_path):
    actor_name = "test_single_host_data_iterator_2d_truncation"
    verify_actor = VerificationActor.options(name=actor_name).remote()

    import numpy as np

    import ray

    # Create 60 rows. Each row is {"features": np.ones((8,))}
    # 60 is not divisible by 16, so the last 12 rows should be truncated.
    # Thus, each local batch is (16, 8) in shape and we expect exactly 3 batches.
    ds = ray.data.from_items([{"features": np.ones((8,))} for _ in range(60)])

    trainer = JaxTrainer(
        train_loop_per_worker=train_func_with_data_single_host,
        train_loop_config={"drop_last": True},
        scaling_config=ScalingConfig(
            use_tpu=True,
            num_workers=1,
            resources_per_worker={"TPU": 8},
            accelerator_type="TPU-V6E",
        ),
        datasets={"train": ds},
        run_config=RunConfig(
            storage_path=str(tmp_path),
            callbacks=[CustomMetricsCallback(actor_name)],
            # Mock 8 local CPU devices exclusively for this worker process.
            # This enables jax.device_put to successfully execute local 2D resharding
            # completely in-memory because jax.process_count() defaults to 1.
            worker_runtime_env={
                "env_vars": {
                    "JAX_PLATFORMS": "cpu",
                    "XLA_FLAGS": "--xla_force_host_platform_device_count=8",
                }
            },
        ),
    )
    result = trainer.fit()
    assert result.error is None

    reports = ray.get(verify_actor.get_reports.remote())
    assert len(reports) == 1

    for r in reports:
        assert r["devices"] == 8
        assert r["local_devices"] == 8
        assert len(r["batches"]) == 3

        for batch_shape in r["batches"]:
            assert batch_shape == (16, 8)


def test_single_host_data_iterator_2d_truncation_failure(ray_tpu_single_host, tmp_path):
    import numpy as np

    import ray

    # Create 60 rows. Each row is {"features": np.ones((8,))}
    # 60 is not divisible by 16. So if drop_last=False (the default), it should raise a ValueError.
    ds = ray.data.from_items([{"features": np.ones((8,))} for _ in range(60)])

    trainer = JaxTrainer(
        train_loop_per_worker=train_func_with_data_single_host,
        scaling_config=ScalingConfig(
            use_tpu=True,
            num_workers=1,
            resources_per_worker={"TPU": 8},
            accelerator_type="TPU-V6E",
        ),
        datasets={"train": ds},
        run_config=RunConfig(
            storage_path=str(tmp_path),
            worker_runtime_env={
                "env_vars": {
                    "JAX_PLATFORMS": "cpu",
                    "XLA_FLAGS": "--xla_force_host_platform_device_count=8",
                }
            },
        ),
    )
    with pytest.raises(
        Exception, match="evenly divisible by the number of local JAX devices"
    ):
        trainer.fit()


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


def train_func_with_collate_fn(config):
    import numpy as np

    from ray import train
    from ray.data.collate_fn import NumpyBatchCollateFn

    class CustomCollateFn(NumpyBatchCollateFn):
        def __call__(self, batch):
            # Combine "col1" and "col2" columns into a single "features" tensor
            return np.stack((batch["col1"], batch["col2"]), axis=1)

    ds_shard = train.get_dataset_shard("train")

    batches = []
    for batch in ds_shard.iter_jax_batches(
        batch_size=16,
        collate_fn=CustomCollateFn(),
    ):
        # The output of collate_fn is now a single jax.Array (global sharding)
        # Check a specific value: it should be [1.0, 2.0] at the first position
        assert np.array_equal(batch[0], np.array([1.0, 2.0]))
        batches.append(batch.shape)

    train.report(
        {
            "batches": batches,
        }
    )


def test_single_host_data_iterator_collate_fn(ray_tpu_single_host, tmp_path):
    actor_name = "test_single_host_data_iterator_collate_fn"
    verify_actor = VerificationActor.options(name=actor_name).remote()

    import ray

    # Create 32 rows. Each row has "col1" (ones) and "col2" (twos).
    ds = ray.data.from_items([{"col1": 1.0, "col2": 2.0} for _ in range(32)])

    trainer = JaxTrainer(
        train_loop_per_worker=train_func_with_collate_fn,
        scaling_config=ScalingConfig(
            use_tpu=True,
            num_workers=1,
            resources_per_worker={"TPU": 8},
            accelerator_type="TPU-V6E",
        ),
        datasets={"train": ds},
        run_config=RunConfig(
            storage_path=str(tmp_path),
            callbacks=[CustomMetricsCallback(actor_name)],
            worker_runtime_env={
                "env_vars": {
                    "JAX_PLATFORMS": "cpu",
                    "XLA_FLAGS": "--xla_force_host_platform_device_count=8",
                }
            },
        ),
    )
    result = trainer.fit()
    assert result.error is None

    reports = ray.get(verify_actor.get_reports.remote())
    assert len(reports) == 1

    for r in reports:
        # We expect 2 batches (32 rows / 16 batch_size)
        # Each batch should have shape (16, 2)
        assert len(r["batches"]) == 2
        for batch_shape in r["batches"]:
            assert batch_shape == (16, 2)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
