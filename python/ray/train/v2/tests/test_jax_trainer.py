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


@pytest.fixture(scope="module")
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
