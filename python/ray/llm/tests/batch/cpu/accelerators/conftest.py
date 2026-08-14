import pytest

import ray
import ray.llm._internal.common.accelerators as accelerators
from ray.cluster_utils import Cluster


@pytest.fixture(autouse=True)
def capture_accelerator_logs(caplog):
    """Attach caplog handler to ray.llm logger since propagate=False prevents root propagation."""
    accelerators.logger.addHandler(caplog.handler)
    try:
        yield
    finally:
        accelerators.logger.removeHandler(caplog.handler)


@pytest.fixture
def ray_tpu_cluster():
    """Simulate a multi-host TPU v6e-16 slice (4x4 topology / 4 nodes / 4 chips each)."""
    ray.shutdown()
    cluster = Cluster()
    pod_type = "v6e-16"
    topology = "4x4"
    for i in range(4):
        slice_env = {
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
        resources = {
            "TPU": 4,
            "accelerator_type:TPU-V6E": 4,
        }
        if i == 0:
            resources[f"TPU-{pod_type}-head"] = 1

        cluster.add_node(
            num_cpus=8,
            resources=resources,
            labels=labels,
            env_vars=slice_env,
        )

    ray.init(address=cluster.address)
    try:
        yield cluster
    finally:
        ray.shutdown()
        cluster.shutdown()
