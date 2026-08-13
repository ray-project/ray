import pytest

import ray
import ray.llm._internal.batch.processor.base as processor_base
import ray.llm._internal.common.accelerators as accelerators
from ray.cluster_utils import Cluster


@pytest.fixture(autouse=True)
def capture_accelerator_logs(caplog):
    # ray.llm loggers set propagate=False; attach caplog.handler directly so pytest can capture logs.
    loggers = [accelerators.logger, processor_base.logger]
    for l in loggers:
        l.addHandler(caplog.handler)
    yield
    for l in loggers:
        l.removeHandler(caplog.handler)


@pytest.fixture
def ray_tpu_cluster():
    """Simulates a multi-host TPU v6e-16 slice (4x4 topology / 4 nodes / 4 chips each)."""
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
    yield cluster
    ray.shutdown()
    cluster.shutdown()
