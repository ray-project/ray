import pytest

import ray
from ray.tests.conftest import _ray_start_cluster


@pytest.fixture
def llm_config_with_mock_engine(llm_config):
    # Make sure engine is mocked.
    if llm_config.runtime_env is None:
        llm_config.runtime_env = {}
    llm_config.runtime_env.setdefault("env_vars", {})[
        "RAYLLM_VLLM_ENGINE_CLS"
    ] = "ray.llm.tests.serve.mocks.mock_vllm_engine.MockVLLMEngine"
    yield llm_config


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
