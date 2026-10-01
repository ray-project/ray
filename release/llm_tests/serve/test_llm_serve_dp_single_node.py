"""Test several DP gangs sharing a single GPU node.

Cluster: 1 node x 4 GPUs. Two DP=2 gangs fill the node, so their DP masters,
ports, and per-rank bundles have to coexist on the same host.
"""

from collections import defaultdict

import pytest

import ray
from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.serve._private.common import DeploymentID, ReplicaState
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME
from ray.serve.llm import LLMConfig, ModelLoadingConfig, build_dp_deployment
from ray.serve.schema import ApplicationStatus

from utils import shutdown_serve_and_wait_for_controller


@pytest.fixture(autouse=True)
def cleanup_ray_resources():
    """Automatically cleanup Ray resources between tests to prevent conflicts."""
    yield
    shutdown_serve_and_wait_for_controller()
    ray.shutdown()


def is_default_app_running():
    """Check if the default application is running successfully."""
    try:
        default_app = serve.status().applications[SERVE_DEFAULT_APP_NAME]
        return default_app.status == ApplicationStatus.RUNNING
    except (KeyError, AttributeError):
        return False


def test_llm_serve_data_parallelism_multi_group_single_node():
    """Two DP gangs run side by side on one node."""
    deployment_name = "DPServer:microsoft--Phi-tiny-MoE-instruct"
    dp_size = 2
    num_replicas = 2

    llm_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(
            model_id="microsoft/Phi-tiny-MoE-instruct",
            model_source="microsoft/Phi-tiny-MoE-instruct",
        ),
        deployment_config=dict(num_replicas=num_replicas),
        engine_kwargs=dict(
            tensor_parallel_size=1,
            pipeline_parallel_size=1,
            data_parallel_size=dp_size,
            distributed_executor_backend="ray",
            max_model_len=1024,
            max_num_seqs=32,
            enforce_eager=True,
        ),
        placement_group_config={"bundles": [{"GPU": 1, "CPU": 1}]},
        runtime_env={"env_vars": {"VLLM_DISABLE_COMPILE_CACHE": "1"}},
    )

    serve.run(build_dp_deployment(llm_config), blocking=False)
    wait_for_condition(is_default_app_running, timeout=300)

    deployment_id = DeploymentID(name=deployment_name, app_name=SERVE_DEFAULT_APP_NAME)
    controller = serve.context._global_client._controller
    replicas = ray.get(
        controller._dump_replica_states_for_testing.remote(deployment_id)
    )
    running = replicas.get([ReplicaState.RUNNING])
    assert len(running) == num_replicas * dp_size

    gangs = defaultdict(list)
    for r in running:
        assert r.gang_context is not None
        gangs[r.gang_context.gang_id].append(r)
    assert len(gangs) == num_replicas

    # Both gangs must share the one GPU node.
    node_ids = {r.actor_node_id for r in running}
    assert len(node_ids) == 1, f"Expected all replicas on one node, got {node_ids}"


if __name__ == "__main__":
    pytest.main(["-v", __file__])
