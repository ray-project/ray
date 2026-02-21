import time
from typing import Literal, List, Generator

import pytest
import ray
from ray import serve
from ray.serve.llm import LLMConfig, ModelLoadingConfig, build_llm_deployment

MODEL_ID = "Qwen/Qwen2.5-0.5B-Instruct"
RAY_MODEL_ID = "qwen-0.5b"


def get_llm_config(
    tensor_parallel_size: int = 1,
) -> LLMConfig:
    """Create LLMConfig with specified parallelism parameters."""
    return LLMConfig(
        model_loading_config=ModelLoadingConfig(
            model_id=RAY_MODEL_ID,
            model_source=MODEL_ID,
        ),
        deployment_config=dict(
            name="test",
            num_replicas=2,
        ),
        engine_kwargs=dict(
            tensor_parallel_size=tensor_parallel_size,
            enforce_eager=True,
        ),
        runtime_env={"env_vars": {"VLLM_USE_V1": "1"}},
    )


def find_replica_ids(deployment_name: str) -> List[str]:
    actors = ray.util.list_named_actors("serve")
    found_replica_ids = []
    for actor in actors:
        if deployment_name in actor["name"]:
            found_replica_ids.append(actor["name"])
    return found_replica_ids


def kill_replica(replica_id: str) -> None:
    actor = ray.get_actor(replica_id, namespace="serve")
    ray.kill(actor)


@pytest.fixture(name="app", scope="function")
def start_ray_serve(
    tensor_parallel_size: int = 1,
) -> Generator:
    """Start Ray Serve with specified parallelism parameters."""
    llm_config: LLMConfig = get_llm_config(tensor_parallel_size)
    app = build_llm_deployment(llm_config, name_prefix="LLM:")
    serve.run(app, blocking=False)
    yield app
    serve.shutdown()


def wait_for_deployment_status(
    deployment_name: str, status: Literal["HEALTHY", "UNHEALTHY"], timeout_s: int = 120
) -> None:
    s = time.time()
    while time.time() - s < timeout_s:
        print(f"Waiting for deployment {deployment_name} to become {status}")
        state = serve.status()
        if state.applications["default"].deployments[deployment_name].status == status:
            return
        time.sleep(1)
    raise TimeoutError(
        f"Deployment {deployment_name} did not become "
        f"{status} within {timeout_s} seconds"
    )


def test_recovery_from_replica_failure(app) -> None:
    """Tests that the deployment recovers from replica failure."""
    dname = "LLM:test"
    wait_for_deployment_status(dname, "HEALTHY", timeout_s=60)

    # Kill both replicas
    replica_ids = find_replica_ids(dname)
    for replica_id in replica_ids:
        print(f"Killing replica {replica_id}")
        kill_replica(replica_id)

    # wait for deployment to get unhealthy
    wait_for_deployment_status(dname, "UNHEALTHY", timeout_s=60)

    # Wait again for deployment to get healthy
    wait_for_deployment_status(dname, "HEALTHY", timeout_s=60)


if __name__ == "__main__":
    pytest.main(["-xvs", __file__])
