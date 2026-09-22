import time
from typing import Generator, List, Literal

import pytest
import ray
from ray import serve
from ray._common.test_utils import wait_for_condition
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


@pytest.fixture(name="startup_s", scope="function")
def start_ray_serve(
    tensor_parallel_size: int = 1,
) -> Generator[float, None, None]:
    """Start Ray Serve and yield the startup time in seconds."""
    llm_config: LLMConfig = get_llm_config(tensor_parallel_size)
    app = build_llm_deployment(llm_config, name_prefix="LLM:")
    start = time.time()
    serve.run(app, blocking=False)
    yield time.time() - start
    serve.shutdown()


def wait_for_deployment_status(
    deployment_name: str,
    status: Literal["HEALTHY", "UNHEALTHY"],
    timeout_s: float = 120,
) -> None:
    def check() -> bool:
        print(f"Waiting for deployment {deployment_name} to become {status}")
        state = serve.status()
        return (
            state.applications["default"].deployments[deployment_name].status == status
        )

    wait_for_condition(check, timeout=timeout_s, retry_interval_ms=1000)


def test_recovery_from_replica_failure(startup_s: float) -> None:
    """Tests that the deployment recovers from replica failure."""
    dname = "LLM:test"
    wait_for_deployment_status(dname, "HEALTHY", timeout_s=60)

    # Kill both replicas
    old_replica_ids = find_replica_ids(dname)
    assert len(old_replica_ids) == 2, old_replica_ids
    for replica_id in old_replica_ids:
        print(f"Killing replica {replica_id}")
        kill_replica(replica_id)

    # Start the clock once the actors are gone. UNHEALTHY lags by a health check.
    wait_for_condition(
        lambda: set(old_replica_ids).isdisjoint(find_replica_ids(dname)),
        timeout=60,
        retry_interval_ms=1000,
    )
    recovery_start = time.time()

    # wait for deployment to get unhealthy
    wait_for_deployment_status(dname, "UNHEALTHY", timeout_s=60)

    # Recovery is a cold start of the same replicas. Budget from measured startup.
    recovery_budget_s = max(120, 2 * startup_s)
    wait_for_deployment_status(dname, "HEALTHY", timeout_s=recovery_budget_s)
    recovery_s = time.time() - recovery_start
    print(f"Startup took {startup_s:.1f}s, recovery took {recovery_s:.1f}s")

    new_replica_ids = find_replica_ids(dname)
    assert set(old_replica_ids).isdisjoint(new_replica_ids), (
        old_replica_ids,
        new_replica_ids,
    )


if __name__ == "__main__":
    pytest.main(["-xvs", __file__])
