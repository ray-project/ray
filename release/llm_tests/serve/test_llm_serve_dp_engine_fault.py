"""Test DP deployment fault tolerance when a vLLM GPU worker process is killed.

Finds a RayWorkerWrapper actor (the GPU worker process spun up by vLLM's
ray distributed backend) and kills it to simulate a DP replica failure:

    worker killed → compiled DAG error → EngineCore fatal error
    → engine_dead flag set → check_health() raises
    → Serve RESTART_GANG tears down the gang

The test verifies that the surviving DP replicas continue serving requests
and the faulty gang is fully replaced.
"""

import asyncio
import os
import signal
import time

import pytest

import ray
from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME
from ray.serve.llm import LLMConfig, ModelLoadingConfig, build_dp_deployment
from ray.serve.schema import ApplicationStatus, ReplicaState
from ray.util.state import list_actors
from vllm.entrypoints.openai.completion.protocol import CompletionRequest


@pytest.fixture(autouse=True)
def cleanup():
    yield
    serve.shutdown()
    ray.shutdown()


def is_default_app_running():
    try:
        app = serve.status().applications[SERVE_DEFAULT_APP_NAME]
        return app.status == ApplicationStatus.RUNNING
    except (KeyError, AttributeError):
        return False


def get_num_running_replicas(deployment_name: str) -> int:
    """Get the number of RUNNING replicas for a deployment."""
    dep = (
        serve.status().applications[SERVE_DEFAULT_APP_NAME].deployments[deployment_name]
    )
    return dep.replica_states.get(ReplicaState.RUNNING, 0)


def find_vllm_worker_actors():
    """Find all alive RayWorkerWrapper actors created by vLLM's ray distributed executor backend."""
    actors = list_actors(
        filters=[
            ("class_name", "=", "RayWorkerWrapper"),
            ("state", "=", "ALIVE"),
        ]
    )
    return actors


def kill_one_vllm_worker():
    """Kill one RayWorkerWrapper actor by sending SIGKILL to its process.

    Returns the node_id of the killed worker so we can verify which gang gets torn down.
    """
    actors = find_vllm_worker_actors()
    assert len(actors) > 0, "No alive RayWorkerWrapper actors found"

    target = actors[0]
    target_pid = target.pid
    target_node_id = target.node_id

    @ray.remote(num_cpus=0)
    def kill_pid(pid):
        os.kill(pid, signal.SIGKILL)
        return f"Killed pid {pid}"

    result = ray.get(
        kill_pid.options(
            scheduling_strategy=ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                node_id=target_node_id, soft=False
            ),
        ).remote(target_pid)
    )
    return target_node_id, result


def test_llm_serve_dp_engine_fault():
    """DP deployment recovers when a vLLM GPU worker process is killed."""
    deployment_name = "DPServer:microsoft--Phi-tiny-MoE-instruct"
    dp_size = 2
    num_replicas = 2
    expected_serve_replicas = num_replicas * dp_size

    llm_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(
            model_id="microsoft/Phi-tiny-MoE-instruct",
            model_source="microsoft/Phi-tiny-MoE-instruct",
        ),
        deployment_config=dict(
            num_replicas=num_replicas,
            health_check_period_s=1,
            health_check_timeout_s=3,
        ),
        engine_kwargs=dict(
            tensor_parallel_size=1,
            pipeline_parallel_size=1,
            data_parallel_size=dp_size,
            distributed_executor_backend="ray",
            enable_expert_parallel=True,
            max_model_len=1024,
            max_num_seqs=32,
            enforce_eager=True,
        ),
        runtime_env={
            "env_vars": {
                "VLLM_DISABLE_COMPILE_CACHE": "1",
                # Shorter compiled graph timeout so the dead worker is detected quickly.
                "RAY_CGRAPH_get_timeout": "30",
            },
        },
    )

    handle = serve.run(build_dp_deployment(llm_config), blocking=False)
    wait_for_condition(is_default_app_running, timeout=180)

    # Step 1: Verify steady state — all replicas running.
    assert get_num_running_replicas(deployment_name) == expected_serve_replicas

    # Verify RayWorkerWrapper actors exist.
    workers_before = find_vllm_worker_actors()
    assert len(workers_before) > 0

    # Step 2: Start continuous request sending, then kill a GPU worker.
    @ray.remote
    class RequestSender:
        def __init__(self, h):
            self._handle = h.options(stream=True)
            self.total = 0
            self.errors = []
            self._stop = False

        async def send(self, prompt="test", max_tokens=1, ignore_errors=False):
            req = CompletionRequest(
                model="microsoft/Phi-tiny-MoE-instruct",
                prompt=prompt,
                max_tokens=max_tokens,
            )
            try:
                async for _ in self._handle.completions.remote(req):
                    pass
                self.total += 1
            except Exception as e:
                if not ignore_errors:
                    self.errors.append(str(e))
                self.total += 1

        async def run(self):
            while not self._stop:
                await self.send(prompt="Hello, world!", max_tokens=5)
                await asyncio.sleep(0.1)

        def stop(self):
            self._stop = True

        def get_results(self):
            return self.total, self.errors

    sender = RequestSender.remote(handle)

    # Send a few warm-up requests to confirm the deployment works.
    for _ in range(5):
        sender.send.remote(ignore_errors=True)
        time.sleep(0.5)

    # Kill one RayWorkerWrapper GPU worker process.
    killed_node_id, kill_msg = kill_one_vllm_worker()
    print(f"Killed vLLM worker on node {killed_node_id}: {kill_msg}")

    # Step 3: Wait for the faulty gang to be torn down.
    # Detection: compiled DAG timeout (~30s) + health check overhead.
    wait_for_condition(
        lambda: get_num_running_replicas(deployment_name) < expected_serve_replicas,
        timeout=60,
    )

    # Step 4: Verify the surviving replicas continue serving requests.
    time.sleep(2)
    sender.run.remote()

    # Step 5: Wait for full recovery — all replicas back to RUNNING.
    wait_for_condition(
        lambda: get_num_running_replicas(deployment_name) == expected_serve_replicas,
        timeout=120,
    )

    # Step 6: Assert zero downtime — requests should have kept flowing
    # through the surviving gang during recovery.
    ray.get(sender.stop.remote())
    total, errors = ray.get(sender.get_results.remote())
    assert total > 0, "Expected at least one successful request"
    assert len(errors) == 0, f"Expected zero errors, got {errors}"


if __name__ == "__main__":
    pytest.main(["-v", __file__])
