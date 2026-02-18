"""Tests for fault-tolerant data parallel LLM serving with zero downtime.

Validates that when a rank or node goes down in a gang-scheduled DP deployment,
the remaining gangs continue serving traffic with no request drops. After recovery,
all gangs serve traffic together.
"""

import threading
import time
from typing import Dict, List

import pytest
import requests

import ray
from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.serve._private.common import DeploymentID, ReplicaState
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME
from ray.serve.context import _get_global_client
from ray.serve.llm import (
    LLMConfig,
    ModelLoadingConfig,
    build_dp_openai_app,
)
from ray.serve.schema import ApplicationStatus

MODEL_ID = "microsoft/Phi-tiny-MoE-instruct"
RAY_URL = "http://localhost:8000"

# How long to send requests before/after failure injection
BASELINE_DURATION_S = 10
STABILIZATION_DURATION_S = 10
REQUEST_INTERVAL_S = 0.5
REQUEST_TIMEOUT_S = 30


@pytest.fixture(autouse=True)
def cleanup():
    yield
    serve.shutdown()
    ray.shutdown()


def is_default_app_running():
    try:
        default_app = serve.status().applications[SERVE_DEFAULT_APP_NAME]
        return default_app.status == ApplicationStatus.RUNNING
    except (KeyError, AttributeError):
        return False


def get_deployment_name() -> str:
    """Find the GangDPServer deployment name from the running app."""
    status = serve.status()
    app = status.applications.get(SERVE_DEFAULT_APP_NAME)
    assert app is not None, "Default app not found"
    for name in app.deployments:
        if "GangDPServer" in name:
            return name
    raise RuntimeError(
        f"Could not find GangDPServer deployment. "
        f"Deployments: {list(app.deployments.keys())}"
    )


def find_replica_actor_names(deployment_name: str) -> List[str]:
    """Find all actor names for replicas of a deployment."""
    actors = ray.util.list_named_actors("serve")
    return [a["name"] for a in actors if deployment_name in a["name"]]


def kill_replica(actor_name: str) -> None:
    actor = ray.get_actor(actor_name, namespace="serve")
    ray.kill(actor)


def find_gang_members(
    deployment_name: str,
    app_name: str = SERVE_DEFAULT_APP_NAME,
) -> Dict[str, List[str]]:
    """Group replica actor names by gang_id.

    Returns:
        Dict mapping gang_id -> list of actor names belonging to that gang.
    """
    controller = _get_global_client()._controller
    deployment_id = DeploymentID(name=deployment_name, app_name=app_name)
    replicas = ray.get(
        controller._dump_replica_states_for_testing.remote(deployment_id)
    )
    running = replicas.get([ReplicaState.RUNNING])

    # Map unique_id -> gang_id
    uid_to_gang = {
        r.replica_id.unique_id: r.gang_context.gang_id for r in running
    }

    # Map actor names to gangs by matching unique_id substrings
    all_actors = ray.util.list_named_actors("serve")
    gangs: Dict[str, List[str]] = {}
    for actor in all_actors:
        name = actor["name"]
        if deployment_name not in name:
            continue
        for uid, gang_id in uid_to_gang.items():
            if uid in name:
                gangs.setdefault(gang_id, []).append(name)
                break

    return gangs


def check_num_replicas_running(deployment_name: str, expected: int) -> bool:
    """Check that the deployment has exactly `expected` RUNNING replicas."""
    try:
        status = serve.status()
        app = status.applications.get(SERVE_DEFAULT_APP_NAME)
        if app is None:
            return False
        dep = app.deployments.get(deployment_name)
        if dep is None:
            return False
        running = dep.replica_states.get("RUNNING", 0)
        return running == expected
    except Exception:
        return False


def wait_for_server_ready(
    url: str,
    model_id: str,
    timeout: int = 300,
    retry_interval: int = 2,
) -> None:
    """Poll the server until it can handle a completion request."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            resp = requests.post(
                f"{url}/v1/completions",
                json={
                    "model": model_id,
                    "prompt": "test",
                    "max_tokens": 5,
                    "temperature": 0,
                },
                timeout=10,
            )
            if resp.status_code == 200:
                return
        except Exception:
            pass
        time.sleep(retry_interval)
    raise TimeoutError(f"Server at {url} not ready within {timeout}s")


class ContinuousRequester:
    """Sends HTTP requests continuously in a background thread, tracking outcomes."""

    def __init__(self, url: str, model_id: str):
        self.url = url
        self.model_id = model_id
        self.successes = 0
        self.failures = 0
        self.errors: List[str] = []
        self._stop = threading.Event()
        self._thread = None

    def start(self):
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=60)

    def _run(self):
        while not self._stop.is_set():
            try:
                resp = requests.post(
                    f"{self.url}/v1/completions",
                    json={
                        "model": self.model_id,
                        "prompt": "The capital of France is",
                        "max_tokens": 5,
                        "temperature": 0.0,
                    },
                    timeout=REQUEST_TIMEOUT_S,
                )
                if resp.status_code == 200:
                    self.successes += 1
                else:
                    self.failures += 1
                    self.errors.append(
                        f"HTTP {resp.status_code}: {resp.text[:200]}"
                    )
            except Exception as e:
                self.failures += 1
                self.errors.append(str(e))
            time.sleep(REQUEST_INTERVAL_S)


def deploy_gang_dp(dp_size: int, num_replicas: int):
    """Deploy a fault-tolerant gang DP app with the given configuration."""
    llm_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(
            model_id=MODEL_ID,
            model_source=MODEL_ID,
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

    app = build_dp_openai_app(
        {"llm_config": llm_config, "enable_fault_tolerance": True}
    )
    serve.run(app, blocking=False)


def test_no_downtime_on_rank_failure():
    """Test no request drops when a single rank goes down.

    Configuration: gang_size=2, num_replicas=4 (2 gangs of 2).

    Kills one rank, causing its gang to restart atomically. The surviving
    gang continues serving all traffic with zero request drops. After
    recovery, both gangs serve traffic together.
    """
    dp_size = 2
    num_replicas = 4

    deploy_gang_dp(dp_size=dp_size, num_replicas=num_replicas)
    wait_for_condition(is_default_app_running, timeout=300)
    wait_for_server_ready(RAY_URL, model_id=MODEL_ID)

    dname = get_deployment_name()

    # Start sending requests continuously
    requester = ContinuousRequester(RAY_URL, MODEL_ID)
    requester.start()

    try:
        # Establish baseline: all requests should succeed
        time.sleep(BASELINE_DURATION_S)
        assert requester.successes > 0, (
            "No successful requests during baseline period"
        )
        assert requester.failures == 0, (
            f"Failures during baseline: {requester.errors}"
        )

        # Kill one replica — triggers its gang (of 2) to restart
        replica_names = find_replica_actor_names(dname)
        assert len(replica_names) == num_replicas, (
            f"Expected {num_replicas} replicas, found {len(replica_names)}"
        )
        print(f"Killing replica: {replica_names[0]}")
        kill_replica(replica_names[0])

        # Wait for the killed gang to fully recover
        wait_for_condition(
            check_num_replicas_running,
            deployment_name=dname,
            expected=num_replicas,
            timeout=300,
        )

        # Let the recovered gang stabilize and verify requests still flow
        time.sleep(STABILIZATION_DURATION_S)
    finally:
        requester.stop()

    # Verify zero request drops throughout the entire test
    assert requester.failures == 0, (
        f"Expected zero failures but got {requester.failures}. "
        f"Errors: {requester.errors[:10]}"
    )
    assert requester.successes > 0, "Expected successful requests"

    # Verify both gangs are serving (all replicas recovered)
    assert check_num_replicas_running(dname, num_replicas), (
        f"Expected {num_replicas} running replicas after recovery"
    )


def test_no_downtime_on_node_failure():
    """Test no request drops when an entire node/gang goes down.

    Configuration: gang_size=4, num_replicas=8 (2 gangs of 4).

    Kills all replicas of one gang (simulating a node failure). The surviving
    gang continues serving all traffic with zero request drops. After
    recovery, both gangs serve traffic together.
    """
    dp_size = 4
    num_replicas = 8

    deploy_gang_dp(dp_size=dp_size, num_replicas=num_replicas)
    wait_for_condition(is_default_app_running, timeout=300)
    wait_for_server_ready(RAY_URL, model_id=MODEL_ID)

    dname = get_deployment_name()

    # Start sending requests continuously
    requester = ContinuousRequester(RAY_URL, MODEL_ID)
    requester.start()

    try:
        # Establish baseline: all requests should succeed
        time.sleep(BASELINE_DURATION_S)
        assert requester.successes > 0, (
            "No successful requests during baseline period"
        )
        assert requester.failures == 0, (
            f"Failures during baseline: {requester.errors}"
        )

        # Identify gangs and kill all replicas of one gang
        gangs = find_gang_members(dname)
        assert len(gangs) == 2, f"Expected 2 gangs, found {len(gangs)}"

        target_gang_id = list(gangs.keys())[0]
        target_replicas = gangs[target_gang_id]
        assert len(target_replicas) == dp_size, (
            f"Expected {dp_size} replicas in gang, found {len(target_replicas)}"
        )

        print(f"Killing all replicas of gang {target_gang_id}: {target_replicas}")
        for actor_name in target_replicas:
            kill_replica(actor_name)

        # Wait for the killed gang to fully recover
        wait_for_condition(
            check_num_replicas_running,
            deployment_name=dname,
            expected=num_replicas,
            timeout=300,
        )

        # Let the recovered gang stabilize and verify requests still flow
        time.sleep(STABILIZATION_DURATION_S)
    finally:
        requester.stop()

    # Verify zero request drops throughout the entire test
    assert requester.failures == 0, (
        f"Expected zero failures but got {requester.failures}. "
        f"Errors: {requester.errors[:10]}"
    )
    assert requester.successes > 0, "Expected successful requests"

    # Verify both gangs are serving (all replicas recovered)
    assert check_num_replicas_running(dname, num_replicas), (
        f"Expected {num_replicas} running replicas after recovery"
    )


if __name__ == "__main__":
    pytest.main(["-xvs", __file__])
