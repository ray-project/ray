"""Test DP deployment fault tolerance when a vLLM engine worker hangs.

Uses a vLLM plugin that monkey-patches the MoE expert-parallel all-to-all
primitives (AgRsAll2AllManager.dispatch/combine) to simulate a NCCL hang.
A Ray named actor (``fault_signal``) coordinates which worker hangs:

    worker hang → compiled DAG timeout → EngineCore fatal error
    → engine_dead flag set → check_health() raises
    → Serve RESTART_GANG tears down the gang

The test sets RAY_CGRAPH_get_timeout to a short value so the compiled DAG
detects the hung worker quickly.
"""

import asyncio
import pathlib
import subprocess
import sys
import tempfile
import time
from collections import defaultdict

import pytest

import ray
from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.serve._private.common import DeploymentID, ReplicaState
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME
from ray.serve.llm import LLMConfig, ModelLoadingConfig, build_dp_deployment
from ray.serve.schema import ApplicationStatus
from vllm.entrypoints.openai.completion.protocol import CompletionRequest

PLUGIN_DIR = pathlib.Path(__file__).parent / "plugins" / "vllm_fault"


@ray.remote(num_cpus=0)
class FaultSignal:
    """Coordinates fault injection across worker processes."""

    def __init__(self):
        self._armed = False
        self._target_node_id = None
        self._claimed = False

    def arm(self, target_node_id: str):
        """Arm the fault for workers on target_node_id."""
        self._armed = True
        self._target_node_id = target_node_id
        self._claimed = False

    def check_and_claim(self, worker_node_id: str):
        """Check whether worker_node_id should hang.

        Args:
            worker_node_id: The node ID of the worker to check.

        Returns:
            A tuple containing:
            - should_hang (bool): True for the first caller on the armed node.
            - stop_checking (bool): True once the caller can skip future checks.
        """
        if not self._armed:
            return False, False
        if worker_node_id != self._target_node_id:
            return False, True
        if self._claimed:
            return False, True
        self._claimed = True
        return True, True

    def disarm(self):
        """Disarm the fault and reset the claim."""
        self._armed = False
        self._target_node_id = None
        self._claimed = False


@pytest.fixture(scope="session", autouse=True)
def install_fault_plugin():
    """Install the vLLM fault-injection plugin on every node."""
    with tempfile.TemporaryDirectory() as tmpdir:
        subprocess.check_call(
            [sys.executable, "-m", "pip", "wheel", "--no-deps", "-w", tmpdir,
             str(PLUGIN_DIR)],
        )
        whl_files = list(pathlib.Path(tmpdir).glob("*.whl"))
        assert len(whl_files) == 1
        whl_bytes = whl_files[0].read_bytes()
        whl_name = whl_files[0].name

    ray.init()
    whl_ref = ray.put(whl_bytes)
    node_ids = [n["NodeID"] for n in ray.nodes() if n["Alive"]]

    @ray.remote(num_cpus=0)
    def install_plugin(whl_data, filename):
        with tempfile.TemporaryDirectory() as d:
            whl_path = pathlib.Path(d) / filename
            whl_path.write_bytes(whl_data)
            subprocess.check_call(
                [sys.executable, "-m", "pip", "install", "--force-reinstall",
                 str(whl_path)],
            )

    ray.get(
        [
            install_plugin.options(
                scheduling_strategy=ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                    node_id=node_id, soft=False
                ),
            ).remote(whl_ref, whl_name)
            for node_id in node_ids
        ]
    )
    ray.shutdown()


@pytest.fixture(autouse=True)
def cleanup():
    yield
    try:
        signal = ray.get_actor("fault_signal", namespace="fault_test")
        ray.kill(signal)
    except (ValueError, Exception):
        pass

    serve.shutdown()
    ray.shutdown()

def is_default_app_running():
    try:
        app = serve.status().applications[SERVE_DEFAULT_APP_NAME]
        return app.status == ApplicationStatus.RUNNING
    except (KeyError, AttributeError):
        return False


def test_llm_serve_dp_engine_fault():
    """DP deployment recovers from an engine-level A2A communication hang.

    The plugin hangs one worker's all-to-all op. The compiled DAG times
    out (RAY_CGRAPH_get_timeout), causing EngineCore to die and Serve to
    tear down and replace the faulty gang.
    """
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
                # Shorter compiled graph timeout so the hang is detected earlier.
                "RAY_CGRAPH_get_timeout": "30",
            },
        },
    )

    handle = serve.run(build_dp_deployment(llm_config), blocking=False)
    wait_for_condition(is_default_app_running, timeout=180)

    deployment_id = DeploymentID(name=deployment_name, app_name=SERVE_DEFAULT_APP_NAME)
    controller = serve.context._global_client._controller

    # Phase 1: Verify steady state
    replicas = ray.get(controller._dump_replica_states_for_testing.remote(deployment_id))
    running = replicas.get([ReplicaState.RUNNING])
    assert len(running) == expected_serve_replicas

    gangs = defaultdict(list)
    for r in running:
        assert r.gang_context is not None
        gangs[r.gang_context.gang_id].append(r)
    assert len(gangs) == num_replicas

    original_replica_ids = {r.replica_id.unique_id for r in running}

    # Phase 2: Arm the fault only on one gang's node
    target_gang_id = next(iter(gangs))
    target_node_id = gangs[target_gang_id][0].actor_node_id

    signal = FaultSignal.options(name="fault_signal", namespace="fault_test").remote()
    ray.get(signal.arm.remote(target_node_id))

    # Send requests until one routes to the armed gang.
    # MoE dispatch/combine hits the patched function and hangs the worker.
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
    for _ in range(5):
        sender.send.remote(ignore_errors=True)
        time.sleep(0.5)

    # Phase 3: Wait for the faulty gang to be torn down
    def gang_teardown_detected():
        reps = ray.get(
            controller._dump_replica_states_for_testing.remote(deployment_id)
        )
        r = reps.get([ReplicaState.RUNNING])
        return len(r) < expected_serve_replicas

    # Detection takes ~30s (compiled graph timeout) + health check overhead.
    wait_for_condition(gang_teardown_detected, timeout=60)

    # Disarm so replacement replicas start healthy.
    ray.get(signal.disarm.remote())

    # Phase 4: Verify the surviving gang serves with zero errors
    # Brief pause so the router finishes draining the dying replicas.
    time.sleep(2)
    sender.run.remote()

    # Phase 5: Wait for full recovery
    def all_replicas_recovered():
        reps = ray.get(
            controller._dump_replica_states_for_testing.remote(deployment_id)
        )
        r = reps.get([ReplicaState.RUNNING])
        if len(r) != expected_serve_replicas:
            return False
        current_ids = {rep.replica_id.unique_id for rep in r}
        new_ids = current_ids - original_replica_ids
        return len(new_ids) == dp_size

    wait_for_condition(all_replicas_recovered, timeout=120)

    # Phase 6: Assert zero downtime
    ray.get(sender.stop.remote())
    total, errors = ray.get(sender.get_results.remote())
    assert total > 0, "Expected at least one successful request"
    assert len(errors) == 0, f"Expected zero errors, got {errors}"


if __name__ == "__main__":
    pytest.main(["-v", __file__])
