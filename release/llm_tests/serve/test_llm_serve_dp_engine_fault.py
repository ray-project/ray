"""Test DP deployment fault tolerance when a vLLM engine worker crashes.

Uses a vLLM plugin that monkey-patches the MoE expert-parallel all-to-all
primitives (AgRsAll2AllManager.dispatch/combine) to simulate a NCCL failure
and triggers the following chain:

    worker crash → EngineCore detects failure → engine_dead flag set
    → check_health() raises → Serve RESTART_GANG tears down the gang
"""

import asyncio
import pathlib
import subprocess
import sys
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
SIGNAL_FILE = "/tmp/a2a_fault_signal"


# ── fixtures ────────────────────────────────────────────────────────


@pytest.fixture(scope="session", autouse=True)
def install_fault_plugin():
    """pip-install the vLLM A2A fault-injection plugin on every node.

    Builds a wheel on the head node, puts it in the Ray object store, and
    installs from the wheel on every node (including head) so that
    RayWorkerWrapper actors also load the plugin.
    """
    import tempfile

    # Build a wheel from the plugin source directory (head node only).
    with tempfile.TemporaryDirectory() as tmpdir:
        subprocess.check_call(
            [sys.executable, "-m", "pip", "wheel", "--no-deps", "-w", tmpdir,
             str(PLUGIN_DIR)],
        )
        # Find the built wheel.
        whl_files = list(pathlib.Path(tmpdir).glob("*.whl"))
        assert len(whl_files) == 1, f"Expected 1 wheel, got {whl_files}"
        whl_bytes = whl_files[0].read_bytes()
        whl_name = whl_files[0].name

    # Put the wheel bytes into the Ray object store so every node can access it.
    ray.init()
    whl_ref = ray.put(whl_bytes)
    node_ids = [n["NodeID"] for n in ray.nodes() if n["Alive"]]

    @ray.remote(num_cpus=0)
    def _install_plugin(whl_data, filename):
        import subprocess as sp
        import sys as _sys
        import tempfile as tf

        with tf.TemporaryDirectory() as d:
            whl_path = pathlib.Path(d) / filename
            whl_path.write_bytes(whl_data)
            sp.check_call(
                [_sys.executable, "-m", "pip", "install", "--force-reinstall",
                 str(whl_path)],
            )

    ray.get(
        [
            _install_plugin.options(
                scheduling_strategy=ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                    node_id=nid, soft=False
                ),
            ).remote(whl_ref, whl_name)
            for nid in node_ids
        ]
    )
    ray.shutdown()


@pytest.fixture(autouse=True)
def cleanup():
    yield
    _delete_signal_all_nodes()
    serve.shutdown()
    ray.shutdown()


# ── signal-file helpers (work across nodes) ─────────────────────────


@ray.remote(num_cpus=0)
def _create_signal():
    open(SIGNAL_FILE, "w").close()


@ray.remote(num_cpus=0)
def _delete_signal():
    import os

    try:
        os.remove(SIGNAL_FILE)
    except FileNotFoundError:
        pass


def _get_alive_node_ids():
    return [n["NodeID"] for n in ray.nodes() if n["Alive"]]


def _create_signal_all_nodes():
    node_ids = _get_alive_node_ids()
    ray.get(
        [
            _create_signal.options(
                scheduling_strategy=ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                    node_id=nid, soft=False
                ),
            ).remote()
            for nid in node_ids
        ]
    )


def _delete_signal_all_nodes():
    try:
        node_ids = _get_alive_node_ids()
        ray.get(
            [
                _delete_signal.options(
                    scheduling_strategy=ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                        node_id=nid, soft=False
                    ),
                ).remote()
                for nid in node_ids
            ]
        )
    except Exception:
        pass


# ── helpers ─────────────────────────────────────────────────────────


def _is_default_app_running():
    try:
        app = serve.status().applications[SERVE_DEFAULT_APP_NAME]
        return app.status == ApplicationStatus.RUNNING
    except (KeyError, AttributeError):
        return False


# ── test ────────────────────────────────────────────────────────────


def test_llm_serve_dp_engine_fault():
    """DP deployment recovers from an engine-level A2A communication crash.

    Cluster: 2 nodes × 4 GPUs = 8 GPUs.
    Deployment: 2 DP groups × dp_size 2 = 4 Serve replicas.
    Model: microsoft/Phi-tiny-MoE-instruct with expert parallelism so
           the MoE layers trigger AgRsAll2AllManager.dispatch/combine.
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
            health_check_timeout_s=5,
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
            },
        },
    )

    handle = serve.run(build_dp_deployment(llm_config), blocking=False)
    wait_for_condition(_is_default_app_running, timeout=300)

    deployment_id = DeploymentID(
        name=deployment_name, app_name=SERVE_DEFAULT_APP_NAME
    )
    controller = serve.context._global_client._controller

    # ── Phase 1: verify steady state (4 RUNNING replicas, 2 gangs) ──
    replicas = ray.get(
        controller._dump_replica_states_for_testing.remote(deployment_id)
    )
    running = replicas.get([ReplicaState.RUNNING])
    assert len(running) == expected_serve_replicas

    gangs = defaultdict(list)
    for r in running:
        assert r.gang_context is not None
        gangs[r.gang_context.gang_id].append(r)
    assert len(gangs) == num_replicas

    original_replica_ids = {r.replica_id.unique_id for r in running}

    # ── Phase 2: arm the fault on ONE gang's node only ──
    # Each gang is packed onto a single node (PACK placement strategy).
    # We pick one gang and create the signal file ONLY on its node so
    # that the other gang's workers never see the signal and stay healthy.
    target_gang_id = next(iter(gangs))
    target_node_id = gangs[target_gang_id][0].actor_node_id

    ray.get(
        _create_signal.options(
            scheduling_strategy=ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                node_id=target_node_id, soft=False
            ),
        ).remote()
    )

    # Fire-and-forget: send requests until one routes to the armed gang.
    # The MoE dispatch/combine hits the patched function and kills the
    # worker process.
    @ray.remote
    class _TriggerActor:
        def __init__(self, h):
            self._handle = h.options(stream=True)

        async def send(self):
            req = CompletionRequest(
                model="microsoft/Phi-tiny-MoE-instruct",
                prompt="test",
                max_tokens=1,
            )
            try:
                async for _ in self._handle.completions.remote(req):
                    pass
            except Exception:
                pass  # Expected — the worker will crash

    trigger = _TriggerActor.remote(handle)
    for _ in range(3):
        trigger.send.remote()
        time.sleep(0.5)

    # ── Phase 3: wait for the faulty gang to be torn down ──
    def gang_teardown_detected():
        reps = ray.get(
            controller._dump_replica_states_for_testing.remote(deployment_id)
        )
        r = reps.get([ReplicaState.RUNNING])
        return len(r) < expected_serve_replicas

    wait_for_condition(gang_teardown_detected, timeout=60)

    # Disarm so replacement replicas start healthy.
    _delete_signal_all_nodes()  # clean all nodes in case of rescheduling

    # ── Phase 4: verify the surviving gang serves with zero errors ──
    # Brief pause so the router finishes draining the dying replicas.
    time.sleep(2)

    @ray.remote
    class RequestSender:
        def __init__(self, h):
            self._handle = h.options(stream=True)
            self.total = 0
            self.errors = []
            self._stop = False

        async def run(self):
            req = CompletionRequest(
                model="microsoft/Phi-tiny-MoE-instruct",
                prompt="Hello, world!",
                max_tokens=5,
            )
            while not self._stop:
                try:
                    async for _ in self._handle.completions.remote(req):
                        pass
                    self.total += 1
                except Exception as e:
                    self.errors.append(str(e))
                    self.total += 1
                await asyncio.sleep(0.1)

        def stop(self):
            self._stop = True

        def get_results(self):
            return self.total, self.errors

    sender = RequestSender.remote(handle)
    sender.run.remote()

    # ── Phase 5: wait for full recovery ──
    def all_replicas_recovered():
        reps = ray.get(
            controller._dump_replica_states_for_testing.remote(deployment_id)
        )
        r = reps.get([ReplicaState.RUNNING])
        if len(r) != expected_serve_replicas:
            return False
        current_ids = {rep.replica_id.unique_id for rep in r}
        new_ids = current_ids - original_replica_ids
        # The faulty gang (dp_size replicas) must have been replaced.
        return len(new_ids) >= dp_size

    wait_for_condition(all_replicas_recovered, timeout=180)

    # ── Phase 6: assert zero dropped requests ──
    ray.get(sender.stop.remote())
    total, errors = ray.get(sender.get_results.remote())
    assert total > 0, "Expected at least one successful request"
    assert len(errors) == 0, f"Expected zero errors, got {errors}"


if __name__ == "__main__":
    pytest.main(["-v", __file__])
