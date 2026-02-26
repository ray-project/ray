import pathlib
from collections import defaultdict
import asyncio
import time

import pytest
import yaml

import ray
from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.experimental.internal_kv import _internal_kv_list
from ray.llm._internal.serve.serving_patterns.data_parallel.dp_server import (
    GangMasterInfoRegistry,
)
from ray.serve._private.common import DeploymentID, ReplicaState
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME, SERVE_NAMESPACE
from ray.serve.llm import (
    build_dp_deployment,
    build_dp_openai_app,
    build_pd_openai_app,
    build_openai_app,
    LLMConfig,
    LLMServingArgs,
    ModelLoadingConfig,
)
from ray.serve.schema import ApplicationStatus
from vllm.entrypoints.openai.completion.protocol import CompletionRequest

CONFIGS_DIR = pathlib.Path(__file__).parent / "configs"


@pytest.fixture(autouse=True)
def cleanup_ray_resources():
    """Automatically cleanup Ray resources between tests to prevent conflicts."""
    yield
    serve.shutdown()
    ray.shutdown()


def is_default_app_running():
    """Check if the default application is running successfully."""
    try:
        default_app = serve.status().applications[SERVE_DEFAULT_APP_NAME]
        return default_app.status == ApplicationStatus.RUNNING
    except (KeyError, AttributeError):
        return False


@pytest.mark.parametrize(
    "tp_size,pp_size",
    [
        (2, 4),  # TPxPP=8 > 4 GPUs/node, FORCES cross-node placement
        (4, 2),  # TPxPP=8 > 4 GPUs/node, FORCES cross-node placement
    ],
)
def test_llm_serve_multi_node(tp_size, pp_size):
    """Test multi-node Ray Serve LLM deployment with custom placement groups.

    Cluster: 2 nodes x 4 GPUs = 8 total GPUs
    TPxPP=8 exceeds per-node capacity, forcing cross-node deployment.
    """
    total_gpus = tp_size * pp_size
    placement_group_config = {
        "bundles": [{"GPU": 1, "CPU": 1}] * total_gpus,
        "strategy": "PACK",
    }

    llm_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(
            model_id="opt-1.3b",
            model_source="facebook/opt-1.3b",
        ),
        deployment_config=dict(
            autoscaling_config=dict(
                min_replicas=1,
                max_replicas=1,
            ),
        ),
        engine_kwargs=dict(
            tensor_parallel_size=tp_size,
            pipeline_parallel_size=pp_size,
            distributed_executor_backend="ray",
            max_model_len=512,
            max_num_batched_tokens=256,
            enforce_eager=True,
        ),
        placement_group_config=placement_group_config,
        runtime_env={"env_vars": {"VLLM_DISABLE_COMPILE_CACHE": "1"}},
    )

    app = build_openai_app(llm_serving_args=LLMServingArgs(llm_configs=[llm_config]))
    serve.run(app, blocking=False)

    wait_for_condition(is_default_app_running, timeout=300)


@pytest.mark.parametrize(
    "tp_size,dp_size,num_replicas,placement_group_config",
    [
        # TP=1 cases
        (1, 4, None, {"bundles": [{"GPU": 1, "CPU": 1}]}),  # Single group, single node
        (1, 8, None, {"bundles": [{"GPU": 1, "CPU": 1}]}),  # Single group, multi-node
        (1, 2, 2, {"bundles": [{"GPU": 1, "CPU": 1}]}),  # Multi-group, single node
        (1, 4, 2, {"bundles": [{"GPU": 1, "CPU": 1}]}),  # Multi-group, multi-node
        # TP=2 cases — auto-generates correct bundles from TP size
        (2, 2, 1, None),  # TP, single group, single node
        (2, 2, 2, None),  # TP, multi-group, multi-node
        # TP=2 cases — explicit placement_group_config with 2 bundles for TP=2
        (2, 2, None, {"bundles": [{"GPU": 1, "CPU": 1}, {"GPU": 1}]}),
        (2, 2, 2, {"bundles": [{"GPU": 1, "CPU": 1}, {"GPU": 1}]}),
    ],
)
def test_llm_serve_data_parallelism(
    tp_size, dp_size, num_replicas, placement_group_config
):
    """Test Data Parallelism deployment."""
    deployment_config = dict()
    if num_replicas is not None:
        deployment_config["num_replicas"] = num_replicas

    llm_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(
            model_id="microsoft/Phi-tiny-MoE-instruct",
            model_source="microsoft/Phi-tiny-MoE-instruct",
        ),
        deployment_config=deployment_config,
        engine_kwargs=dict(
            tensor_parallel_size=tp_size,
            pipeline_parallel_size=1,
            data_parallel_size=dp_size,
            distributed_executor_backend="ray",
            max_model_len=1024,
            max_num_seqs=32,
            enforce_eager=True,
        ),
        placement_group_config=placement_group_config,
        runtime_env={"env_vars": {"VLLM_DISABLE_COMPILE_CACHE": "1"}},
    )

    app = build_dp_deployment(llm_config)
    serve.run(app, blocking=False)

    wait_for_condition(is_default_app_running, timeout=300)


def test_llm_serve_data_parallelism_autoscaling():
    """Test that DP deployment with autoscaling scales up under load."""
    dp_size = 2
    deployment_name = "DPServer:microsoft--Phi-tiny-MoE-instruct"

    deployment_config = {
        "num_replicas": "auto",
        "autoscaling_config": dict(
            min_replicas=1,
            max_replicas=2,
            upscale_delay_s=0.1,
            downscale_delay_s=5,
            metrics_interval_s=1,
            look_back_period_s=2,
            target_ongoing_requests=1,
        ),
        "max_ongoing_requests": 20,
    }

    llm_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(
            model_id="microsoft/Phi-tiny-MoE-instruct",
            model_source="microsoft/Phi-tiny-MoE-instruct",
        ),
        deployment_config=deployment_config,
        engine_kwargs=dict(
            tensor_parallel_size=2,
            pipeline_parallel_size=1,
            data_parallel_size=dp_size,
            distributed_executor_backend="ray",
            max_model_len=1024,
            max_num_seqs=32,
            enforce_eager=True,
        ),
        runtime_env={"env_vars": {"VLLM_DISABLE_COMPILE_CACHE": "1"}},
    )

    app = build_dp_deployment(llm_config)
    handle = serve.run(app, blocking=False)

    wait_for_condition(is_default_app_running, timeout=300)

    def check_num_replicas_eq(target):
        dep = (
            serve.status()
            .applications[SERVE_DEFAULT_APP_NAME]
            .deployments[deployment_name]
        )
        running = dep.replica_states.get("RUNNING", 0)
        assert running == target
        return True

    def get_total_replicas():
        dep = (
            serve.status()
            .applications[SERVE_DEFAULT_APP_NAME]
            .deployments[deployment_name]
        )
        return sum(dep.replica_states.values())

    def check_total_replicas_gte(target):
        assert get_total_replicas() >= target
        return True

    wait_for_condition(check_num_replicas_eq, target=2, timeout=60)

    request = CompletionRequest(
        model="microsoft/Phi-tiny-MoE-instruct",
        prompt="Write a very long detailed story about",
        max_tokens=1024,
    )
    # Send enough concurrent requests to trigger upscaling
    streaming_handle = handle.options(stream=True)
    results = [streaming_handle.completions.remote(request) for _ in range(20)]

    wait_for_condition(check_total_replicas_gte, target=4, timeout=120)

    total = get_total_replicas()
    assert total % dp_size == 0

    # Drain requests by consuming streaming responses
    for res in results:
        for _ in res:
            pass

    # After requests drain, verify scale-down back to min_replicas
    wait_for_condition(check_num_replicas_eq, target=2, timeout=120)

    dep = (
        serve.status()
        .applications[SERVE_DEFAULT_APP_NAME]
        .deployments[deployment_name]
    )
    running = dep.replica_states.get("RUNNING", 0)
    assert running % dp_size == 0


def test_llm_serve_data_parallelism_cleanup():
    """Test that Data Parallelism KV entries are cleaned up on shutdown."""
    placement_group_config = {
        "bundles": [{"GPU": 1, "CPU": 1}],
    }

    llm_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(
            model_id="microsoft/Phi-tiny-MoE-instruct",
            model_source="microsoft/Phi-tiny-MoE-instruct",
        ),
        deployment_config=dict(),
        engine_kwargs=dict(
            tensor_parallel_size=1,
            pipeline_parallel_size=1,
            data_parallel_size=4,
            distributed_executor_backend="ray",
            max_model_len=1024,
            max_num_seqs=32,
            enforce_eager=True,
        ),
        placement_group_config=placement_group_config,
        runtime_env={"env_vars": {"VLLM_DISABLE_COMPILE_CACHE": "1"}},
    )

    app = build_dp_deployment(llm_config)
    serve.run(app, blocking=False)
    wait_for_condition(is_default_app_running, timeout=300)

    master_keys = _internal_kv_list(GangMasterInfoRegistry._KEY_PREFIX)
    assert len(master_keys) > 0

    serve.shutdown()


def test_llm_serve_data_parallelism_declarative():
    """Test Data Parallelism deployment via declarative config."""
    config_path = CONFIGS_DIR / "serve_phi_tiny_moe_dp4_gang.yaml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    app_config = config["applications"][0]
    app = build_dp_openai_app(app_config["args"])
    serve.run(app, blocking=False)

    wait_for_condition(is_default_app_running, timeout=300)


def test_llm_serve_prefill_decode_with_data_parallelism():
    """Test Prefill-Decode disaggregation with Data Parallelism and Expert Parallelism.

    Cluster: 2 nodes x 4 GPUs = 8 GPUs total
    - Prefill: DP=4 (scheduled on node with "prefill" custom resource)
    - Decode: DP=4 (scheduled on node with "decode" custom resource)

    Note: This test requires RAY_SERVE_USE_COMPACT_SCHEDULING_STRATEGY=1 to be set
    (configured in release_tests.yaml). Without this flag, Serve uses the default
    SPREAD scheduling strategy, which will prevent DP replicas from being colocated.
    """
    model_loading_config = ModelLoadingConfig(
        model_id="deepseek",
        model_source="deepseek-ai/DeepSeek-V2-Lite",
    )
    base_engine_kwargs = {
        "tensor_parallel_size": 1,
        "enable_expert_parallel": True,
        "load_format": "dummy",
        "max_model_len": 512,
        "max_num_batched_tokens": 256,
        "enforce_eager": True,
    }

    prefill_config = LLMConfig(
        model_loading_config=model_loading_config,
        engine_kwargs={
            **base_engine_kwargs,
            "data_parallel_size": 4,
            "kv_transfer_config": {
                "kv_connector": "NixlConnector",
                "kv_role": "kv_both",
            },
        },
        experimental_configs={
            "NIXL_SIDE_CHANNEL_PORT_BASE": 40000,  # Prefill port range
        },
        runtime_env={"env_vars": {"VLLM_DISABLE_COMPILE_CACHE": "1"}},
    )

    decode_config = LLMConfig(
        model_loading_config=model_loading_config,
        engine_kwargs={
            **base_engine_kwargs,
            "data_parallel_size": 4,
            "kv_transfer_config": {
                "kv_connector": "NixlConnector",
                "kv_role": "kv_both",
            },
        },
        experimental_configs={
            "NIXL_SIDE_CHANNEL_PORT_BASE": 41000,  # Decode port range (different)
        },
        runtime_env={"env_vars": {"VLLM_DISABLE_COMPILE_CACHE": "1"}},
    )

    # build_pd_openai_app auto-detects DP and uses build_dp_deployment
    app = build_pd_openai_app(
        {
            "prefill_config": prefill_config,
            "decode_config": decode_config,
        }
    )
    serve.run(app, blocking=False)

    wait_for_condition(is_default_app_running, timeout=300)


def test_llm_serve_data_parallelism_worker_fault():
    """Test DP deployment recovers after replica death."""

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
            num_replicas=2,
            health_check_period_s=1,
            health_check_timeout_s=5,
        ),
        engine_kwargs=dict(
            tensor_parallel_size=1,
            pipeline_parallel_size=1,
            data_parallel_size=dp_size,
            distributed_executor_backend="ray",
            max_model_len=1024,
            max_num_seqs=32,
            enforce_eager=True,
        ),
        runtime_env={"env_vars": {"VLLM_DISABLE_COMPILE_CACHE": "1"}},
    )
    handle = serve.run(build_dp_deployment(llm_config), blocking=False)
    wait_for_condition(is_default_app_running, timeout=300)

    deployment_id = DeploymentID(name=deployment_name, app_name=SERVE_DEFAULT_APP_NAME)
    controller = serve.context._global_client._controller

    # Verify 4 running replicas in 2 gangs.
    replicas = ray.get(controller._dump_replica_states_for_testing.remote(deployment_id))
    running = replicas.get([ReplicaState.RUNNING])
    assert len(running) == expected_serve_replicas

    gangs = defaultdict(list)
    for r in running:
        assert r.gang_context is not None
        gangs[r.gang_context.gang_id].append(r)
    assert len(gangs) == 2

    # Pick one gang and kill one of its replicas.
    target_gang_id = list(gangs.keys())[0]
    victim = gangs[target_gang_id][0]
    victim_actor = ray.get_actor(victim.replica_id.to_full_id_str(), namespace=SERVE_NAMESPACE)
    original_replica_ids = {r.replica_id.unique_id for r in running}

    # Background request sender
    @ray.remote
    class RequestSender:
        def __init__(self, handle):
            self._handle = handle.options(stream=True)
            self.total = 0
            self.errors = []
            self._stop = False

        async def run(self):
            request = CompletionRequest(
                model="microsoft/Phi-tiny-MoE-instruct",
                prompt="Hello, world!",
                max_tokens=5,
            )
            while not self._stop:
                try:
                    async for _ in self._handle.completions.remote(request):
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
    time.sleep(1)

    # Kill the victim replica
    ray.kill(victim_actor, no_restart=False)

    # Wait for the killed gang to be replaced
    def all_replicas_recovered():
        replicas = ray.get(
            controller._dump_replica_states_for_testing.remote(deployment_id)
        )
        running = replicas.get([ReplicaState.RUNNING])
        if len(running) != expected_serve_replicas:
            return False
        current_ids = {r.replica_id.unique_id for r in running}
        new_ids = current_ids - original_replica_ids
        return len(new_ids) == dp_size

    wait_for_condition(all_replicas_recovered, timeout=180)

    # Stop sender and verify no requests were dropped
    ray.get(sender.stop.remote())
    total, errors = ray.get(sender.get_results.remote())
    assert total > 0
    assert len(errors) == 0


if __name__ == "__main__":
    pytest.main(["-v", __file__])
