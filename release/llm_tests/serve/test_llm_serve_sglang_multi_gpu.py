"""E2E tests for SGLang deployments that use every GPU on the node.

Each test owns its serve.run/serve.shutdown and gates teardown on GPU memory
clearing so the next deploy does not OOM.
"""

import concurrent.futures
import re
import subprocess
import sys
from typing import List

import pytest
from openai import OpenAI
from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.llm._internal.serve.engines.sglang import SGLangServer
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME
from ray.serve.llm import LLMConfig, build_openai_app
from ray.serve.schema import ApplicationStatus
from ray.util.state import list_actors

MODEL_ID = "Qwen/Qwen2.5-0.5B-Instruct"
RAY_MODEL_ID = "qwen-0.5b-sglang"
# Headroom over the pre-deploy GPU baseline that still counts as "cleared".
_GPU_MEMORY_CLEAR_TOLERANCE_MB = 2000


def _app_is_running():
    try:
        return (
            serve.status().applications[SERVE_DEFAULT_APP_NAME].status
            == ApplicationStatus.RUNNING
        )
    except (KeyError, AttributeError):
        return False


def get_gpu_memory_used_mb() -> List[float]:
    """Return GPU memory used (MB) per device via nvidia-smi."""
    result = subprocess.run(
        ["nvidia-smi", "--query-gpu=memory.used", "--format=csv,noheader,nounits"],
        capture_output=True,
        text=True,
        check=True,
    )
    return [float(x.strip()) for x in result.stdout.strip().split("\n") if x.strip()]


def get_total_gpu_memory_mb() -> float:
    """Return total GPU memory used (MB) across all devices."""
    return sum(get_gpu_memory_used_mb())


def wait_for_gpu_memory_to_clear(threshold_mb: float, timeout: float = 240) -> None:
    """Block until total GPU memory used falls below threshold_mb.

    serve.shutdown() returns before an engine has released its GPU memory (the
    direct-ingress drain keeps the old replica resident for its full graceful
    shutdown window), so a test that redeploys on the same GPUs must wait for the
    previous replica to free memory first or the next deployment OOMs.
    """
    wait_for_condition(
        lambda: get_total_gpu_memory_mb() < threshold_mb,
        timeout=timeout,
        retry_interval_ms=2000,
    )


def _shutdown_and_wait_for_gpu_clear(baseline_mb: float) -> None:
    """Shut Serve down and block until the engine releases its GPU memory.

    serve.shutdown() returns before the replica frees GPU memory (the
    direct-ingress drain keeps the old replica resident for its full graceful
    shutdown window), so a later deployment on the same GPUs OOMs unless we wait
    for used memory to fall back near the pre-deploy baseline first.
    """
    serve.shutdown()
    wait_for_gpu_memory_to_clear(baseline_mb + _GPU_MEMORY_CLEAR_TOLERANCE_MB)


def test_sglang_serve_e2e_multi_gpu():
    """Verify SGLang multi-GPU deployment works with tp_size=2.

    Requires a node with at least 2 GPUs. Confirms that:
    - Placement group bundles pack all GPUs into a single node-sized bundle
      ([{"GPU": 2, "CPU": 1}]) — RayEngine requires one bundle per node.
    - The model loads and serves inference correctly across both GPUs.
    """
    llm_config = LLMConfig(
        model_loading_config={
            "model_id": RAY_MODEL_ID,
            "model_source": MODEL_ID,
        },
        deployment_config={
            "autoscaling_config": {
                "min_replicas": 1,
                "max_replicas": 1,
            }
        },
        server_cls=SGLangServer,
        engine_kwargs={
            "model_path": MODEL_ID,
            "tp_size": 2,
            "mem_fraction_static": 0.8,
        },
    )

    baseline_gpu_mb = get_total_gpu_memory_mb()
    app = build_openai_app({"llm_configs": [llm_config]})
    serve.run(app, blocking=False)

    try:
        wait_for_condition(_app_is_running, timeout=300)

        deployment_options = SGLangServer.get_deployment_options(llm_config)
        expected_bundles = [{"GPU": 2, "CPU": 1}]
        assert deployment_options["placement_group_bundles"] == expected_bundles, (
            f"Expected placement group bundles {expected_bundles}, "
            f"got {deployment_options['placement_group_bundles']}"
        )

        client = OpenAI(base_url="http://localhost:8000/v1", api_key="fake-key")

        chat_resp = client.chat.completions.create(
            model=RAY_MODEL_ID,
            messages=[{"role": "user", "content": "What is the capital of France?"}],
            max_tokens=64,
            temperature=0.0,
        )
        assert chat_resp.choices[0].message.content.strip()

        comp_resp = client.completions.create(
            model=RAY_MODEL_ID,
            prompt="The capital of France is",
            max_tokens=64,
            temperature=0.0,
        )
        assert comp_resp.choices[0].text.strip()
    finally:
        _shutdown_and_wait_for_gpu_clear(baseline_gpu_mb)


def test_sglang_serve_e2e_pipeline_parallel():
    """Verify SGLang multi-GPU deployment works with tp_size=2, pp_size=2.

    Requires a node with at least 4 GPUs. Confirms that:
    - Placement group bundles pack all GPUs into a single node-sized bundle
      ([{"GPU": 4, "CPU": 1}]) — RayEngine assigns every tp/pp rank on a node
      to the same bundle, so the bundle must hold all GPUs for that node.
    - The model loads and serves inference correctly across all 4 GPUs.
    """
    llm_config = LLMConfig(
        model_loading_config={
            "model_id": RAY_MODEL_ID,
            "model_source": MODEL_ID,
        },
        deployment_config={
            "autoscaling_config": {
                "min_replicas": 1,
                "max_replicas": 1,
            }
        },
        server_cls=SGLangServer,
        engine_kwargs={
            "model_path": MODEL_ID,
            "tp_size": 2,
            "pp_size": 2,
            "mem_fraction_static": 0.8,
        },
    )

    baseline_gpu_mb = get_total_gpu_memory_mb()
    app = build_openai_app({"llm_configs": [llm_config]})
    serve.run(app, blocking=False)

    try:
        wait_for_condition(_app_is_running, timeout=300)

        # tp_size=2, pp_size=2 → num_devices=4 → one bundle with all 4 GPUs
        deployment_options = SGLangServer.get_deployment_options(llm_config)
        expected_bundles = [{"GPU": 4, "CPU": 1}]
        assert deployment_options["placement_group_bundles"] == expected_bundles, (
            f"Expected placement group bundles {expected_bundles}, "
            f"got {deployment_options['placement_group_bundles']}"
        )

        client = OpenAI(base_url="http://localhost:8000/v1", api_key="fake-key")

        chat_resp = client.chat.completions.create(
            model=RAY_MODEL_ID,
            messages=[{"role": "user", "content": "What is the capital of France?"}],
            max_tokens=64,
            temperature=0.0,
        )
        assert chat_resp.choices[0].message.content.strip()

        comp_resp = client.completions.create(
            model=RAY_MODEL_ID,
            prompt="The capital of France is",
            max_tokens=64,
            temperature=0.0,
        )
        assert comp_resp.choices[0].text.strip()
    finally:
        _shutdown_and_wait_for_gpu_clear(baseline_gpu_mb)


def test_sglang_serve_e2e_multi_replica():
    """Verify SGLang serves correctly with two replicas.

    Requires a node with at least 2 GPUs. Each replica runs tp_size=1 and owns a
    separate placement group, so sglang names its scheduler actor with a distinct
    `_pg<id>_bundle` suffix and both replicas come up without colliding
    (sgl-project/sglang#22917). Confirms two distinct scheduler placement groups
    are alive and that concurrent requests are served.
    """
    llm_config = LLMConfig(
        model_loading_config={
            "model_id": RAY_MODEL_ID,
            "model_source": MODEL_ID,
        },
        deployment_config={
            "autoscaling_config": {
                "min_replicas": 2,
                "max_replicas": 2,
            }
        },
        server_cls=SGLangServer,
        engine_kwargs={
            "model_path": MODEL_ID,
            "tp_size": 1,
            "mem_fraction_static": 0.8,
        },
    )

    baseline_gpu_mb = get_total_gpu_memory_mb()
    app = build_openai_app({"llm_configs": [llm_config]})
    serve.run(app, blocking=False)

    try:
        wait_for_condition(_app_is_running, timeout=600)

        # sgl-project/sglang#22917 suffixes each scheduler-actor name with its
        # placement-group id, so two replicas yield two distinct ids. Before that
        # fix the second replica reused the first's name and never came up.
        scheduler_pgs = set()
        for actor in list_actors(filters=[("state", "=", "ALIVE")], limit=10000):
            match = re.search(r"_pg([0-9a-f]+)_bundle", actor.name or "")
            if match:
                scheduler_pgs.add(match.group(1))
        assert len(scheduler_pgs) == 2, (
            f"expected 2 distinct sglang scheduler placement groups, got "
            f"{len(scheduler_pgs)}: {scheduler_pgs}"
        )

        client = OpenAI(base_url="http://localhost:8000/v1", api_key="fake-key")

        def _chat(i):
            resp = client.chat.completions.create(
                model=RAY_MODEL_ID,
                messages=[{"role": "user", "content": f"Name city number {i}."}],
                max_tokens=16,
                temperature=0.0,
            )
            return resp.choices[0].message.content.strip()

        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
            answers = list(executor.map(_chat, range(16)))
        assert all(answers), "some concurrent requests returned empty content"
    finally:
        _shutdown_and_wait_for_gpu_clear(baseline_gpu_mb)


if __name__ == "__main__":
    sys.exit(pytest.main(["-xvs", __file__]))
