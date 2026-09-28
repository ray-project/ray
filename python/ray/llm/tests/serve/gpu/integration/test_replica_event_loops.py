import json
import sys

import openai
import pytest
import requests
from prometheus_client.parser import text_string_to_metric_families

import ray
from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.serve.llm import (
    LLMConfig,
    LLMServingArgs,
    ModelLoadingConfig,
    build_openai_app,
)

METRICS_URL = "http://127.0.0.1:9999/metrics"
EVENT_LOOP_METRIC = "ray_serve_event_loop_monitoring_iterations_total"


def _get_replica_event_loop_samples():
    response = requests.get(METRICS_URL, timeout=5)
    response.raise_for_status()

    samples = []
    for family in text_string_to_metric_families(response.text):
        samples.extend(
            sample
            for sample in family.samples
            if sample.name == EVENT_LOOP_METRIC
            and sample.labels.get("component") == "replica"
            and sample.labels.get("application") == "default"
        )
    return samples


def test_default_replica_event_loops_are_consolidated(
    model_llama_3_2_216M, shutdown_ray_and_serve
):
    ray.init(
        num_cpus=8,
        num_gpus=1,
        include_dashboard=False,
        _metrics_export_port=9999,
        _system_config={"metrics_report_interval_ms": 1000},
    )

    model_id = "llama-216m"
    config = LLMConfig(
        model_loading_config=ModelLoadingConfig(
            model_id=model_id,
            model_source=model_llama_3_2_216M,
        ),
        engine_kwargs={
            "enforce_eager": True,
            "gpu_memory_utilization": 0.4,
            "max_model_len": 1024,
            "use_tqdm_on_load": False,
        },
        deployment_config={"num_replicas": 1},
    )
    serve.run(build_openai_app(LLMServingArgs(llm_configs=[config])))

    client = openai.OpenAI(
        base_url="http://127.0.0.1:8000/v1",
        api_key="test",
        timeout=60,
    )
    completion = client.completions.create(
        model=model_id,
        prompt="Hello",
        max_tokens=2,
    )
    assert completion.choices[0].text

    # The request initializes OpenAiIngress's model handle. Wait for both actual
    # replicas to publish more than one heartbeat before checking for extra loops.
    def replica_main_loops_have_reported():
        main_samples = [
            sample
            for sample in _get_replica_event_loop_samples()
            if sample.labels.get("loop_type") == "main" and sample.value >= 2
        ]
        deployment_names = {sample.labels["deployment"] for sample in main_samples}
        return "OpenAiIngress" in deployment_names and any(
            name.startswith("LLMServer:") for name in deployment_names
        )

    wait_for_condition(replica_main_loops_have_reported, timeout=90)

    samples = _get_replica_event_loop_samples()
    main_samples = [
        sample for sample in samples if sample.labels.get("loop_type") == "main"
    ]
    actor_to_deployment = {
        sample.labels["actor_id"]: sample.labels["deployment"]
        for sample in main_samples
        if sample.labels["deployment"] == "OpenAiIngress"
        or sample.labels["deployment"].startswith("LLMServer:")
    }
    loops_by_deployment = {
        deployment: sorted(
            {
                sample.labels["loop_type"]
                for sample in samples
                if sample.labels["actor_id"] == actor_id
            }
        )
        for actor_id, deployment in actor_to_deployment.items()
    }
    print(f"Replica event loops: {json.dumps(loops_by_deployment, sort_keys=True)}")

    assert set(loops_by_deployment) == {
        "OpenAiIngress",
        f"LLMServer:{model_id}",
    }
    assert all(loop_types == ["main"] for loop_types in loops_by_deployment.values())


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
