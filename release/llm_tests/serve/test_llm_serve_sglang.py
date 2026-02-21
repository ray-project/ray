import sys

import pytest
from openai import OpenAI

from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.llm.examples.sglang.modules.sglang_engine import SGLangServer
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME
from ray.serve.llm import LLMConfig, build_openai_app
from ray.serve.schema import ApplicationStatus

MODEL_ID = "Qwen/Qwen2.5-0.5B-Instruct"
RAY_MODEL_ID = "qwen-0.5b-sglang"


def _app_is_running():
    try:
        return (
            serve.status().applications[SERVE_DEFAULT_APP_NAME].status
            == ApplicationStatus.RUNNING
        )
    except (KeyError, AttributeError):
        return False


def test_sglang_serve_e2e():
    """Verify the SGLang custom server_cls example works end-to-end with build_openai_app."""
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
            "tp_size": 1,
            "mem_fraction_static": 0.8,
        },
    )

    app = build_openai_app({"llm_configs": [llm_config]})
    serve.run(app, blocking=False)

    wait_for_condition(_app_is_running, timeout=300)
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
    serve.shutdown()


def test_sglang_serve_e2e_multi_gpu():
    """Verify SGLang multi-GPU deployment works with tp_size=2.

    Requires a node with at least 2 GPUs. Confirms that:
    - Placement group bundles are correctly constructed as [{"GPU": 1, "CPU": 1}, {"GPU": 1}]
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

    app = build_openai_app({"llm_configs": [llm_config]})
    serve.run(app, blocking=False)

    wait_for_condition(_app_is_running, timeout=300)

    deployment_options = SGLangServer.get_deployment_options(llm_config)
    expected_bundles = [{"GPU": 1, "CPU": 1}, {"GPU": 1}]
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
    serve.shutdown()


if __name__ == "__main__":
    sys.exit(pytest.main(["-xvs", __file__]))
