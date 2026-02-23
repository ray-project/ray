import time

import pytest
from ray import serve
from ray.serve.llm import (
    LLMConfig,
    ModelLoadingConfig,
    build_dp_openai_app,
)

from test_utils import create_openai_client, wait_for_server_ready

MODEL_ID = "microsoft/Phi-tiny-MoE-instruct"
RAY_URL = "http://localhost:8000"


def _deploy_and_query(app, model_id: str, prompt: str) -> str:
    """Deploy a Serve app, query it, and shut down."""
    serve.run(app, blocking=False)
    wait_for_server_ready(RAY_URL, model_id=model_id)

    client = create_openai_client(RAY_URL)
    response = client.completions.create(
        model=model_id,
        prompt=prompt,
        max_tokens=20,
        temperature=0.0,
        seed=42,
    )
    result = response.choices[0].text
    try:
        serve.shutdown()
    except Exception:
        pass
    time.sleep(5)
    return result


def test_llm_serve_gang_data_parallelism_correctness():
    """Test that gang DP produces the same output as non-gang DP."""
    model_loading_config = ModelLoadingConfig(
        model_id=MODEL_ID,
        model_source=MODEL_ID,
    )
    common_engine_kwargs = dict(
        tensor_parallel_size=1,
        pipeline_parallel_size=1,
        distributed_executor_backend="ray",
        max_model_len=1024,
        max_num_seqs=32,
        enforce_eager=True,
    )
    runtime_env = {"env_vars": {"VLLM_DISABLE_COMPILE_CACHE": "1"}}
    placement_group_config = {"bundles": [{"GPU": 1, "CPU": 1}]}
    prompt = "The capital of France is"

    # Non-gang DP
    llm_config_dp = LLMConfig(
        model_loading_config=model_loading_config,
        deployment_config=dict(),
        engine_kwargs={**common_engine_kwargs, "data_parallel_size": 4},
        experimental_configs=dict(dp_size_per_node=4),
        placement_group_config=placement_group_config,
        runtime_env=runtime_env,
    )
    app = build_dp_openai_app({"llm_config": llm_config_dp})
    dp_result = _deploy_and_query(app, MODEL_ID, prompt)

    # Gang DP
    llm_config_gang = LLMConfig(
        model_loading_config=model_loading_config,
        deployment_config=dict(),
        engine_kwargs={**common_engine_kwargs, "data_parallel_size": 4},
        placement_group_config=placement_group_config,
        runtime_env=runtime_env,
    )
    app = build_dp_openai_app(
        {"llm_config": llm_config_gang}
    )
    gang_dp_result = _deploy_and_query(app, MODEL_ID, prompt)

    assert dp_result == gang_dp_result, (
        f"DP vs Gang-DP mismatch:\n"
        f"  dp:       {dp_result!r}\n"
        f"  gang_dp:  {gang_dp_result!r}"
    )


if __name__ == "__main__":
    pytest.main(["-xvs", __file__])
