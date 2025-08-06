import pytest

from ray.llm._internal.serve.deployments.llm.vllm.vllm_engine import VLLMEngine
from ray.serve.llm import LLMConfig, ModelLoadingConfig


@pytest.mark.asyncio
async def test_vllm_engine_start_with_custom_resource_bundle(
    # defined in conftest.py
    model_smolvlm_256m,
):
    """vLLM engine starts with custom resource bundle."""
    llm_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(
            model_id="smolvlm-256m",
            model_source=model_smolvlm_256m,
        ),
        engine_kwargs=dict(
            gpu_memory_utilization=0.4,
            use_tqdm_on_load=False,
            enforce_eager=True,
        ),
        resources_per_bundle=dict(GPU=0.49),
        runtime_env=dict(
            env_vars={
                "VLLM_RAY_PER_WORKER_GPUS": "0.49",
                "VLLM_DISABLE_COMPILE_CACHE": "1",
            },
        ),
    )

    engine = VLLMEngine(llm_config)
    await engine.start()
    await engine.check_health()
    engine.shutdown()
