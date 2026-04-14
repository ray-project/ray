"""
Multi-turn batch inference via Ray Data + ServeDeploymentProcessor.

Deploys an LLM through Ray Serve, then chains two processors that share
the same engine: the first generates a response, the second refines it.
"""

# __serve_deploy_start__
import ray
from ray import serve
from ray.serve.llm import (
    LLMConfig,
    ModelLoadingConfig,
    build_llm_deployment,
)

llm_config = LLMConfig(
    model_loading_config=ModelLoadingConfig(
        model_id="facebook/opt-1.3b",
        model_source="facebook/opt-1.3b",
    ),
    deployment_config=dict(
        autoscaling_config=dict(
            min_replicas=1,
            max_replicas=1,
        ),
    ),
    engine_kwargs=dict(
        enable_prefix_caching=True,
        enable_chunked_prefill=True,
        max_num_batched_tokens=4096,
    ),
)

APP_NAME = "demo_app"
DEPLOYMENT_NAME = "demo_deployment"

llm_app = build_llm_deployment(
    llm_config, override_serve_options=dict(name=DEPLOYMENT_NAME)
)
serve.run(llm_app, name=APP_NAME)
# __serve_deploy_end__

# __serve_processor_config_start__
from ray.data.llm import ServeDeploymentProcessorConfig, build_processor
from ray.serve.llm.openai_api_models import CompletionRequest

config = ServeDeploymentProcessorConfig(
    deployment_name=DEPLOYMENT_NAME,
    app_name=APP_NAME,
    dtype_mapping={
        "CompletionRequest": CompletionRequest,
    },
    concurrency=1,
    batch_size=64,
)
# __serve_processor_config_end__

# __serve_multi_turn_start__
processor1 = build_processor(
    config,
    preprocess=lambda row: dict(
        method="completions",
        dtype="CompletionRequest",
        request_kwargs=dict(
            model="facebook/opt-1.3b",
            prompt=f"This is a prompt for {row['id']}",
            stream=False,
        ),
    ),
    postprocess=lambda row: dict(
        prompt=row["choices"][0]["text"],
    ),
)

processor2 = build_processor(
    config,
    preprocess=lambda row: dict(
        method="completions",
        dtype="CompletionRequest",
        request_kwargs=dict(
            model="facebook/opt-1.3b",
            prompt=row["prompt"],
            stream=False,
        ),
    ),
    postprocess=lambda row: row,
)

ds = ray.data.range(10)
ds = processor2(processor1(ds))
print(ds.take_all())
# __serve_multi_turn_end__

serve.shutdown()
