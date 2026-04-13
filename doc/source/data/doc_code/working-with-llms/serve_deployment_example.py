"""
Multi-turn batch inference via Ray Data + ServeDeploymentProcessor.

Deploys an LLM through Ray Serve, then chains two processors that share
the same engine: the first generates a response, the second refines it.
"""

# __serve_deploy_start__
import ray
from ray import serve
from ray.serve.llm import LLMConfig, build_llm_deployment

APP_NAME = "llm_app"
DEPLOYMENT_NAME = "llm_deployment"
MODEL_ID = "facebook/opt-1.3b"

llm_config = LLMConfig(
    model_loading_config=dict(
        model_id=MODEL_ID,
        model_source="facebook/opt-1.3b",
    ),
    engine_kwargs=dict(
        enable_prefix_caching=True,
        enable_chunked_prefill=True,
        max_num_batched_tokens=4096,
    ),
    deployment_config=dict(
        autoscaling_config=dict(min_replicas=1, max_replicas=1),
    ),
)

llm_app = build_llm_deployment(
    llm_config, override_serve_options=dict(name=DEPLOYMENT_NAME)
)
serve.run(llm_app, name=APP_NAME)
# __serve_deploy_end__

# __serve_processor_config_start__
from ray.data.llm import ServeDeploymentProcessorConfig, build_processor
from ray.serve.llm.openai_api_models import ChatCompletionRequest

config = ServeDeploymentProcessorConfig(
    deployment_name=DEPLOYMENT_NAME,
    app_name=APP_NAME,
    dtype_mapping={"ChatCompletionRequest": ChatCompletionRequest},
    batch_size=64,
    concurrency=1,
)
# __serve_processor_config_end__

# __serve_multi_turn_start__
# Turn 1: generate an initial response from the prompt
processor1 = build_processor(
    config,
    preprocess=lambda row: dict(
        method="chat",
        dtype="ChatCompletionRequest",
        request_kwargs=dict(
            model=MODEL_ID,
            messages=[{"role": "user", "content": row["prompt"]}],
            max_tokens=64,
            stream=False,
        ),
    ),
    postprocess=lambda row: dict(
        prompt=row["prompt"],
        first_response=row["choices"][0]["message"]["content"],
    ),
)

# Turn 2: feed the first response back for refinement
processor2 = build_processor(
    config,
    preprocess=lambda row: dict(
        method="chat",
        dtype="ChatCompletionRequest",
        request_kwargs=dict(
            model=MODEL_ID,
            messages=[
                {"role": "user", "content": row["prompt"]},
                {"role": "assistant", "content": row["first_response"]},
                {"role": "user", "content": "Summarize your response in one sentence."},
            ],
            max_tokens=64,
            stream=False,
        ),
    ),
    postprocess=lambda row: dict(
        prompt=row["prompt"],
        first_response=row["first_response"],
        summary=row["choices"][0]["message"]["content"],
    ),
)

ds = ray.data.from_items(
    [{"prompt": f"Explain concept {i} in machine learning."} for i in range(10)]
)
ds = processor1(ds)
ds = processor2(ds)
for row in ds.iter_rows():
    print(row)
# __serve_multi_turn_end__

serve.shutdown()
