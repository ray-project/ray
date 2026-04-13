"""
Multimodal batch inference via Ray Data + ServeDeploymentProcessor.

Deploys a vision-language model (Qwen3.5) through Ray Serve, then processes
a dataset of image URLs using ServeDeploymentProcessorConfig.
"""

import ray
from ray import serve
from ray.serve.llm import LLMConfig, build_llm_deployment

APP_NAME = "multimodal_app"
DEPLOYMENT_NAME = "qwen3_5_4b_multimodal_deployment"
MODEL_ID = "qwen3.5-4b"

ASSET_URL = (
    "https://air-example-data.s3.amazonaws.com/rayllm-ossci/assets/cherry_blossom.jpg"
)

# __serve_multimodal_deploy_start__
MODEL_ID = "qwen3.5-4b"

llm_config = LLMConfig(
    model_loading_config=dict(
        model_id=MODEL_ID,
        model_source="Qwen/Qwen3.5-4B",
    ),
    engine_kwargs=dict(
        max_model_len=4096,
        limit_mm_per_prompt={"image": 1},
    ),
    deployment_config=dict(
        num_replicas=1,
    ),
)

llm_app = build_llm_deployment(
    llm_config, override_serve_options=dict(name=DEPLOYMENT_NAME)
)
serve.run(llm_app, name=APP_NAME)
# __serve_multimodal_deploy_end__

ds = ray.data.from_items([{"image_url": ASSET_URL}] * 10)

from ray.data.llm import ServeDeploymentProcessorConfig, build_processor
from ray.serve.llm.openai_api_models import ChatCompletionRequest

# __serve_multimodal_processor_start__
config = ServeDeploymentProcessorConfig(
    deployment_name=DEPLOYMENT_NAME,
    app_name=APP_NAME,
    dtype_mapping={"ChatCompletionRequest": ChatCompletionRequest},
    batch_size=4,
    concurrency=1,
)

processor = build_processor(
    config,
    preprocess=lambda row: dict(
        method="chat",
        dtype="ChatCompletionRequest",
        request_kwargs=dict(
            model=MODEL_ID,
            messages=[
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": "Explain this image"},
                        {"type": "image_url", "image_url": {"url": row["image_url"]}},
                    ],
                }
            ],
            max_tokens=256,
            stream=False,
        ),
    ),
    postprocess=lambda row: dict(
        output=row["choices"][0]["message"]["content"],
    ),
)
# __serve_multimodal_processor_end__

ds = processor(ds)
for row in ds.iter_rows():
    print(f"Output: {row['output']}\n")

serve.shutdown()
