# serve_qwq_32b.py
from ray.serve.llm import LLMConfig, build_openai_app
import os

llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="my-qwq-32B",
        model_source="Qwen/QwQ-32B",
    ),
    accelerator_type="A100-40G",
    deployment_config=dict(
        autoscaling_config=dict(
            min_replicas=2,
            max_replicas=2,
        )
    ),
    engine_kwargs=dict(
        tensor_parallel_size=8,
        max_model_len=32768,
        reasoning_parser="deepseek_r1"
        ### Uncomment if your model is gated and needs your Hugging Face token to access it.
        # hf_token=os.environ["HF_TOKEN"],
    ),
)

app = build_openai_app({"llm_configs": [llm_config]})
