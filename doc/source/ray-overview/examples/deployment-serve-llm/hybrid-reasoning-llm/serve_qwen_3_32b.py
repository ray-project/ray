# serve_qwen_3_32b.py
from ray import serve
from ray.serve.llm import LLMConfig, build_openai_app
import os

llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="my-qwen-3-32b",
        model_source="Qwen/Qwen3-32B",
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
        reasoning_parser="qwen3",
        ### Uncomment if your model is gated and need your Huggingface Token to access it
        # hf_token=os.environ["HF_TOKEN"],
    ),
)
app = build_openai_app({"llm_configs": [llm_config]})
