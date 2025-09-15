# serve_qwen_3_32b.py
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
            min_replicas=1,
            max_replicas=2,
        )
    ),
    ### Uncomment if your model is gated and needs your Hugging Face token to access it.
    # runtime_env=dict(env_vars={"HF_TOKEN": os.environ.get("HF_TOKEN")}),
    engine_kwargs=dict(
        tensor_parallel_size=8, max_model_len=32768, reasoning_parser="qwen3"
    ),
)
app = build_openai_app({"llm_configs": [llm_config]})
