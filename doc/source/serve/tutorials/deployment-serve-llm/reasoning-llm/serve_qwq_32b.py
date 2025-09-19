# serve_qwq_32b.py
from ray.serve.llm import LLMConfig, build_openai_app
import os

llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="my-qwq-32B",
        model_source="Qwen/QwQ-32B",
    ),
    accelerator_type="L40S",
    deployment_config=dict(
        autoscaling_config=dict(
            min_replicas=1,
            max_replicas=2,
        )
    ),
    ### Uncomment if your model is gated and needs your Hugging Face token to access it
    # runtime_env=dict(env_vars={"HF_TOKEN": os.environ.get("HF_TOKEN")}),
    engine_kwargs=dict(
        # 4 GPUs is enough but you can increase tensor_parallel_size to more GPUs for better concurrency.
        tensor_parallel_size=4, max_model_len=32768, reasoning_parser="deepseek_r1"
    ),
)

app = build_openai_app({"llm_configs": [llm_config]})
