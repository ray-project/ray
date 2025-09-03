# serve_qwen_VL.py
from ray.serve.llm import LLMConfig, build_openai_app
import os

llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="my-qwen-VL",
        model_source="qwen/Qwen2.5-VL-7B-Instruct",
    ),
    accelerator_type="L40S",
    deployment_config=dict(
        autoscaling_config=dict(
            min_replicas=2,
            max_replicas=2,
        )
    ),
    engine_kwargs=dict(
        max_model_len=8192,
        ### Uncomment if your model is gated and needs your Hugging Face token to access it.
        # hf_token=os.environ["HF_TOKEN"],
    ),
)

app = build_openai_app({"llm_configs": [llm_config]})
