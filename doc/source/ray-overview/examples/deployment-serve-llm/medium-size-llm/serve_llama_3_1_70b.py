#serve_llama_3_1_70b.py
from ray import serve
from ray.serve.llm import LLMConfig, build_openai_app
import os

llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="my-llama-3.1-70b",
        # Or Qwen/Qwen2.5-72B-Instruct for an ungated model
        model_source="meta-llama/Llama-3.1-70B-Instruct",
    ),
    accelerator_type="A100-40G",
    deployment_config=dict(
        autoscaling_config=dict(
            min_replicas=2, max_replicas=4,
        )
    ),
    engine_kwargs=dict(
        max_model_len=32768,
        ### If your model is not gated, you can skip `hf_token`
        # Share your Hugging Face Token to the vllm engine so it can access the gated Llama 3
        hf_token=os.environ["HF_TOKEN"],
        # Split weights among 8 GPUs in the node
        tensor_parallel_size=8
    ),
)

app = build_openai_app({"llm_configs": [llm_config]})