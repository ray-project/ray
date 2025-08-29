# serve_llama_3_1_70b.py
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
            min_replicas=1,
            max_replicas=4,
        )
    ),
    ### If your model is not gated, you can skip `hf_token`
    # Share your Hugging Face Token to the vllm engine so it can access the gated Llama 3
    # Type `export HF_TOKEN=<YOUR-HUGGINGFACE-TOKEN>` in a terminal
    runtime_env=dict(env_vars={"HF_TOKEN": os.environ.get("HF_TOKEN")}),
    engine_kwargs=dict(
        max_model_len=32768,
        # Split weights among 8 GPUs in the node
        tensor_parallel_size=8,
    ),
)

app = build_openai_app({"llm_configs": [llm_config]})
