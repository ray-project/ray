from ray.serve.llm import LLMConfig
from ray.serve.llm import build_openai_app

# Define the configuration as provided
llm_config = LLMConfig(
    model_loading_config={"model_id": "Qwen/Qwen2.5-32B-Instruct"},
    engine_kwargs={
        "max_num_batched_tokens": 8192,
        "max_model_len": 8192,
        "max_num_seqs": 64,
        "tensor_parallel_size": 4,
        "trust_remote_code": True,
    },
    accelerator_type="A10G",
    deployment_config={
        "autoscaling_config": {"target_ongoing_requests": 32},
        "max_ongoing_requests": 64,
    },
)


# Build and deploy the model with OpenAI api compatibility:
llm_app = build_openai_app({"llm_configs": [llm_config]})
