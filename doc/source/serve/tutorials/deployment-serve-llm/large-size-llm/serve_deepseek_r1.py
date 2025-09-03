# serve_deepseek_r1.py
from ray.serve.llm import LLMConfig, build_openai_app

llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="my-deepseek-r1",
        model_source="deepseek-ai/DeepSeek-R1",
    ),
    accelerator_type="H100",
    deployment_config=dict(
        autoscaling_config=dict(
            min_replicas=1,
            max_replicas=1,
        )
    ),
    ### Uncomment if your model is gated and needs your Hugging Face token to access it.
    # runtime_env=dict(
    #    env_vars={
    #        "HF_TOKEN": os.environ.get("HF_TOKEN")
    #    }
    # ),
    engine_kwargs=dict(
        max_model_len=16384,
        # Split weights among 8 GPUs in the node
        tensor_parallel_size=8,
        pipeline_parallel_size=2,
        reasoning_parser="deepseek_r1",  # Optional: separate reasoning content from the final answer
    ),
)

app = build_openai_app({"llm_configs": [llm_config]})
