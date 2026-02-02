from ray.serve.llm import LLMConfig, build_openai_app

llm_config = LLMConfig(
    model_loading_config=dict(
        # The name your clients will use in the OpenAI-compatible API.
        model_id="Qwen/Qwen3-4B-Instruct-2507-FP8",
        # Hugging Face repo to pull from.
        model_source="Qwen/Qwen3-4B-Instruct-2507-FP8",
    ),
    # L4 (Ada) is FP8-friendly. Prefer H100 for best FP8 throughput.
    accelerator_type="L4",
    deployment_config=dict(
        autoscaling_config=dict(
            num_replicas=1,  # use 1 replica for now
        )
    ),
    # vLLM engine flags.
    engine_kwargs=dict(
        # Qwen3 supports 262,144 context natively; but you need a GPU with large memory to serve.
        max_model_len=65536,
        # Qwen models use custom chat templates; needed for some Hugging Face repos.
        trust_remote_code=True,
        gpu_memory_utilization=0.9,
        enable_auto_tool_choice=True,
        tool_call_parser="hermes",
    ),
)

app = build_openai_app({"llm_configs": [llm_config]})
