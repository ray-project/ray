# __deepseek_setup_start__

from ray import serve
from ray.serve.llm import LLMConfig, build_openai_app

llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="deepseek",
        model_source="deepseek-ai/DeepSeek-R1",
    ),
    deployment_config=dict(autoscaling_config=dict(
        min_replicas=1,
        max_replicas=1,
    )),
    # Change to the accelerator type of the node
    accelerator_type="H100",
    runtime_env={"env_vars": {"VLLM_USE_V1": "1"}},
    # Customize engine arguments as needed (e.g. vLLM engine kwargs)
    engine_kwargs=dict(
        tensor_parallel_size=8,
        pipeline_parallel_size=2,
        gpu_memory_utilization=0.92,
        dtype="auto",
        max_num_seqs=40,
        max_model_len=16384,
        enable_chunked_prefill=True,
        enable_prefix_caching=True,
        trust_remote_code=True,
    ),
)

# Deploy the application
llm_app = build_openai_app({"llm_configs": [llm_config]})
serve.run(llm_app)

# __deepseek_setup_end__

