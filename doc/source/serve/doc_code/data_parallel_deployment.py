"""
DeepSeek-V3 deployment with Data+Expert Parallel across 16 GPUs.
Single deployment using all available GPUs for maximum horizontal scaling.
"""

from ray import serve
from ray.serve.llm import LLMConfig, ModelLoadingConfig
from ray.llm._internal.serve.serving_patterns.data_parallel.dp_server import (
    build_openai_dp_app,
)

LLM_CONFIG = LLMConfig(
    model_loading_config=ModelLoadingConfig(
        model_id=deepseek,
        model_source=deepseek-ai/DeepSeek-V3,
    ),
    engine_kwargs=dict(
        data_parallel_size=16,
        tensor_parallel_size=1,
        enable_expert_parallel=True,
        enable_prefix_caching=False,
        max_model_len=16384,
        max_num_seqs=4096,
        enable_dbo=True,
    ),
    experimental_configs={
        "dp_size_per_node": 8,
    },
    runtime_env={
        "env_vars": {
            "VLLM_USE_DEEP_GEMM": "1",
            "VLLM_ALL2ALL_BACKEND": "deepep_low_latency",
            "VLLM_MOE_DP_CHUNK_SIZE": "256",
            "VLLM_SKIP_P2P_CHECK": "1",
            "VLLM_RANDOMIZE_DP_DUMMY_INPUTS": "1",
            "NVIDIA_GDRCOPY": "enabled",
            "VLLM_LOGGING_LEVEL": "DEBUG",
            "NCCL_DEBUG": "WARN",
            "PYTORCH_CUDA_ALLOC_CONF": "expandable_segments:True",
            "VLLM_MOE_ROUTING_SIMULATION_STRATEGY": "uniform_random",
        }
    },
)

# Customize ingress deployment options
ingress_deployment_config = {
    "autoscaling_config": {
        "min_replicas": 64,
        "max_replicas": 64,
    },
}

# Build and deploy the application with custom ingress config
app = build_openai_dp_app(LLM_CONFIG, ingress_deployment_config=ingress_deployment_config)
serve.run(app, blocking=True)

