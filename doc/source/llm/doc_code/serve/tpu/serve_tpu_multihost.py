# flake8: noqa
"""
Example Ray Serve LLM application for a multi-host TPU slice.
"""

# __serve_tpu_multihost_start__
from ray import serve
from ray.serve.llm import LLMConfig, LLMServingArgs, build_openai_app

llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="google/gemma-4-31B-it",
        model_source="/data/google/gemma-4-31B-it",
    ),
    accelerator_type="TPU-V6E",
    accelerator_config={"kind": "tpu", "topology": "4x4"},
    engine_kwargs=dict(
        tensor_parallel_size=16,
        max_model_len=8192,
        max_num_batched_tokens=8192,
        distributed_executor_backend="ray",
    ),
)

app = build_openai_app(LLMServingArgs(llm_configs=[llm_config]))

if __name__ == "__main__":
    serve.run(app, blocking=True)
# __serve_tpu_multihost_end__


# __serve_torchtpu_multihost_start__
from ray import serve
from ray.serve.llm import LLMConfig, LLMServingArgs, build_openai_app

llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="Qwen/Qwen3-8B",
        model_source="Qwen/Qwen3-8B",
    ),
    accelerator_type="TPU-V6E",
    accelerator_config={"kind": "tpu", "topology": "4x4"},
    # Run one vLLM worker actor per logical TPU device.
    placement_group_config={"bundle_per_worker": {"TPU": 1}},
    runtime_env={
        "env_vars": {
            "TPU_MULTIHOST_BACKEND": "ray",
        }
    },
    engine_kwargs=dict(
        tensor_parallel_size=16,
        max_model_len=4096,
        max_num_batched_tokens=4096,
        distributed_executor_backend="ray",
    ),
)

app = build_openai_app(LLMServingArgs(llm_configs=[llm_config]))

if __name__ == "__main__":
    serve.run(app, blocking=True)
# __serve_torchtpu_multihost_end__
