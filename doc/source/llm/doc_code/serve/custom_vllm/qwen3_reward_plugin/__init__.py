# __register_start__
"""vLLM plugin entry point: register the custom architecture."""
def register() -> None:
    from vllm import ModelRegistry

    if "Qwen3CustomRewardModel" not in ModelRegistry.get_supported_archs():
        ModelRegistry.register_model(
            "Qwen3CustomRewardModel",
            "qwen3_reward_plugin.qwen3_rm:Qwen3CustomRewardModel",
        )
# __register_end__
