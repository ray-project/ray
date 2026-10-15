# __setup_start__
from setuptools import setup

setup(
    name="qwen3-reward-plugin",
    version="0.1.0",
    packages=["qwen3_reward_plugin"],
    # vLLM discovers and runs this entry point in every process (the engine core
    # and each rank worker) via load_general_plugins().
    entry_points={
        "vllm.general_plugins": [
            "qwen3_reward = qwen3_reward_plugin:register",
        ],
    },
)
# __setup_end__
