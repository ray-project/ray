# __serve_hook_start__
"""LLMServer subclass for ``LLMConfig.server_cls``: importing it registers the plugin."""

from ray.llm._internal.serve.core.server.llm_server import LLMServer

from qwen3_reward_plugin import register

register()


class RewardModelServer(LLMServer):
    pass
# __serve_hook_end__
