from ray.rllib.core.columns import Columns


DEFAULT_AGENT_ID = "default_agent"
DEFAULT_POLICY_ID = "default_policy"
DEFAULT_MODULE_ID = DEFAULT_POLICY_ID  # TODO (sven): Change this to "default_module"


__all__ = [
    "Columns",
    "DEFAULT_AGENT_ID",
    "DEFAULT_MODULE_ID",
    "DEFAULT_POLICY_ID",
]
