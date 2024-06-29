from ray.rllib.core.columns import Columns


DEFAULT_AGENT_ID = "default_agent"
DEFAULT_POLICY_ID = "default_policy"
DEFAULT_MODULE_ID = DEFAULT_POLICY_ID  # TODO (sven): Change this to "default_module"

COMPONENT_ENV_TO_MODULE_CONNECTOR = "env_to_module_connector"
COMPONENT_LEARNER = "learner"
COMPONENT_MODULE_TO_ENV_CONNECTOR = "module_to_env_connector"
COMPONENT_MODULES_TO_BE_UPDATED = "modules_to_be_updated"
COMPONENT_OPTIMIZER = "optimizer"
COMPONENT_RL_MODULE = "rl_module"


__all__ = [
    "Columns",
    "COMPONENT_ENV_TO_MODULE_CONNECTOR",
    "COMPONENT_MODULE_TO_ENV_CONNECTOR",
    "COMPONENT_MODULES_TO_BE_UPDATED",
    "COMPONENT_OPTIMIZER," "COMPONENT_RL_MODULE",
    "DEFAULT_AGENT_ID",
    "DEFAULT_MODULE_ID",
    "DEFAULT_POLICY_ID",
]
