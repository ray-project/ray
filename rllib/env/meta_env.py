from ray.rllib.env.apis.task_settable_env import TaskSettableEnv
from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning("MetaEnv", "TaskSettableEnv", error=False)
MetaEnv = TaskSettableEnv
