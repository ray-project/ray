from ray.rllib.env.env_runner_group import EnvRunnerGroup
from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning(
    old="WorkerSet",
    new="ray.rllib.env.env_runner_group.EnvRunnerGroup",
    help="The class has only be renamed w/o any changes in functionality.",
    error=False,
)

WorkerSet = EnvRunnerGroup
