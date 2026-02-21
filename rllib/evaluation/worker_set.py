from ray._common.deprecation import Deprecated


@Deprecated(
    new="ray.rllib.env.env_runner_group.EnvRunnerGroup",
    help="The class has only be renamed w/o any changes in functionality.",
    error=True,
)
class WorkerSet:
    pass
