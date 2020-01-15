from ray.rllib.utils.annotations import PublicAPI


@PublicAPI
class EnvContext(dict):
    """Wraps env configurations to include extra rllib metadata.

    These attributes can be used to parameterize environments per process.
    For example, one might use `worker_index` to control which data file an
    environment reads in on initialization.

    RLlib auto-sets these attributes when constructing registered envs.

    Attributes:
        worker_index (int): When there are multiple workers created, this
            uniquely identifies the worker the env is created in.
        vector_index (int): When there are multiple envs per worker, this
            uniquely identifies the env index within the worker.
        remote (bool): Whether environment should be remote or not.
    """

    def __init__(self, env_config, worker_index, vector_index=0, remote=False):
        dict.__init__(self, env_config)
        self.worker_index = worker_index
        self.vector_index = vector_index
        self.remote = remote

    def copy_with_overrides(self,
                            env_config=None,
                            worker_index=None,
                            vector_index=None,
                            remote=None):
        return EnvContext(
            env_config if env_config is not None else self,
            worker_index if worker_index is not None else self.worker_index,
            vector_index if vector_index is not None else self.vector_index,
            remote if remote is not None else self.remote,
        )
