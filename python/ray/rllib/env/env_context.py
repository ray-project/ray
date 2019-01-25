from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


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
    """

    def __init__(self, env_config, worker_index, vector_index=0, remote=False,
                 placeholder_env=False):
        dict.__init__(self, env_config)
        self.worker_index = worker_index
        self.vector_index = vector_index
        self.remote = remote
        self.placeholder_env = placeholder_env

    def align(self, env_config=None, worker_index=None, vector_index=None,
              remote=None, placeholder_env=None):
        return EnvContext(
            env_config if env_config is not None else self,
            worker_index if worker_index is not None else self.worker_index,
            vector_index if vector_index is not None else self.vector_index,
            remote if remote is not None else self.remote,
            placeholder_env if placeholder_env is not None
            else self.placeholder_env,
        )
