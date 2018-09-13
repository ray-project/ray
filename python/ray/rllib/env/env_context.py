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

    def __init__(self, env_config, worker_index, vector_index=0):
        dict.__init__(self, env_config)
        self.worker_index = worker_index
        self.vector_index = vector_index

    def with_vector_index(self, vector_index):
        return EnvContext(
            self, worker_index=self.worker_index, vector_index=vector_index)
