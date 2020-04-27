from copy import deepcopy


class BackendConfig:
    # configs not needed for actor creation when
    # instantiating a replica
    _serve_configs = [
        "_num_replicas", "max_batch_size", "has_accept_batch_annotation"
    ]

    # configs which when changed leads to restarting
    # the existing replicas.
    restart_on_change_fields = ["resources", "num_cpus", "num_gpus"]

    def __init__(self,
                 num_replicas=1,
                 resources=None,
                 max_batch_size=None,
                 num_cpus=None,
                 num_gpus=None,
                 memory=None,
                 object_store_memory=None,
                 has_accept_batch_annotation=False):
        """
        Class for defining backend configuration.
        """
        # backend metadata
        self.has_accept_batch_annotation = has_accept_batch_annotation

        # serve configs
        self.num_replicas = num_replicas
        self.max_batch_size = max_batch_size

        # ray actor configs
        self.resources = resources
        self.num_cpus = num_cpus
        self.num_gpus = num_gpus
        self.memory = memory
        self.object_store_memory = object_store_memory

    @property
    def num_replicas(self):
        return self._num_replicas

    @num_replicas.setter
    def num_replicas(self, val):
        if not (val > 0):
            raise Exception("num_replicas must be greater than zero")
        self._num_replicas = val

    def __iter__(self):
        for k in self.__dict__.keys():
            key, val = k, self.__dict__[k]
            if key == "_num_replicas":
                key = "num_replicas"
            yield key, val

    def get_actor_creation_args(self, init_args):
        ret_d = deepcopy(self.__dict__)
        for k in self._serve_configs:
            ret_d.pop(k)
        ret_d["args"] = init_args
        return ret_d
