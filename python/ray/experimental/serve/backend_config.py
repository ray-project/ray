import pprint
import json
from copy import deepcopy


class BackendConfig:
    # configs not needed for actor creation when
    # instantiating a replica
    serve_configs = ["num_replicas", "max_batch_size"]

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
                 object_store_memory=None):
        """
        Class for defining backend configuration.
        """

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

    def __repr__(self):
        ret = "<{klass}({attrs})>".format(
            klass=self.__class__.__name__,
            attrs=", ".join(
                "{}={!r}".format(k, v) for k, v in self.__dict__.items()))
        return pprint.pformat(ret)

    def __str__(self):
        return json.dumps(self.__dict__)

    def _asdict(self):
        ret_d = deepcopy(self.__dict__)
        val = ret_d.pop('_num_replicas')
        ret_d['num_replicas'] = val
        return ret_d

    @classmethod
    def from_str(cls, json_string):
        return cls(**json.loads(json_string))
