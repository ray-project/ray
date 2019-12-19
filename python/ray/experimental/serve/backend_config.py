import pprint
import json


class BackendConfig:
    # configs not needed for actor creation when
    # instantiating a replica
    serve_configs = ["num_replicas", "max_batch_size"]

    # configs which when changed leads to restarting
    # the existing replicas.
    restart_configs = ["resources", "num_cpus", "num_gpus"]

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

    def __repr__(self):
        ret = "<{klass}({attrs})>".format(
            klass=self.__class__.__name__,
            attrs=", ".join(
                "{}={!r}".format(k, v) for k, v in self.__dict__.items()))
        return pprint.pformat(ret)

    def __str__(self):
        return json.dumps(self.__dict__)

    @classmethod
    def from_str(cls, json_string):
        return cls(**json.loads(json_string))
