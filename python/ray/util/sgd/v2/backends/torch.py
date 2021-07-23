from dataclasses import dataclass

from ray.util.sgd.v2.backend import BackendConfig, BackendExecutor


class TorchExecutor(BackendExecutor):
    pass


@dataclass
class TorchConfig(BackendConfig):
    pass
