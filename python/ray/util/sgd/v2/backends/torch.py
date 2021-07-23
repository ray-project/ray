from dataclasses import dataclass

from ray.util.sgd.v2.backends.backend import BackendExecutor, BackendConfig


class TorchExecutor(BackendExecutor):
    pass


@dataclass
class TorchConfig(BackendConfig):
    pass
