from dataclasses import dataclass

from ray.util.sgd.v2.backends.backend import BackendExecutor, BackendConfig


@dataclass
class TorchConfig(BackendConfig):
    def backend_name(self):
        return "torch"


def setup_torch_process_group():
    pass


def shutdown_torch():
    pass


class TorchExecutor(BackendExecutor):
    pass
