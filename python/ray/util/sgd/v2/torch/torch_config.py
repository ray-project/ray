from dataclasses import dataclass

from ray.util.sgd.v2.backend_config import BackendConfig


@dataclass
class TorchConfig(BackendConfig):
    # address: str  # The connection address to use for the process group.
    # process_group_timeout: int  # The timeout for process group creation.

    def backend_name(self):
        return "torch"
