import abc


class BaseConnectorBackend(abc.ABC):
    def __init__(self: "BaseConnectorBackend", kv_transfer_config: dict):
        self.kv_transfer_config = kv_transfer_config

    @abc.abstractmethod
    def setup(self: "BaseConnectorBackend") -> None:
        pass
