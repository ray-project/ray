import abc


class BaseConnectorBackend(abc.ABC):
    def __init__(self, kv_transfer_config):
        """Base class for connector backends.

        :type kv_transfer_config: vllm.config.KVTransferConfig
        :param kv_transfer_config: Configuration for the KV transfer. We don't use type hint to enable runtime dependency on vLLM.
        """
        self.kv_transfer_config = kv_transfer_config

    @abc.abstractmethod
    def setup(self) -> None:
        pass
