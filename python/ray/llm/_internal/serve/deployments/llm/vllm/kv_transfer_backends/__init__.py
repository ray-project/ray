from typing import Dict

from ray.llm._internal.serve.deployments.llm.vllm.kv_transfer_backends.base import (
    BaseConnectorBackend,
)
from ray.llm._internal.serve.deployments.llm.vllm.kv_transfer_backends.lmcache_connector_v1 import (
    LMCacheConnectorV1Backend,
)
from ray.llm._internal.serve.deployments.llm.vllm.kv_transfer_backends.nixl_connector import (
    NixlConnectorBackend,
)

SUPPORTED_BACKENDS: Dict[str, BaseConnectorBackend] = {
    "LMCacheConnectorV1": LMCacheConnectorV1Backend,
    "NixlConnector": NixlConnectorBackend,
}
