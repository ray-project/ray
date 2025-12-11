from typing import Dict

from ray.llm._internal.serve.engines.vllm.kv_transfer.base import (
    BaseConnectorBackend,
)
from ray.llm._internal.serve.engines.vllm.kv_transfer.lmcache import (
    LMCacheConnectorV1Backend,
)
from ray.llm._internal.serve.engines.vllm.kv_transfer.nixl import (
    NixlConnectorBackend,
)

SUPPORTED_BACKENDS: Dict[str, BaseConnectorBackend] = {
    "LMCacheConnectorV1": LMCacheConnectorV1Backend,
    "NixlConnector": NixlConnectorBackend,
}
