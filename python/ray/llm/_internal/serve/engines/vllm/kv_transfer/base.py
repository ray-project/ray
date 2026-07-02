"""Back-compat shim + vLLM-specific connector base.

The neutral connector base now lives in
``ray.llm._internal.serve.engines.common.kv_transfer.base``. This module
re-exports it (so existing imports keep working) and adds
``VLLMConnectorBackend``, which owns the vLLM-only ``kv_transfer_config``
property that the neutral base no longer carries.
"""

from typing import TYPE_CHECKING, Any, Dict

from ray.llm._internal.serve.engines.common.kv_transfer.base import (  # noqa: F401
    BaseConnectorBackend,
    DefaultConnectorBackend,
    DefaultPDProtocolMixin,
    base_prefill_kv_transfer_params,
    clamp_request_to_single_token,
)

if TYPE_CHECKING:
    from ray.llm._internal.serve.engines.common.kv_transfer.base import (  # noqa: F401
        RequestType,
    )


class VLLMConnectorBackend(BaseConnectorBackend):
    """Connector base for vLLM connectors that need ``kv_transfer_config``.

    The neutral ``BaseConnectorBackend`` is engine-agnostic and does not carry
    ``kv_transfer_config`` (a vLLM concept). vLLM connectors inherit this
    subclass instead so they keep the property.
    """

    @property
    def kv_transfer_config(self) -> Dict[str, Any]:
        engine_kwargs = self.llm_config.engine_kwargs
        kv_transfer_config = engine_kwargs.get("kv_transfer_config")
        assert (
            kv_transfer_config is not None
        ), "In Connector backend, kv_transfer_config is not set"
        return kv_transfer_config
