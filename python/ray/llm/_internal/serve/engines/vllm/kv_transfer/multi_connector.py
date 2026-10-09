import copy
from typing import TYPE_CHECKING, List

from ray.llm._internal.serve.engines.vllm.kv_transfer.base import (
    BaseConnectorBackend,
)
from ray.llm._internal.serve.engines.vllm.kv_transfer.factory import (
    KVConnectorBackendFactory,
)

if TYPE_CHECKING:
    from ray.llm._internal.serve.core.configs.llm_config import LLMConfig


class MultiConnectorBackend(BaseConnectorBackend):
    """Wraps multiple sub-connectors.

    The P/D protocol (``prepare_prefill_request`` / ``prepare_decode_request`` and
    the ``requires_peer_binding`` / ``concurrent_handoff`` policy) is delegated to
    the *first* (top-most) sub-connector listed in ``connectors`` — that
    connector's policy governs request shaping and handoff for the group. Each
    sub-connector's ``setup()`` still runs.
    """

    def __init__(self, llm_config: "LLMConfig"):
        super().__init__(llm_config)
        self._connector_backends: List[BaseConnectorBackend] = []

    def setup(self) -> None:
        """Setup all connectors listed in the kv_transfer_config."""
        kv_transfer_config = self.kv_transfer_config
        connectors = kv_transfer_config.get("kv_connector_extra_config", {}).get(
            "connectors", []
        )
        if not connectors:
            # Fail fast at setup rather than with a cryptic error when the
            # orchestrator later delegates to a (missing) top-most sub-connector.
            raise ValueError(
                "MultiConnector requires at least one sub-connector in "
                "kv_connector_extra_config.connectors."
            )

        for connector in connectors:
            connector_backend_str = connector.get("kv_connector")
            if connector_backend_str is None:
                raise ValueError("kv_connector is not set in the connector")

            if connector_backend_str == "MultiConnector":
                raise ValueError(
                    "Nesting MultiConnector within MultiConnector is not supported."
                )

            # Merge parent config with connector-specific config
            sub_llm_config = copy.deepcopy(self.llm_config)
            sub_llm_config.engine_kwargs["kv_transfer_config"] = {
                **{
                    k: v
                    for k, v in kv_transfer_config.items()
                    if k != "kv_connector_extra_config"
                },
                **connector,
            }

            # Use factory to get backend class lazily
            connector_backend = KVConnectorBackendFactory.create_backend(
                connector_backend_str, sub_llm_config
            )
            connector_backend.setup()
            self._connector_backends.append(connector_backend)

    @property
    def _primary(self) -> BaseConnectorBackend:
        """The top-most sub-connector, whose protocol governs the group."""
        if not self._connector_backends:
            raise ValueError(
                "MultiConnectorBackend has no sub-connectors; was setup() called?"
            )
        return self._connector_backends[0]

    @property
    def requires_peer_binding(self) -> bool:
        return bool(self._connector_backends) and self._primary.requires_peer_binding

    @property
    def concurrent_handoff(self) -> bool:
        return bool(self._connector_backends) and self._primary.concurrent_handoff

    def prepare_prefill_request(self, *, request, peer):
        return self._primary.prepare_prefill_request(request=request, peer=peer)

    def prepare_decode_request(self, *, request, peer, prefill_response):
        return self._primary.prepare_decode_request(
            request=request, peer=peer, prefill_response=prefill_response
        )
