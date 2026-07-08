"""vLLM connector base + the default (vLLM-shaped) P/D protocol policy.

The engine-neutral connector base lives in
``ray.llm._internal.serve.engines.common.kv_transfer.base`` and knows nothing
about any specific engine. This module adds the vLLM-specific layer on top:

  * ``VLLMConnectorBackend`` — adds the ``kv_transfer_config`` property that vLLM
    connectors need (the neutral base does not carry it).
  * ``DefaultPDProtocolMixin`` / ``DefaultConnectorBackend`` — the standard
    (no-peer-binding, sequential) P/D policy, expressed with vLLM's
    ``kv_transfer_params``. vLLM connectors (nixl, lmcache, multi) inherit it; a
    non-vLLM engine implements its own request shaping instead.

``BaseConnectorBackend`` and ``clamp_request_to_single_token`` are re-exported
from the neutral base so existing imports of them from this path keep working.
"""

from typing import TYPE_CHECKING, Any, Dict, Optional

from ray.llm._internal.serve.engines.common.kv_transfer.base import (  # noqa: F401
    BaseConnectorBackend,
    clamp_request_to_single_token,
)

if TYPE_CHECKING:
    from ray.llm._internal.serve.engines.common.kv_transfer.base import (  # noqa: F401
        RequestType,
    )


def base_prefill_kv_transfer_params() -> Dict[str, Any]:
    """The ``kv_transfer_params`` common to a prefill (producer) request.

    Tells the prefill engine to produce KV for a remote decode. Connectors layer
    their own keys (e.g. a transfer id, DP/TP routing) on top of these. This is a
    vLLM concept (``kv_transfer_params``), hence it lives on the vLLM side.
    """
    return {
        "do_remote_decode": True,
        "do_remote_prefill": False,
        "remote_engine_id": None,
        "remote_block_ids": None,
    }


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


class DefaultPDProtocolMixin:
    """The default P/D protocol policy: no peer binding, sequential handoff.

    Implements ``prepare_prefill_request`` / ``prepare_decode_request`` for
    connectors that follow the standard policy: the prefill engine is told to
    produce KV for a remote decode (clamped to a single non-streaming token),
    and the decode engine forwards the ``kv_transfer_params`` that the prefill
    engine returned on its first response chunk.

    This policy is expressed with vLLM's ``kv_transfer_params``, so it lives on
    the vLLM side. A non-vLLM engine implements its own request shaping.

    Mix this in *before* ``VLLMConnectorBackend`` in a backend's bases so its
    concrete methods satisfy the abstract methods.
    """

    def prepare_prefill_request(
        self, *, request: "RequestType", peer: Optional[Dict[str, Any]]
    ) -> "RequestType":
        """Shape the prefill request under the default P/D protocol policy.

        Deep-copies the request, stamps the standard ``kv_transfer_params`` that
        tell the prefill engine to produce KV for a remote decode, and clamps it
        to a single, non-streaming token. ``peer`` is ignored.
        """
        assert (
            getattr(request, "kv_transfer_params", None) is None
        ), "kv_transfer_params should be empty before orchestrator"
        prefill_request = request.model_copy(deep=True)
        prefill_request.kv_transfer_params = {
            **base_prefill_kv_transfer_params(),
            "remote_host": None,
            "remote_port": None,
        }
        clamp_request_to_single_token(prefill_request)
        return prefill_request

    def prepare_decode_request(
        self,
        *,
        request: "RequestType",
        peer: Optional[Dict[str, Any]],
        prefill_response: Optional[Any],
    ) -> "RequestType":
        """Shape the decode request under the default P/D protocol policy.

        Deep-copies the request and, only when a prefill response chunk was
        captured, forwards its ``kv_transfer_params`` so the decode engine
        pulls/receives the KV produced by prefill. In concurrent-handoff mode
        ``prefill_response`` is None and the request is left unmodified. ``peer``
        is ignored.
        """
        decode_request = request.model_copy(deep=True)
        if prefill_response is not None:
            decode_request.kv_transfer_params = prefill_response.kv_transfer_params
        return decode_request


class DefaultConnectorBackend(DefaultPDProtocolMixin, VLLMConnectorBackend):
    """Concrete connector backend using the default P/D protocol policy.

    Used as the factory fallback for connectors that are not registered with a
    dedicated backend class: they get a no-op ``setup()`` and the default
    request-shaping policy. ``BaseConnectorBackend`` is abstract, so the factory
    must return a concrete class like this one.
    """

    pass
