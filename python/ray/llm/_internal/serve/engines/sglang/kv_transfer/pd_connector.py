"""SGLang P/D connector backend for Ray Serve LLM.

SGLang runs prefill and decode concurrently; prefill PUSHES the KV cache to
decode through a bootstrap server that lives on the prefill worker. The decode
side must know the prefill node's bootstrap (host, port) up front -> both
protocol flags are on:
  * ``concurrent_handoff = True``  (prefill pushes; decode needs nothing from
    prefill's response)
  * ``requires_peer_binding = True`` (decode binds to the selected prefill
    replica's bootstrap address before dispatch).

Modeled on ``MoRIIOConnectorBackend`` (the existing both-at-once / address-first
connector). Unlike MoRIIO it adds SGLang bootstrap fields (not
``kv_transfer_params``) and has no ``kv_transfer_config`` -> it inherits the
neutral ``BaseConnectorBackend`` directly.

``setup()`` runs on the prefill engine: it picks a free bootstrap port (SGLang's
default 8998 collides when replicas share a node) and sets the SGLang server
``host`` to the routable node IP (SGLang binds the bootstrap server to
``server_args.host``, default 127.0.0.1 -> a remote decode would get Connection
refused). The address is published via ``replica_metadata`` and the decode
orchestrator stamps it onto both requests via ``peer``.

``bootstrap_room`` is derived deterministically from the incoming request id, so
the two stateless ``prepare_*`` calls agree without per-request backend state.
"""

import hashlib
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple

import ray
from ray.llm._internal.serve.engines.common.kv_transfer.base import (
    BaseConnectorBackend,
)

if TYPE_CHECKING:
    from ray.llm._internal.serve.engines.common.kv_transfer.base import RequestType

# SGLang's default disaggregation bootstrap port. Colocated replicas collide on
# it, so setup() shifts by _compute_port_offset().
DEFAULT_BOOTSTRAP_PORT_BASE = 8998

# bootstrap_room must fit SGLang's range (it computes room % dp_size). 62 bits
# matches the prototype's secrets.randbits(62); derived from the request id hash
# so both prepare_* calls agree.
_ROOM_BITS = 62


class SGLangConnectorBackend(BaseConnectorBackend):
    """SGLang P/D connector: concurrent handoff, prefill-address-first."""

    concurrent_handoff: bool = True
    requires_peer_binding: bool = True

    # Set by setup(); published via replica_metadata().
    _bootstrap_host: Optional[str] = None
    _bootstrap_port: Optional[int] = None

    def setup(self) -> None:
        """Pick a free bootstrap port + set host to the node IP, before engine start."""
        offset = self._compute_port_offset()
        engine_kwargs = self.llm_config.engine_kwargs

        # SGLang binds the bootstrap server to server_args.host (default
        # 127.0.0.1). The remote decode dials the node IP we advertise, so bind
        # the routable IP or it gets Connection refused.
        host = ray.util.get_node_ip_address()
        engine_kwargs["host"] = host

        port = engine_kwargs.get("disaggregation_bootstrap_port")
        if port is None:
            port = DEFAULT_BOOTSTRAP_PORT_BASE + offset
            engine_kwargs["disaggregation_bootstrap_port"] = port

        self._bootstrap_host = host
        self._bootstrap_port = port

    def replica_metadata(self) -> Dict[str, Any]:
        """Publish this (prefill) replica's bootstrap address for the decode peer."""
        return {
            "bootstrap_host": self._bootstrap_host,
            "bootstrap_port": self._bootstrap_port,
        }

    def _peer_address(self, peer: Optional[Dict[str, Any]]) -> Tuple[str, int]:
        host = (peer or {}).get("bootstrap_host")
        port = (peer or {}).get("bootstrap_port")
        if not host or not port:
            raise ValueError(
                "SGLang peer is missing bootstrap_host/bootstrap_port: the "
                "selected prefill replica did not publish its bootstrap address "
                "(is the prefill deployment using llm_engine='SGLang' with a "
                "disaggregation_transfer_backend?)."
            )
        return host, port

    def _bootstrap_room(self, request: Any) -> int:
        """Deterministic per-request room id from the request id (stateless).

        Both prepare_* calls run on the same request id, so they agree; uniqueness
        per request is inherited from the (uuid-defaulted) request id.
        """
        seed = str(request.request_id)
        digest = hashlib.sha256(seed.encode()).hexdigest()
        return int(digest, 16) & ((1 << _ROOM_BITS) - 1)

    def _stamp(self, request: Any, peer: Optional[Dict[str, Any]]) -> Any:
        host, port = self._peer_address(peer)
        out = request.model_copy(deep=True)
        out.bootstrap_host = host
        out.bootstrap_port = port
        out.bootstrap_room = self._bootstrap_room(request)
        return out

    def prepare_prefill_request(
        self, *, request: "RequestType", peer: Optional[Dict[str, Any]]
    ) -> "RequestType":
        return self._stamp(request, peer)

    def prepare_decode_request(
        self,
        *,
        request: "RequestType",
        peer: Optional[Dict[str, Any]],
        prefill_response: Optional[Any],
    ) -> "RequestType":
        # Concurrent handoff: prefill_response is None; decode needs only the
        # SAME prefill bootstrap host/port/room to rendezvous.
        return self._stamp(request, peer)
